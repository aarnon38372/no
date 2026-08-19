import os,json,asyncio,logging,base64,time
from datetime import datetime,timezone
import aiohttp,discord,websockets
from cryptography.hazmat.primitives import serialization,hashes
from cryptography.hazmat.primitives.asymmetric import padding

TOKEN=os.environ["DISCORD_TOKEN"]; CHANNEL=int(os.environ["DISCORD_CHANNEL_ID"])
KEY_ID=os.environ["KALSHI_API_KEY_ID"]
KEY_PEM=os.environ["KALSHI_PRIVATE_KEY"].replace("\\n","\n")
SERIES=os.getenv("KALSHI_SERIES","KXBTC15M")
REST="https://api.elections.kalshi.com/trade-api/v2"
WS="wss://external-api-ws.kalshi.com/trade-api/ws/v2"; PATH="/trade-api/ws/v2"
INTERVAL=float(os.getenv("DISCORD_UPDATE_INTERVAL","0.75"))
logging.basicConfig(level=logging.INFO); log=logging.getLogger("bot")
client=discord.Client(intents=discord.Intents.default())
http=None; market=None; msg=None; state={}; dirty=asyncio.Event(); ws_task=None; started=False
key=serialization.load_pem_private_key(KEY_PEM.encode(),password=None)

def headers():
 t=str(int(time.time()*1000)); raw=(t+"GET"+PATH).encode()
 sig=key.sign(raw,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
 return {"KALSHI-ACCESS-KEY":KEY_ID,"KALSHI-ACCESS-SIGNATURE":base64.b64encode(sig).decode(),"KALSHI-ACCESS-TIMESTAMP":t}
def parse(v):
 try:return datetime.fromisoformat(v.replace("Z","+00:00"))
 except:return None
def pc(v):
 try:
  x=float(v)*100
  return f"{x:.1f}¢" if x%1 else f"{int(x)}¢"
 except:return "—"
def left():
 c=parse(market.get("close_time","")) if market else None
 s=max(0,int((c-datetime.now(timezone.utc)).total_seconds())) if c else 0
 return f"{s//60:02d}:{s%60:02d}"
async def find_market():
 async with http.get(f"{REST}/markets",params={"series_ticker":SERIES,"status":"open","limit":100}) as r:d=await r.json()
 now=datetime.now(timezone.utc); ms=[m for m in d.get("markets",[]) if parse(m.get("close_time","")) and parse(m["close_time"])>now]
 return min(ms,key=lambda m:parse(m["close_time"])) if ms else None
def val(n): return state.get(n,market.get(n) if market else None)
def _num(v):
 try: return float(v)
 except: return None

def _yes_book():
 # WebSocket YES bid/ask are the source of truth. Fall back to REST only
 # before the first ticker packet arrives.
 yb = state.get("yes_bid_dollars")
 ya = state.get("yes_ask_dollars")
 if yb is None: yb = market.get("yes_bid_dollars") if market else None
 if ya is None: ya = market.get("yes_ask_dollars") if market else None
 return _num(yb), _num(ya)

def embed():
 yb, ya = _yes_book()

 # Binary complement:
 # NO bid = 1 - YES ask
 # NO ask = 1 - YES bid
 nb = max(0.0, min(1.0, 1.0 - ya)) if ya is not None else None
 na = max(0.0, min(1.0, 1.0 - yb)) if yb is not None else None

 last = state.get("price_dollars")
 if last is None:
  last = market.get("last_price_dollars") if market else None

 e=discord.Embed(title="₿ BTC 15-Minute Kalshi Market",description=market.get("title","Live market"),color=0x5865F2,timestamp=datetime.now(timezone.utc))
 e.add_field(name="🟢 UP / YES",value=f'Bid **{pc(yb)}**\\nAsk **{pc(ya)}**',inline=True)
 e.add_field(name="🔴 DOWN / NO",value=f'Bid **{pc(nb)}**\\nAsk **{pc(na)}**',inline=True)
 e.add_field(name="Last Trade",value=f'**{pc(last)}**',inline=True)
 e.add_field(name="⏱ Time Left",value=f"**{left()}**",inline=True)
 e.add_field(name="Feed",value="⚡ Kalshi WebSocket",inline=True)
 e.add_field(name="Ticker",value=f'`{market.get("ticker","—")}`',inline=False)
 e.set_footer(text="Live Kalshi WebSocket • DOWN derived from live YES book")
 return e

async def feed(ticker):
 global state
 backoff=1
 while market and market.get("ticker")==ticker:
  try:
   async with websockets.connect(WS,additional_headers=headers(),ping_interval=20) as w:
    await w.send(json.dumps({"id":1,"cmd":"subscribe","params":{"channels":["ticker"],"market_tickers":[ticker]}}))
    log.info("Kalshi WS live: %s",ticker); backoff=1
    async for raw in w:
     d=json.loads(raw)
     if d.get("type")=="ticker" and d.get("msg",{}).get("market_ticker")==ticker:
      state.update(d["msg"]); dirty.set()
     elif d.get("type")=="error": log.warning("WS error %s",d)
  except asyncio.CancelledError: raise
  except Exception as e: log.warning("WS reconnect %r",e)
  await asyncio.sleep(backoff); backoff=min(15,backoff*2)
async def render():
 while True:
  try: await asyncio.wait_for(dirty.wait(),timeout=1)
  except asyncio.TimeoutError: pass
  dirty.clear()
  if market and msg:
   try: await msg.edit(embed=embed())
   except discord.HTTPException as e: log.warning("Discord edit %r",e)
  await asyncio.sleep(INTERVAL)
async def manager():
 global market,msg,state,ws_task
 await client.wait_until_ready(); ch=client.get_channel(CHANNEL) or await client.fetch_channel(CHANNEL)
 while True:
  try:
   m=await find_market()
   if m and (not market or m["ticker"]!=market["ticker"]):
    old=msg; market=m; state={}
    if ws_task: ws_task.cancel()
    if old:
     try: await old.delete()
     except: pass
    msg=await ch.send(embed=embed()); ws_task=asyncio.create_task(feed(m["ticker"])); dirty.set()
    log.info("Market %s",m["ticker"])
  except Exception as e: log.warning("Discovery %r",e)
  await asyncio.sleep(2)
@client.event
async def on_ready():
 global started
 log.info("Discord ready: %s",client.user)
 if not started:
  started=True; asyncio.create_task(manager()); asyncio.create_task(render())
async def main():
 global http
 async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as s:
  http=s; await client.start(TOKEN)
asyncio.run(main())
