"""Reliable Kalshi BTC 15m live-market Discord display."""
import asyncio, base64, json, logging, os, time
from datetime import datetime, timezone
import aiohttp, discord
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

TOKEN=os.environ["DISCORD_TOKEN"]; CHANNEL=int(os.environ["DISCORD_CHANNEL_ID"]); KEY_ID=os.environ["KALSHI_API_KEY_ID"]; KEY_PEM=os.environ["KALSHI_PRIVATE_KEY"].replace("\\n","\n")
SERIES=os.getenv("KALSHI_SERIES","KXBTC15M"); REST=os.getenv("KALSHI_REST_URL","https://api.elections.kalshi.com/trade-api/v2").rstrip("/"); WS=os.getenv("KALSHI_WS_URL","wss://api.elections.kalshi.com/trade-api/ws/v2")
INTERVAL=max(1.0,float(os.getenv("DISCORD_UPDATE_INTERVAL","1.25"))); STALE=max(8.0,float(os.getenv("KALSHI_STALE_SECONDS","15")))
logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"),format="%(asctime)s %(levelname)s %(name)s: %(message)s"); log=logging.getLogger("live-market")
client=discord.Client(intents=discord.Intents.default()); http=None; market=None; message=None; state={}; state_updated=0.0; feed_task=None; started=False; channel=None
key=serialization.load_pem_private_key(KEY_PEM.encode(),password=None)

def signed(method,path):
    t=str(int(time.time()*1000)); raw=(t+method.upper()+path.split("?")[0]).encode(); sig=key.sign(raw,padding.PSS(mgf=padding.MGF1(hashes.SHA256()),salt_length=padding.PSS.DIGEST_LENGTH),hashes.SHA256())
    return {"KALSHI-ACCESS-KEY":KEY_ID,"KALSHI-ACCESS-SIGNATURE":base64.b64encode(sig).decode(),"KALSHI-ACCESS-TIMESTAMP":t}
def parse(v):
    try:return datetime.fromisoformat(v.replace("Z","+00:00"))
    except Exception:return None
def num(v):
    try:return float(v)
    except Exception:return None
def pc(v):
    x=num(v)
    if x is None:return "—"
    c=x*100 if x<=1.00001 else x
    return f"{c:.1f}¢".replace(".0¢","¢")
def left():
    c=parse(market.get("close_time","")) if market else None; s=max(0,int((c-datetime.now(timezone.utc)).total_seconds())) if c else 0; return f"{s//60:02d}:{s%60:02d}"
async def get_json(endpoint,params=None):
    path=f"/trade-api/v2{endpoint}"
    async with http.get(REST+endpoint,params=params,headers=signed("GET",path)) as r: r.raise_for_status(); return await r.json()
async def find_market():
    d=await get_json("/markets",{"series_ticker":SERIES,"status":"open","limit":100}); now=datetime.now(timezone.utc)
    ms=[m for m in d.get("markets",[]) if parse(m.get("close_time","")) and parse(m["close_time"])>now]
    return min(ms,key=lambda m:parse(m["close_time"])) if ms else None
async def rest_refresh():
    global market,state,state_updated
    if not market:return
    try:
        m=(await get_json(f"/markets/{market['ticker']}")).get("market",{}); market.update(m)
        for k in ("yes_bid_dollars","yes_ask_dollars","last_price_dollars","volume_fp","open_interest_fp"):
            if m.get(k) is not None:state[k]=m[k]
        state_updated=time.monotonic()
    except Exception as e:log.warning("REST refresh failed: %s",e)
def yes_book():
    yb=num(state.get("yes_bid_dollars",market.get("yes_bid_dollars") if market else None)); ya=num(state.get("yes_ask_dollars",market.get("yes_ask_dollars") if market else None)); return yb,ya
def embed():
    yb,ya=yes_book(); nb=max(0.0,min(1.0,1.0-ya)) if ya is not None else None; na=max(0.0,min(1.0,1.0-yb)) if yb is not None else None
    up=ya if ya is not None else yb; down=na if na is not None else nb; age=(time.monotonic()-state_updated) if state_updated else 999; live=age<STALE
    color=0x23A55A if live else 0xF0B232
    e=discord.Embed(title="₿  BTC • 15 MIN",description=("🟢 **LIVE KALSHI MARKET**" if live else "🟡 **RECONNECTING • LAST KNOWN PRICES**"),color=color,timestamp=datetime.now(timezone.utc))
    e.add_field(name="📈 UP",value=f"# {pc(up)}\n`BID {pc(yb)}  •  ASK {pc(ya)}`",inline=False); e.add_field(name="📉 DOWN",value=f"# {pc(down)}\n`BID {pc(nb)}  •  ASK {pc(na)}`",inline=False)
    e.add_field(name="⏱ TIME LEFT",value=f"**{left()}**",inline=True); e.add_field(name="⚡ FEED",value=("**LIVE**" if live else "**RECOVERING**"),inline=True); e.add_field(name="📡 AGE",value=f"**{age:.0f}s**" if state_updated else "**—**",inline=True)
    e.set_footer(text=f"{market.get('ticker','KXBTC15M') if market else 'Waiting for market'} • auto-rollover • self-healing feed"); return e

async def feed(ticker):
    global state,state_updated
    backoff=1
    while market and market.get("ticker")==ticker:
        try:
            async with http.ws_connect(WS,headers=signed("GET","/trade-api/ws/v2"),heartbeat=15,receive_timeout=30) as ws:
                await ws.send_json({"id":1,"cmd":"subscribe","params":{"channels":["ticker"],"market_tickers":[ticker]}}); log.info("WS live: %s",ticker); backoff=1
                async for item in ws:
                    if item.type==aiohttp.WSMsgType.TEXT:
                        d=json.loads(item.data)
                        if d.get("type")=="ticker" and d.get("msg",{}).get("market_ticker")==ticker: state.update(d["msg"]); state_updated=time.monotonic()
                        elif d.get("type")=="error": raise RuntimeError(str(d))
                    elif item.type in {aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR}: break
        except asyncio.CancelledError:raise
        except Exception as e:log.warning("WS reconnect for %s: %s",ticker,e)
        await asyncio.sleep(backoff); backoff=min(20,backoff*2)

async def ensure_message():
    global message
    if not market:return
    if message:
        try: await message.edit(embed=embed()); return
        except discord.NotFound: message=None
        except discord.HTTPException as e: log.warning("Discord edit failed: %s",e); return
    try: message=await channel.send(embed=embed())
    except discord.HTTPException as e:log.warning("Discord send failed: %s",e)

async def manager():
    global market,message,state,state_updated,feed_task,channel
    await client.wait_until_ready(); channel=client.get_channel(CHANNEL) or await client.fetch_channel(CHANNEL)
    while not client.is_closed():
        try:
            m=await find_market()
            if m and (not market or m["ticker"]!=market.get("ticker")):
                old=message; market=m; state={}; state_updated=0.0
                if feed_task: feed_task.cancel(); await asyncio.gather(feed_task,return_exceptions=True)
                if old:
                    try:await old.delete()
                    except discord.HTTPException:pass
                message=None; await rest_refresh(); await ensure_message(); feed_task=asyncio.create_task(feed(m["ticker"]),name=f"kalshi-feed-{m['ticker']}"); log.info("Rolled to %s",m["ticker"])
            elif m: market.update(m)
            if feed_task and feed_task.done():
                err=feed_task.exception() if not feed_task.cancelled() else None; log.warning("Feed task stopped unexpectedly: %r; restarting",err); feed_task=asyncio.create_task(feed(market["ticker"]),name=f"kalshi-feed-{market['ticker']}")
        except asyncio.CancelledError:raise
        except Exception:log.exception("Manager pass failed")
        await asyncio.sleep(2)

async def renderer():
    while not client.is_closed():
        try:
            if market: await ensure_message()
        except asyncio.CancelledError:raise
        except Exception:log.exception("Renderer pass failed")
        await asyncio.sleep(INTERVAL)

async def watchdog():
    global feed_task
    while not client.is_closed():
        try:
            if market:
                age=time.monotonic()-state_updated if state_updated else 999
                if age>STALE:
                    log.warning("Ticker data stale for %.1fs; refreshing REST and recycling socket",age); await rest_refresh()
                    if feed_task: feed_task.cancel(); await asyncio.gather(feed_task,return_exceptions=True)
                    feed_task=asyncio.create_task(feed(market["ticker"]),name=f"kalshi-feed-{market['ticker']}")
        except asyncio.CancelledError:raise
        except Exception:log.exception("Watchdog pass failed")
        await asyncio.sleep(max(4,STALE/2))

@client.event
async def on_ready():
    global started
    log.info("Discord ready: %s",client.user)
    if not started:
        started=True; asyncio.create_task(manager(),name="market-manager"); asyncio.create_task(renderer(),name="discord-renderer"); asyncio.create_task(watchdog(),name="feed-watchdog")
async def main():
    global http
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=12)) as s:
        http=s; await client.start(TOKEN)
if __name__=="__main__":asyncio.run(main())
