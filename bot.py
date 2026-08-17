import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import discord
from dotenv import load_dotenv

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
DISCORD_CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID", "0"))
KALSHI_SERIES = os.getenv("KALSHI_SERIES", "KXBTC15M").strip()
KALSHI_POLL_INTERVAL = float(os.getenv("KALSHI_POLL_INTERVAL", "1.0"))
DATA_DIR = Path(os.getenv("DATA_DIR", "data"))

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_FALLBACK_BASE = "https://external-api.kalshi.com/trade-api/v2"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
COINBASE_PRODUCT = "BTC-USD"

DATA_DIR.mkdir(parents=True, exist_ok=True)
HISTORY_FILE = DATA_DIR / "markets.json"
STATE_FILE = DATA_DIR / "state.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("btc15m")

intents = discord.Intents.default()
client = discord.Client(intents=intents)

http: aiohttp.ClientSession | None = None
btc_price: float | None = None
current_market: dict | None = None
current_message: discord.Message | None = None
last_signature = None
tasks_started = False


def parse_time(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    except Exception:
        return None


def load_json(path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path, obj):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    tmp.replace(path)


def price_to_cents(v):
    if v in (None, ""):
        return None
    try:
        f = float(v)
        # *_dollars values are 0..1; legacy integer values are 0..100.
        return f * 100 if f <= 1 else f
    except Exception:
        return None


def fmt_price(v):
    p = price_to_cents(v)
    if p is None:
        return "—"
    return f"{p:.1f}¢" if abs(p - round(p)) > 1e-9 else f"{int(round(p))}¢"


def field(m, dollar_name, legacy_name):
    return m.get(dollar_name) if m.get(dollar_name) is not None else m.get(legacy_name)


def fmt_volume(m):
    v = m.get("volume_fp")
    if v is None:
        v = m.get("volume")
    try:
        return f"{float(v):,.0f}"
    except Exception:
        return "—"


def remaining_seconds(m):
    close = parse_time(m.get("close_time"))
    if not close:
        return 0
    return max(0, int((close - datetime.now(timezone.utc)).total_seconds()))


def fmt_remaining(m):
    s = remaining_seconds(m)
    mm, ss = divmod(s, 60)
    return f"{mm:02d}:{ss:02d}"


def signature(m):
    return (
        m.get("ticker"),
        field(m, "yes_bid_dollars", "yes_bid"),
        field(m, "yes_ask_dollars", "yes_ask"),
        field(m, "no_bid_dollars", "no_bid"),
        field(m, "no_ask_dollars", "no_ask"),
        m.get("volume_fp", m.get("volume")),
        round(btc_price or 0, 2),
        fmt_remaining(m),
    )


def save_history(m):
    if not m:
        return
    hist = load_json(HISTORY_FILE, [])
    ticker = m.get("ticker")
    existing = next((x for x in hist if x.get("ticker") == ticker), None)
    record = {
        "ticker": ticker,
        "event_ticker": m.get("event_ticker"),
        "title": m.get("title"),
        "open_time": m.get("open_time"),
        "close_time": m.get("close_time"),
        "yes_bid": field(m, "yes_bid_dollars", "yes_bid"),
        "yes_ask": field(m, "yes_ask_dollars", "yes_ask"),
        "no_bid": field(m, "no_bid_dollars", "no_bid"),
        "no_ask": field(m, "no_ask_dollars", "no_ask"),
        "volume": m.get("volume_fp", m.get("volume")),
        "result": m.get("result"),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    if existing:
        existing.update(record)
    else:
        hist.append(record)
    save_json(HISTORY_FILE, hist[-5000:])


async def fetch_markets_from(base):
    assert http
    url = f"{base}/markets"
    params = {"series_ticker": KALSHI_SERIES, "status": "open", "limit": 100}
    async with http.get(url, params=params, timeout=aiohttp.ClientTimeout(total=8)) as r:
        if r.status != 200:
            raise RuntimeError(f"Kalshi HTTP {r.status}: {(await r.text())[:250]}")
        return (await r.json()).get("markets", [])


async def fetch_current_market():
    last_error = None
    for base in (KALSHI_BASE, KALSHI_FALLBACK_BASE):
        try:
            markets = await fetch_markets_from(base)
            now = datetime.now(timezone.utc)
            candidates = []
            for m in markets:
                close = parse_time(m.get("close_time"))
                if close and close > now:
                    candidates.append((close, m))
            if candidates:
                candidates.sort(key=lambda x: x[0])
                return candidates[0][1]
        except Exception as e:
            last_error = e
    if last_error:
        log.warning("Kalshi fetch failed: %r", last_error)
    return None


async def fetch_closed_market(ticker):
    if not ticker or not http:
        return None
    for base in (KALSHI_BASE, KALSHI_FALLBACK_BASE):
        try:
            async with http.get(
                f"{base}/markets/{ticker}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as r:
                if r.status == 200:
                    data = await r.json()
                    return data.get("market", data)
        except Exception:
            pass
    return None


async def btc_ws_loop():
    global btc_price
    assert http
    subscribe = {
        "type": "subscribe",
        "product_ids": [COINBASE_PRODUCT],
        "channels": ["ticker"],
    }
    backoff = 2
    while not client.is_closed():
        try:
            log.info("Connecting Coinbase BTC websocket...")
            async with http.ws_connect(COINBASE_WS, heartbeat=20, timeout=15) as ws:
                await ws.send_json(subscribe)
                log.info("Coinbase BTC websocket connected.")
                backoff = 2
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if data.get("type") == "ticker" and data.get("product_id") == COINBASE_PRODUCT:
                            try:
                                btc_price = float(data["price"])
                            except Exception:
                                pass
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("BTC websocket error: %r", e)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)


def build_embed(m):
    yes_bid = fmt_price(field(m, "yes_bid_dollars", "yes_bid"))
    yes_ask = fmt_price(field(m, "yes_ask_dollars", "yes_ask"))
    no_bid = fmt_price(field(m, "no_bid_dollars", "no_bid"))
    no_ask = fmt_price(field(m, "no_ask_dollars", "no_ask"))
    btc = f"${btc_price:,.2f}" if btc_price else "Connecting…"

    embed = discord.Embed(
        title="₿ BTC 15-Minute Kalshi Market",
        description=m.get("title") or m.get("subtitle") or "Live KXBTC15M market",
        color=0x5865F2,
        timestamp=datetime.now(timezone.utc),
    )
    embed.add_field(name="BTC Spot", value=f"**{btc}**", inline=False)
    embed.add_field(name="🟢 UP / YES", value=f"Bid **{yes_bid}**\nAsk **{yes_ask}**", inline=True)
    embed.add_field(name="🔴 DOWN / NO", value=f"Bid **{no_bid}**\nAsk **{no_ask}**", inline=True)
    embed.add_field(name="Volume", value=fmt_volume(m), inline=True)
    embed.add_field(name="⏱ Time Left", value=f"**{fmt_remaining(m)}**", inline=True)
    embed.add_field(name="Status", value="🟢 LIVE", inline=True)
    embed.add_field(name="Ticker", value=f"`{m.get('ticker', '—')}`", inline=False)
    embed.set_footer(text="Kalshi: 1.0s polling • BTC spot: Coinbase public WebSocket")
    return embed


async def get_channel():
    ch = client.get_channel(DISCORD_CHANNEL_ID)
    if ch is None:
        ch = await client.fetch_channel(DISCORD_CHANNEL_ID)
    return ch


async def restore_message(channel, market):
    global current_message
    state = load_json(STATE_FILE, {})
    if state.get("market_ticker") != market.get("ticker") or not state.get("message_id"):
        return False
    try:
        current_message = await channel.fetch_message(int(state["message_id"]))
        return True
    except Exception:
        return False


async def create_message(channel, market):
    global current_message
    current_message = await channel.send(embed=build_embed(market))
    save_json(STATE_FILE, {
        "message_id": current_message.id,
        "market_ticker": market.get("ticker"),
    })
    log.info("Created Discord message %s", current_message.id)


async def delete_current_message():
    global current_message
    if current_message:
        try:
            await current_message.delete()
        except discord.NotFound:
            pass
        except Exception as e:
            log.warning("Could not delete old Discord message: %r", e)
    current_message = None
    save_json(STATE_FILE, {})


async def market_loop():
    global current_market, current_message, last_signature
    await client.wait_until_ready()
    channel = await get_channel()
    log.info("Market loop started. Poll interval: %.1fs", KALSHI_POLL_INTERVAL)

    while not client.is_closed():
        started = asyncio.get_running_loop().time()
        try:
            market = await fetch_current_market()
            if not market:
                await asyncio.sleep(max(0, KALSHI_POLL_INTERVAL - (asyncio.get_running_loop().time() - started)))
                continue

            new_ticker = market.get("ticker")
            old_ticker = current_market.get("ticker") if current_market else None

            if new_ticker != old_ticker:
                if current_market:
                    final = await fetch_closed_market(old_ticker)
                    save_history(final or current_market)
                    await delete_current_message()

                current_market = market
                restored = await restore_message(channel, market)
                if not restored:
                    await create_message(channel, market)
                last_signature = None
                log.info("Tracking market %s", new_ticker)
            else:
                current_market = market

            sig = signature(market)
            if sig != last_signature and current_message:
                try:
                    await current_message.edit(embed=build_embed(market))
                    last_signature = sig
                except discord.NotFound:
                    await create_message(channel, market)
                    last_signature = sig
                except discord.HTTPException as e:
                    log.warning("Discord edit error/rate limit: %r", e)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception("Market loop error: %r", e)

        elapsed = asyncio.get_running_loop().time() - started
        await asyncio.sleep(max(0.05, KALSHI_POLL_INTERVAL - elapsed))


@client.event
async def on_ready():
    global tasks_started
    log.info("Discord connected as %s", client.user)
    if not tasks_started:
        tasks_started = True
        asyncio.create_task(btc_ws_loop())
        asyncio.create_task(market_loop())


async def main():
    global http
    if not DISCORD_TOKEN:
        raise RuntimeError("DISCORD_TOKEN is missing.")
    if not DISCORD_CHANNEL_ID:
        raise RuntimeError("DISCORD_CHANNEL_ID is missing or invalid.")

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        http = session
        await client.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
