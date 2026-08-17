KALSHI BTC 15M DISCORD BOT

Required Railway variables:
DISCORD_TOKEN
DISCORD_CHANNEL_ID

Optional:
KALSHI_SERIES=KXBTC15M
KALSHI_POLL_INTERVAL=1.0
DATA_DIR=/data  (use this only if you attach a Railway Volume at /data)

Local test:
1. Copy .env.example to .env
2. Fill DISCORD_TOKEN and DISCORD_CHANNEL_ID
3. pip install -r requirements.txt
4. python bot.py

Railway:
- Deploy this folder/repo as a persistent service.
- Start command: python bot.py
- Do not generate a public domain; this is a worker.
- Add DISCORD_TOKEN and DISCORD_CHANNEL_ID in Variables.
- For persistent market history, add a Volume mounted at /data and set DATA_DIR=/data.
