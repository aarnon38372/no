KALSHI LIVE MARKET — FIXED BUILD

Fixes:
- Supervised manager, renderer, and watchdog loops so one exception cannot permanently stop updates.
- Connected-but-silent WebSocket detection.
- Automatic WebSocket recycle when ticker data goes stale.
- REST refresh fallback during reconnects.
- Automatic feed-task restart if it exits unexpectedly.
- Safe Discord message recreation if the live card is deleted.
- Automatic 15-minute market rollover and old-message cleanup.
- Cleaner mobile-friendly UP/DOWN card with feed age and LIVE/RECOVERING state.

Existing Railway variables still work. Optional tuning:
DISCORD_UPDATE_INTERVAL=1.25
KALSHI_STALE_SECONDS=15
KALSHI_SERIES=KXBTC15M
