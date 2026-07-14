# Kalshi Bot v3 — Convergence + Lag

Short-horizon trading bot for Kalshi BTC/ETH/SOL 15-minute **and** hourly
crypto markets. Rewritten from v2 after live losses traced to five concrete
bugs.

## Why v2 lost money live (all fixed in v3)

1. **Order semantics bug (the big one).** Kalshi's V2 order endpoint quotes
   *everything on the YES leg*: `side="bid"` buys YES, `side="ask"` sells YES
   (≡ buys NO at `1 − price`). v2 placed NO orders with the **NO price** on the
   YES leg, i.e. it *sold YES at the NO price* — so every DOWN trade either
   crossed the book instantly at a terrible level or rested at a nonsense
   price. This alone explains "losses filled, wins blocked."
2. **Unauthenticated cancels.** The resolver sent `DELETE` requests with no
   signature → silent 401s → stale resting orders sat in the book and got
   picked off. Cancels are now signed, and resting orders also carry a
   server-side `expiration_time` so they die on their own.
3. **Wrong-side depth.** v2 read `book["yes"]` as YES-ask liquidity. Both
   orderbook arrays are *bids*; the ask ladder must be derived from the
   opposite side (`yes_ask = 100 − best_no_bid`). Depth checks were reading
   the wrong numbers.
4. **Fantasy paper fills.** v2 paper mode assumed 100% fills at the quoted
   price. That is why paper looked incredible and live was a bloodbath. v3
   paper mode walks the **real** order book for takers and fills makers **only**
   when real trades print through the price (with a queue haircut). Paper now
   predicts live.
5. **Momentum chasing.** Buying *after* a move, at prices market makers have
   already repriced, is structurally negative-EV. v3 never trades on momentum
   alone — it only acts when its own fair-value model says the *available*
   price is mispriced net of fees.

## Strategies

**Convergence** — the fill workhorse. In the final `CONV_MIN_TAU … CONV_MAX_TAU`
seconds of both 15-minute and hourly markets (the 1H markets are only touched
inside their last few minutes, giving you 1H-grade liquidity with 15-minute
exposure — the answer to "1-hour markets aren't a solution"), buy heavy
favourites (`model p ≥ CONV_MIN_P`) when the price offers `≥ CONV_MIN_EV` edge
net of fees. If the ask is too rich, post a **self-expiring post-only bid** at
your model price instead of crossing the spread.

**Lag** — the opportunist (replaces momentum). On a vol-normalized spot burst
(`z ≥ LAG_Z`), take liquidity **only if** the book still lags fair value by
`≥ LAG_MIN_EV` net of taker fees. If the market makers already repriced (they
usually have), the bot stands down. One shot per crypto per `LAG_COOLDOWN_S`.

Fair value comes from a driftless diffusion model:
`p = Φ(ln(S/K) / (σ₁ₛ·√τ·tail))`, with `σ₁ₛ` from an EWMA of Coinbase
WebSocket tick returns (KuCoin REST fallback) and `tail = TAIL_MULT` widening
the normal to respect crypto's fat tails. Pricing is suppressed when the
fast/slow vol ratio exceeds `SPIKE_MAX` (regime too unstable to trust).

## Risk controls

Per-trade caps (`MAX_STAKE_USD`, `STAKE_MAX_CONTRACTS`), never more than
`DEPTH_FRACTION` of visible depth, `MAX_CONCURRENT` positions, one per crypto,
a hard `MAX_DAILY_LOSS_USD` halt, model-based stop-loss (`STOP_P`), UTC news
blackout windows, and a Telegram kill switch.

## Run

```bash
pip install -r requirements.txt
cp .env.example .env      # fill in credentials, keep PAPER_MODE=true
python bot.py
```

Telegram: `/start` opens the control panel (P&L, per-strategy breakdown,
positions, **real** fill-rate stats, balance, settings, kill/resume).

**Run paper for a few days first.** The v3 paper simulator is honest, so its
numbers will be far lower than v2's fantasy — and far closer to what live
actually does. Only flip `PAPER_MODE=false` once paper shows a positive,
stable edge with fill rates you're happy with.

## Persistence on Railway

Railway's filesystem is ephemeral — the SQLite DB resets on redeploy unless you
attach a **Volume** and point `DB_PATH` at it (e.g. `/data/kalshi_v3.db`).
Without a volume, trade history and open-position recovery reset on each deploy.

## Files

- `bot.py` — orchestrator, strategies, risk, Telegram UI
- `engine.py` — spot feeds, vol, fair value, fees, honest paper broker, storage
- `kalshi_client.py` — signed Kalshi V2 client with correct YES-leg semantics
