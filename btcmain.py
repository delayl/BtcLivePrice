"""
BTC Live Price — Multi-source aggregator with volatility, chart, and Discord alerts.
"""

import json
import sys
import os
import time
import random
import math
import threading
import statistics
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone


class C:
    RESET   = "\033[0m"
    LBLUE   = "\033[94m"
    LBLUE_B = "\033[1;94m"
    GREEN   = "\033[92m"
    RED     = "\033[91m"
    YELLOW  = "\033[93m"
    GRAY    = "\033[90m"
    WHITE   = "\033[97m"
    CYAN    = "\033[96m"
    MAGENTA = "\033[95m"


def enable_ansi():
    if sys.platform == "win32":
        try:
            import ctypes
            k = ctypes.windll.kernel32
            k.SetConsoleMode(k.GetStdHandle(-11), 7)
        except Exception:
            pass


enable_ansi()


DEFAULT_CONFIG = {
    "discord": {
        "enabled": False,
        "webhook_url": "",
        "username": "BTC Price Bot",
        "avatar_url": "",
        "notify_on_start": True,
        "notify_on_stop": True,
        "alerts": {
            "price_move_pct": 0.5,
            "volatility_spike": 2.0,
            "spread_wide_usd": 50.0,
            "sources_disagree_min": 5,
            "cooldown_seconds": 60,
        },
    },
    "chart": {
        "enabled": False,
        "window_seconds": 120,
        "update_every_seconds": 2,
        "show_aggression": True,
        "show_volatility": True,
    },
    "volatility": {
        "window_seconds": 300,
        "print_every_seconds": 5,
    },
}


def load_config(path="config.json"):
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                user = json.load(f)
            for section, val in user.items():
                if isinstance(val, dict) and isinstance(cfg.get(section), dict):
                    cfg[section].update(val)
                    if "alerts" in val and isinstance(cfg[section].get("alerts"), dict):
                        cfg[section]["alerts"].update(val["alerts"])
                else:
                    cfg[section] = val
            print(f"[config] loaded {path}")
        except Exception as e:
            print(f"[config] error reading {path}: {e} — using defaults")
    else:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(DEFAULT_CONFIG, f, indent=2)
            print(f"[config] created {path} with defaults")
        except Exception as e:
            print(f"[config] could not write {path}: {e}")
    return cfg


CONFIG = load_config()

REQUEST_TIMEOUT = 4
MAX_AGE_SECONDS = 30
CROSS_VALIDATE_TOLERANCE = 0.01
MIN_AGREEING_SOURCES = 2
USER_AGENT = "Mozilla/5.0 (BTC-Price-Aggregator/1.0)"

SOURCE_MIN_INTERVAL = {
    "CoinGecko":  60.0,
    "Bitstamp":   15.0,
    "Kraken":      2.0,
    "Bitfinex":    2.0,
    "Gemini":      2.0,
}

_last_fetch = {}
_cached = {}
_backoff = {}


def mark_rate_limited(name, seconds=60):
    _backoff[name] = time.time() + seconds + random.uniform(0, 5)


def http_get_json(url, timeout=REQUEST_TIMEOUT):
    req = Request(url, headers={"User-Agent": USER_AGENT,
                                "Accept": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return json.loads(raw)


def http_post_json(url, payload, timeout=REQUEST_TIMEOUT):
    body = json.dumps(payload).encode("utf-8")
    req = Request(url, data=body, method="POST",
                  headers={"User-Agent": USER_AGENT,
                           "Content-Type": "application/json"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", "ignore")


def fetch_binance():
    d = http_get_json("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT")
    return {"price": float(d["price"]), "ts": time.time(), "raw": d}

def fetch_coinbase():
    d = http_get_json("https://api.coinbase.com/v2/prices/BTC-USD/spot")
    return {"price": float(d["data"]["amount"]), "ts": time.time(), "raw": d}

def fetch_kraken():
    d = http_get_json("https://api.kraken.com/0/public/Ticker?pair=XBTUSD")
    r = d["result"]; key = next(iter(r))
    return {"price": float(r[key]["c"][0]), "ts": time.time(), "raw": d}

def fetch_bitstamp():
    d = http_get_json("https://www.bitstamp.net/api/v2/ticker/btcusd/")
    return {"price": float(d["last"]),
            "ts": float(d.get("timestamp", time.time())), "raw": d}

def fetch_okx():
    d = http_get_json("https://www.okx.com/api/v5/market/ticker?instId=BTC-USDT")
    return {"price": float(d["data"][0]["last"]),
            "ts": float(d["data"][0]["ts"]) / 1000.0, "raw": d}

def fetch_bybit():
    d = http_get_json("https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT")
    item = d["result"]["list"][0]
    return {"price": float(item["lastPrice"]), "ts": time.time(), "raw": d}

def fetch_bitfinex():
    d = http_get_json("https://api-pub.bitfinex.com/v2/ticker/tBTCUSD")
    return {"price": float(d[6]), "ts": time.time(), "raw": d}

def fetch_gemini():
    d = http_get_json("https://api.gemini.com/v1/pubticker/btcusd")
    return {"price": float(d["last"]), "ts": time.time(), "raw": d}

def fetch_kucoin():
    d = http_get_json("https://api.kucoin.com/api/v1/market/orderbook/level1?symbol=BTC-USDT")
    ts = float(d["data"].get("time", int(time.time() * 1000))) / 1000.0
    return {"price": float(d["data"]["price"]), "ts": ts, "raw": d}

def fetch_coingecko():
    d = http_get_json("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_last_updated_at=true")
    return {"price": float(d["bitcoin"]["usd"]),
            "ts": float(d["bitcoin"].get("last_updated_at", time.time())),
            "raw": d}

def fetch_coincap():
    d = http_get_json("https://api.coincap.io/v2/assets/bitcoin")
    return {"price": float(d["data"]["priceUsd"]),
            "ts": float(d["data"].get("timestamp", int(time.time() * 1000))) / 1000.0,
            "raw": d}

def fetch_cryptocompare():
    d = http_get_json("https://min-api.cryptocompare.com/data/price?fsym=BTC&tsyms=USD")
    return {"price": float(d["USD"]), "ts": time.time(), "raw": d}

def fetch_bitget():
    d = http_get_json("https://api.bitget.com/api/v2/spot/market/tickers?symbol=BTCUSDT")
    item = d["data"][0]
    ts = float(item.get("ts", int(time.time() * 1000))) / 1000.0
    return {"price": float(item["lastPr"]), "ts": ts, "raw": d}

def fetch_mexc():
    d = http_get_json("https://api.mexc.com/api/v3/ticker/price?symbol=BTCUSDT")
    return {"price": float(d["price"]), "ts": time.time(), "raw": d}

def fetch_htx():
    d = http_get_json("https://api.huobi.pro/market/trade?symbol=btcusdt")
    item = d["tick"]["data"][0]
    return {"price": float(item["price"]),
            "ts": float(item["ts"]) / 1000.0, "raw": d}

def fetch_poloniex():
    d = http_get_json("https://api.poloniex.com/markets/BTC_USDT/price")
    return {"price": float(d["price"]),
            "ts": float(d.get("time", int(time.time() * 1000))) / 1000.0,
            "raw": d}

def fetch_whitebit():
    d = http_get_json("https://whitebit.com/api/v4/public/ticker")
    return {"price": float(d["BTC_USDT"]["last_price"]),
            "ts": time.time(), "raw": d}


SOURCES = {
    "Binance":       fetch_binance,
    "Coinbase":      fetch_coinbase,
    "Kraken":        fetch_kraken,
    "Bitstamp":      fetch_bitstamp,
    "OKX":           fetch_okx,
    "Bybit":         fetch_bybit,
    "Bitfinex":      fetch_bitfinex,
    "Gemini":        fetch_gemini,
    "KuCoin":        fetch_kucoin,
    "CoinGecko":     fetch_coingecko,
    "CoinCap":       fetch_coincap,
    "CryptoCompare": fetch_cryptocompare,
    "Bitget":        fetch_bitget,
    "MEXC":          fetch_mexc,
    "HTX (Huobi)":   fetch_htx,
    "Poloniex":      fetch_poloniex,
    "WhiteBIT":      fetch_whitebit,
}


def fetch_all(timeout=REQUEST_TIMEOUT + 2):
    results = {}
    now = time.time()

    def worker(name, fn):
        if now < _backoff.get(name, 0):
            cached = _cached.get(name)
            if cached:
                return name, cached
            return name, {"ok": False, "error": "backoff", "latency": 0.0}

        min_int = SOURCE_MIN_INTERVAL.get(name, 0)
        last = _last_fetch.get(name, 0)
        if min_int > 0 and (now - last) < min_int and name in _cached:
            return name, _cached[name]

        t0 = time.perf_counter()
        try:
            data = fn()
            res = {"ok": True, "data": data,
                   "latency": time.perf_counter() - t0}
            _last_fetch[name] = time.time()
            _cached[name] = res
            return name, res
        except HTTPError as e:
            if e.code == 429:
                mark_rate_limited(name, 60)
                err = "429 rate limited"
            else:
                err = f"HTTP {e.code}"
            return name, {"ok": False, "error": err,
                          "latency": time.perf_counter() - t0}
        except Exception as e:
            return name, {"ok": False,
                          "error": f"{type(e).__name__}: {e}",
                          "latency": time.perf_counter() - t0}

    with ThreadPoolExecutor(max_workers=len(SOURCES)) as ex:
        futures = [ex.submit(worker, name, fn)
                   for name, fn in SOURCES.items()]
        try:
            for fut in as_completed(futures, timeout=timeout):
                try:
                    name, res = fut.result(timeout=0.1)
                    results[name] = res
                except Exception:
                    pass
        except Exception:
            pass

    for name in SOURCES:
        if name not in results:
            if name in _cached:
                results[name] = _cached[name]
            else:
                results[name] = {"ok": False, "error": "timeout",
                                 "latency": REQUEST_TIMEOUT}
    return results


def aggregate(results):
    now = time.time()
    valid = []

    for name, res in results.items():
        if not res["ok"]:
            continue
        d = res["data"]
        price = d.get("price")
        ts = d.get("ts", now)
        age = now - ts
        if price is None or price <= 0:
            continue
        if age > MAX_AGE_SECONDS:
            continue
        valid.append({"name": name, "price": price,
                      "age": age, "latency": res["latency"]})

    if not valid:
        return None

    prices = [v["price"] for v in valid]
    median = statistics.median(prices)

    agreeing = [v for v in valid
                if abs(v["price"] - median) / median <= CROSS_VALIDATE_TOLERANCE]
    outliers = [v for v in valid if v not in agreeing]

    if len(agreeing) < MIN_AGREEING_SOURCES:
        agreeing = valid
        outliers = []

    best_price = statistics.median([v["price"] for v in agreeing])
    freshest = min(agreeing, key=lambda v: v["age"])

    max_p = max(v["price"] for v in valid)
    min_p = min(v["price"] for v in valid)

    return {
        "best_price": best_price,
        "agreeing": agreeing,
        "outliers": outliers,
        "freshest": freshest,
        "num_sources": len(valid),
        "num_agreeing": len(agreeing),
        "max_price": max_p,
        "min_price": min_p,
        "spread": max_p - min_p,
    }


class VolatilityTracker:
    def __init__(self, window_seconds=300):
        self.window_seconds = window_seconds
        self.samples = deque()
        self.returns = deque()
        self.max_window_returns = 2000

    def add(self, price, ts=None):
        ts = ts or time.time()
        self.samples.append((ts, price))
        cutoff = ts - self.window_seconds
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

        if len(self.samples) >= 2:
            p0 = self.samples[-2][1]
            p1 = price
            if p0 > 0:
                r = (p1 - p0) / p0
                self.returns.append(r)
                while len(self.returns) > self.max_window_returns:
                    self.returns.popleft()

    def stats(self):
        if len(self.samples) < 3:
            return {
                "volatility": 0.0,
                "annualized": 0.0,
                "range": 0.0,
                "range_pct": 0.0,
                "ticks": 0,
                "up_ticks": 0,
                "down_ticks": 0,
                "aggression": 0.0,
            }

        prices = [p for _, p in self.samples]
        rets = list(self.returns)

        vol = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        annualized = vol * math.sqrt(31536000)

        lo, hi = min(prices), max(prices)
        rng = hi - lo
        rng_pct = (rng / lo * 100) if lo > 0 else 0.0

        up = sum(1 for r in rets if r > 0)
        dn = sum(1 for r in rets if r < 0)
        total = up + dn
        aggression = (up - dn) / total if total > 0 else 0.0

        return {
            "volatility": vol,
            "annualized": annualized,
            "range": rng,
            "range_pct": rng_pct,
            "ticks": total,
            "up_ticks": up,
            "down_ticks": dn,
            "aggression": aggression,
        }


class AggressionTracker:
    def __init__(self, window_seconds=120):
        self.window_seconds = window_seconds
        self.bars = deque()
        self._last_price_by_source = {}

    def add(self, results):
        buy = 0.0
        sell = 0.0
        now = time.time()

        for name, res in results.items():
            if not res["ok"]:
                continue
            p = res["data"]["price"]
            prev = self._last_price_by_source.get(name)
            self._last_price_by_source[name] = p
            if prev is None or prev <= 0:
                continue
            delta = p - prev
            w = abs(delta)
            if delta > 0:
                buy += w
            elif delta < 0:
                sell += w

        scale = 10000.0
        buy *= scale
        sell *= scale
        self.bars.append((now, buy, sell, buy - sell))
        cutoff = now - self.window_seconds
        while self.bars and self.bars[0][0] < cutoff:
            self.bars.popleft()

    def series(self):
        if not self.bars:
            return [], [], []
        ts = [b[0] for b in self.bars]
        buy = [b[1] for b in self.bars]
        sell = [b[2] for b in self.bars]
        return ts, buy, sell

    def delta_series(self):
        return [b[3] for b in self.bars]


class DiscordNotifier:
    def __init__(self, cfg):
        self.cfg = cfg
        self.enabled = bool(cfg.get("enabled") and cfg.get("webhook_url"))
        self.webhook_url = cfg.get("webhook_url", "")
        self.username = cfg.get("username", "BTC Price Bot")
        self.avatar_url = cfg.get("avatar_url", "")
        self.alerts = cfg.get("alerts", {})
        self._last_alert = {}

    def _cooldown_ok(self, kind):
        cd = float(self.alerts.get("cooldown_seconds", 60))
        last = self._last_alert.get(kind, 0)
        if time.time() - last < cd:
            return False
        self._last_alert[kind] = time.time()
        return True

    def send(self, content=None, embed=None):
        if not self.enabled:
            return False
        payload = {"username": self.username}
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url
        if content:
            payload["content"] = content
        if embed:
            payload["embeds"] = [embed]
        try:
            status, _ = http_post_json(self.webhook_url, payload)
            return 200 <= status < 300
        except Exception as e:
            print(f"{C.RED}[discord] send failed: {e}{C.RESET}")
            return False

    def notify_start(self, price):
        if not self.cfg.get("notify_on_start", True):
            return
        self.send(embed={
            "title": "BTC Price Bot started",
            "description": f"Current price: **${price:,.2f}**",
            "color": 0x3498db,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def notify_stop(self, price):
        if not self.cfg.get("notify_on_stop", True):
            return
        self.send(embed={
            "title": "BTC Price Bot stopped",
            "description": f"Last price: **${price:,.2f}**",
            "color": 0x95a5a6,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def check_alerts(self, agg, vol_stats, prev_price):
        if not self.enabled or agg is None:
            return

        threshold = float(self.alerts.get("price_move_pct", 0.5))
        if prev_price:
            pct = (agg["best_price"] - prev_price) / prev_price * 100
            if abs(pct) >= threshold and self._cooldown_ok("price_move"):
                color = 0x2ecc71 if pct > 0 else 0xe74c3c
                arrow = "UP" if pct > 0 else "DOWN"
                self.send(embed={
                    "title": f"Price move {arrow} {pct:+.2f}%",
                    "description": f"BTC is now **${agg['best_price']:,.2f}**",
                    "color": color,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "fields": [
                        {"name": "Previous", "value": f"${prev_price:,.2f}", "inline": True},
                        {"name": "Sources", "value": f"{agg['num_agreeing']}/{agg['num_sources']}", "inline": True},
                    ],
                })

        spike_thresh = float(self.alerts.get("volatility_spike", 2.0))
        ann = vol_stats.get("annualized", 0.0)
        if ann >= spike_thresh and self._cooldown_ok("vol_spike"):
            self.send(embed={
                "title": "Volatility spike",
                "description": f"Annualized vol: **{ann*100:.2f}%**",
                "color": 0xf39c12,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        spread_thresh = float(self.alerts.get("spread_wide_usd", 50.0))
        if agg.get("spread", 0) >= spread_thresh and self._cooldown_ok("spread"):
            self.send(embed={
                "title": "Wide cross-exchange spread",
                "description": f"Spread: **${agg['spread']:,.2f}**",
                "color": 0x9b59b6,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "fields": [
                    {"name": "High", "value": f"${agg['max_price']:,.2f}", "inline": True},
                    {"name": "Low", "value": f"${agg['min_price']:,.2f}", "inline": True},
                ],
            })

        disagree_thresh = int(self.alerts.get("sources_disagree_min", 5))
        n_outliers = len(agg.get("outliers", []))
        if n_outliers >= disagree_thresh and self._cooldown_ok("disagree"):
            self.send(embed={
                "title": "Sources disagreeing",
                "description": f"{n_outliers} source(s) outside ±1% of median",
                "color": 0xe67e22,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })


class LiveChartWindow:
    def __init__(self, cfg, volatility, aggression):
        self.cfg = cfg
        self.vol = volatility
        self.agg = aggression
        self.enabled = bool(cfg.get("enabled"))
        self.window_seconds = float(cfg.get("window_seconds", 120))
        self.update_every = float(cfg.get("update_every_seconds", 2))
        self.show_aggression = bool(cfg.get("show_aggression", True))
        self.show_volatility = bool(cfg.get("show_volatility", True))
        self.prices = deque()
        self._stop = threading.Event()
        self._thread = None

    def add_price(self, price):
        now = time.time()
        self.prices.append((now, price))
        cutoff = now - self.window_seconds
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    def start(self):
        if not self.enabled:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run(self):
        try:
            import matplotlib
            matplotlib.use("TkAgg")
            import matplotlib.pyplot as plt
            from matplotlib.animation import FuncAnimation
        except Exception as e:
            print(f"{C.RED}[chart] matplotlib not available: {e}{C.RESET}")
            print(f"{C.GRAY}       install with: pip install matplotlib{C.RESET}")
            return

        n_panels = 1 + (1 if self.show_aggression else 0) + \
                   (1 if self.show_volatility else 0)

        fig, axes = plt.subplots(n_panels, 1, figsize=(9, 2.2 * n_panels),
                                 sharex=False)
        if n_panels == 1:
            axes = [axes]
        fig.suptitle("BTC — Live", color="#3498db", fontsize=11)

        ax_price = axes[0]
        ax_agg = axes[1] if self.show_aggression and n_panels >= 2 else None
        ax_vol = None
        if self.show_volatility:
            idx = 1 + (1 if self.show_aggression else 0)
            if idx < len(axes):
                ax_vol = axes[idx]

        for ax in axes:
            ax.set_facecolor("#0d1117")
            ax.tick_params(colors="#8b949e", labelsize=8)
            for spine in ax.spines.values():
                spine.set_color("#30363d")
            ax.grid(True, color="#21262d", linewidth=0.5)

        fig.patch.set_facecolor("#0d1117")

        def update(_frame):
            now = time.time()

            if len(self.prices) >= 2:
                ts = [t for t, _ in self.prices]
                ps = [p for _, p in self.prices]
                xs = [(t - now) for t in ts]
                ax_price.clear()
                ax_price.set_facecolor("#0d1117")
                ax_price.plot(xs, ps, color="#58a6ff", linewidth=1.4)
                ax_price.set_xlim(-self.window_seconds, 0)
                if ps:
                    lo, hi = min(ps), max(ps)
                    pad = max((hi - lo) * 0.1, 1.0)
                    ax_price.set_ylim(lo - pad, hi + pad)
                ax_price.set_title(f"Price  ${ps[-1]:,.2f}",
                                   color="#58a6ff", fontsize=9,
                                   loc="left")
                ax_price.tick_params(colors="#8b949e", labelsize=7)
                ax_price.grid(True, color="#21262d", linewidth=0.5)

            if ax_agg is not None:
                ts, buy, sell = self.agg.series()
                ax_agg.clear()
                ax_agg.set_facecolor("#0d1117")
                if ts:
                    now2 = time.time()
                    xs = [(t - now2) for t in ts]
                    deltas = self.agg.delta_series()
                    colors = ["#2ea043" if d >= 0 else "#da3633" for d in deltas]
                    ax_agg.bar(xs, deltas, width=0.8, color=colors)
                    ax_agg.axhline(0, color="#30363d", linewidth=0.6)
                    ax_agg.set_xlim(-self.window_seconds, 0)

                    max_abs = max((abs(d) for d in deltas), default=1.0)
                    ax_agg.set_ylim(-max_abs * 1.2, max_abs * 1.2)
                    net = sum(deltas)
                    ax_agg.set_title(
                        f"Aggression (buy vs sell)  net={net:+.1f}",
                        color="#8b949e", fontsize=9, loc="left")
                ax_agg.tick_params(colors="#8b949e", labelsize=7)
                ax_agg.grid(True, color="#21262d", linewidth=0.5)

            if ax_vol is not None:
                st = self.vol.stats()
                ax_vol.clear()
                ax_vol.set_facecolor("#0d1117")
                labels = ["Vol (1σ)", "Range %", "Aggr"]
                vals = [st["volatility"] * 10000,
                        st["range_pct"],
                        st["aggression"] * 100]
                colors = ["#d29922", "#58a6ff",
                          "#2ea043" if st["aggression"] >= 0 else "#da3633"]
                ax_vol.bar(labels, vals, color=colors)
                ax_vol.set_title(
                    f"Volatility  annualized={st['annualized']*100:.1f}%"
                    f"  up/dn={st['up_ticks']}/{st['down_ticks']}",
                    color="#d29922", fontsize=9, loc="left")
                ax_vol.tick_params(colors="#8b949e", labelsize=7)
                ax_vol.grid(True, color="#21262d", linewidth=0.5, axis="y")

            fig.tight_layout(rect=[0, 0, 1, 0.96])

        try:
            self._anim = FuncAnimation(
                fig, update,
                interval=int(self.update_every * 1000),
                cache_frame_data=False)
            plt.show()
        except Exception as e:
            print(f"{C.RED}[chart] error: {e}{C.RESET}")


def fmt_price(p):
    return f"${p:,.2f}"


def print_report(results, agg, vol_stats, discord):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print()
    print(C.LBLUE_B + "  BTC/USD Price Aggregator" + C.RESET
          + C.GRAY + f"   {ts}" + C.RESET)
    print()

    if agg is None:
        print(C.RED + "  No valid sources returned a price." + C.RESET)
        print()
        return

    print(f"  {C.WHITE}BEST PRICE{C.RESET}   "
          f"{C.LBLUE_B}{fmt_price(agg['best_price'])}{C.RESET}"
          f"   {C.GRAY}({agg['num_agreeing']}/{agg['num_sources']} sources agree){C.RESET}")
    print(f"  {C.WHITE}Freshest{C.RESET}     "
          f"{C.CYAN}{agg['freshest']['name']}{C.RESET}  "
          f"{C.GRAY}age {agg['freshest']['age']*1000:.0f} ms"
          f"  latency {agg['freshest']['latency']*1000:.0f} ms{C.RESET}")
    print(f"  {C.WHITE}Spread{C.RESET}       "
          f"{C.MAGENTA}${agg['spread']:,.2f}{C.RESET}"
          f"   {C.GRAY}(hi ${agg['max_price']:,.2f}  /  lo ${agg['min_price']:,.2f}){C.RESET}")
    print()

    ann = vol_stats["annualized"] * 100
    vol = vol_stats["volatility"] * 10000
    rng_pct = vol_stats["range_pct"]
    aggr = vol_stats["aggression"]
    aggr_color = C.GREEN if aggr >= 0 else C.RED
    print(f"  {C.WHITE}Volatility{C.RESET}   "
          f"σ={C.YELLOW}{vol:.2f}{C.RESET}"
          f"   annualized={C.YELLOW}{ann:.1f}%{C.RESET}"
          f"   range={C.YELLOW}{rng_pct:.2f}%{C.RESET}"
          f"   up/dn={C.GREEN}{vol_stats['up_ticks']}{C.RESET}/"
          f"{C.RED}{vol_stats['down_ticks']}{C.RESET}"
          f"   aggr={aggr_color}{aggr:+.2f}{C.RESET}")
    print()

    print(C.LBLUE + f"  {'Source':<16}{'Price':>14}"
                   f"{'Age':>10}{'Latency':>12}   {'Status'}"
          + C.RESET)
    print()

    for name in sorted(results.keys()):
        res = results[name]
        if not res["ok"]:
            print(f"  {C.GRAY}{name:<16}{'—':>14}"
                  f"{'—':>10}{res['latency']*1000:>10.0f}ms   "
                  f"{C.RED}{res['error'][:26]}{C.RESET}")
            continue

        d = res["data"]
        price = d["price"]
        age_ms = (time.time() - d.get("ts", time.time())) * 1000
        lat_ms = res["latency"] * 1000

        if any(v["name"] == name for v in agg["agreeing"]):
            status = f"{C.GREEN}agree{C.RESET}"
        elif any(v["name"] == name for v in agg["outliers"]):
            status = f"{C.YELLOW}outlier{C.RESET}"
        else:
            status = f"{C.GRAY}stale{C.RESET}"

        print(f"  {C.LBLUE}{name:<16}{C.RESET}"
              f"{C.WHITE}{fmt_price(price):>14}{C.RESET}"
              f"{C.GRAY}{age_ms:>8.0f}ms"
              f"{lat_ms:>10.0f}ms{C.RESET}   {status}")

    if discord.enabled:
        print()
        print(f"  {C.GRAY}Discord notifications: {C.GREEN}enabled{C.RESET}")

    print()


def get_btc_price():
    results = fetch_all()
    agg = aggregate(results)
    return results, agg


def run_continuous(interval=1.0, show_table_every=30.0):
    print(C.LBLUE_B + "  Starting live BTC price polling. Ctrl+C to stop."
          + C.RESET + "\n")

    vol = VolatilityTracker(window_seconds=CONFIG["volatility"]["window_seconds"])
    aggression = AggressionTracker(window_seconds=CONFIG["chart"]["window_seconds"])
    discord = DiscordNotifier(CONFIG["discord"])

    chart = LiveChartWindow(CONFIG["chart"], vol, aggression)
    chart.start()

    last_full = 0.0
    last_vol_print = 0.0
    last_len = 0
    prev_price = None
    last_price = None

    try:
        while True:
            results, agg = get_btc_price()

            if agg is not None:
                price = agg["best_price"]
                last_price = price
                vol.add(price)
                aggression.add(results)
                chart.add_price(price)

                discord.check_alerts(agg, vol.stats(), prev_price)
                prev_price = price

                st = vol.stats()
                arrow = ""
                if prev_price is not None:
                    if price > prev_price:
                        arrow = f"{C.GREEN}UP{C.RESET}"
                    elif price < prev_price:
                        arrow = f"{C.RED}DN{C.RESET}"

                line = (f"  {C.LBLUE}BTC:{C.RESET} "
                        f"{C.WHITE}{fmt_price(price)}{C.RESET}"
                        f" {arrow}"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"{agg['num_agreeing']}/{agg['num_sources']} agree"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"σ={C.YELLOW}{st['volatility']*10000:.2f}{C.RESET}"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"aggr={C.GREEN if st['aggression']>=0 else C.RED}"
                        f"{st['aggression']:+.2f}{C.RESET}"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"{C.CYAN}{agg['freshest']['name']}{C.RESET}")
            else:
                line = f"  {C.RED}BTC: no data{C.RESET}"

            sys.stdout.write("\r" + line.ljust(last_len))
            sys.stdout.flush()
            last_len = len(line) + 40

            now = time.time()
            if now - last_full >= show_table_every:
                sys.stdout.write("\n")
                print_report(results, agg, vol.stats(), discord)
                last_full = now
                last_len = 0

            if now - last_vol_print >= CONFIG["volatility"]["print_every_seconds"]:
                st = vol.stats()
                if st["ticks"] > 5:
                    print(f"\n  {C.GRAY}vol update: "
                          f"σ={st['volatility']*10000:.2f}  "
                          f"ann={st['annualized']*100:.1f}%  "
                          f"range={st['range_pct']:.2f}%  "
                          f"up/dn={st['up_ticks']}/{st['down_ticks']}  "
                          f"aggr={st['aggression']:+.2f}{C.RESET}")
                last_vol_print = now

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n" + C.GRAY + "  Stopping..." + C.RESET)
        chart.stop()
        if last_price is not None:
            discord.notify_stop(last_price)
        print(C.GRAY + "  Stopped." + C.RESET)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "once"

    if mode == "live":
        if CONFIG["discord"].get("enabled") and CONFIG["discord"].get("notify_on_start", True):
            try:
                results, agg = get_btc_price()
                if agg:
                    DiscordNotifier(CONFIG["discord"]).notify_start(agg["best_price"])
            except Exception:
                pass
        run_continuous(interval=1.0, show_table_every=30.0)
    else:
        results, agg = get_btc_price()
        vol = VolatilityTracker(window_seconds=CONFIG["volatility"]["window_seconds"])
        if agg:
            vol.add(agg["best_price"])
        discord = DiscordNotifier(CONFIG["discord"])
        print_report(results, agg, vol.stats(), discord)
