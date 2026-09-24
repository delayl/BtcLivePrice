import json
import sys
import time
import random
import statistics
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

def enable_ansi():
    """Enable ANSI colors on Windows 10+."""
    if sys.platform == "win32":
        try:
            import ctypes
            k = ctypes.windll.kernel32
            k.SetConsoleMode(k.GetStdHandle(-11), 7)
        except Exception:
            pass

enable_ansi()

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

    return {
        "best_price": best_price,
        "agreeing": agreeing,
        "outliers": outliers,
        "freshest": freshest,
        "num_sources": len(valid),
        "num_agreeing": len(agreeing),
    }


def fmt_price(p):
    return f"${p:,.2f}"


def print_report(results, agg):
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
    print()

    # Header
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

        if agg is not None:
            if any(v["name"] == name for v in agg["agreeing"]):
                status = f"{C.GREEN}agree{C.RESET}"
            elif any(v["name"] == name for v in agg["outliers"]):
                status = f"{C.YELLOW}outlier{C.RESET}"
            else:
                status = f"{C.GRAY}stale{C.RESET}"
        else:
            status = ""

        print(f"  {C.LBLUE}{name:<16}{C.RESET}"
              f"{C.WHITE}{fmt_price(price):>14}{C.RESET}"
              f"{C.GRAY}{age_ms:>8.0f}ms"
              f"{lat_ms:>10.0f}ms{C.RESET}   {status}")

    print()


def get_btc_price():
    results = fetch_all()
    agg = aggregate(results)
    return results, agg


def run_continuous(interval=1.0, show_table_every=30.0):
    print(C.LBLUE_B + "  Starting live BTC price polling. Ctrl+C to stop."
          + C.RESET + "\n")
    last_full = 0.0
    last_len = 0

    try:
        while True:
            results, agg = get_btc_price()

            if agg is not None:
                line = (f"  {C.LBLUE}BTC:{C.RESET} "
                        f"{C.WHITE}{fmt_price(agg['best_price'])}{C.RESET}"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"{agg['num_agreeing']}/{agg['num_sources']} agree"
                        f"   {C.GRAY}|{C.RESET}  "
                        f"{C.CYAN}{agg['freshest']['name']}{C.RESET}"
                        f" {C.GRAY}({agg['freshest']['age']*1000:.0f}ms){C.RESET}")
            else:
                line = f"  {C.RED}BTC: no data{C.RESET}"

            sys.stdout.write("\r" + line.ljust(last_len))
            sys.stdout.flush()
            last_len = len(line) + 20  # pad for ANSI codes

            now = time.time()
            if now - last_full >= show_table_every:
                sys.stdout.write("\n")
                print_report(results, agg)
                last_full = now
                last_len = 0

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n\n" + C.GRAY + "  Stopped." + C.RESET)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "once"

    if mode == "live":
        run_continuous(interval=1.0, show_table_every=30.0)
    else:
        results, agg = get_btc_price()
        print_report(results, agg)
