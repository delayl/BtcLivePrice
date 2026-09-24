# BTC Live Price 

Pulls BTC/USD price from 17 free public APIs in parallel, filters out delayed data, and shows the consensus price in your terminal.


## Using it as a library

You can import this into your own code to get live BTC price data without running the full CLI.

python
from btcmain import get_btc_price

results, agg = get_btc_price()

if agg:
    print(agg["best_price"])       # float, e.g. 84139.01
    print(agg["num_agreeing"])     # int, how many sources agreed
    print(agg["num_sources"])      # int, how many responded
    print(agg["spread"])           # float, hi - lo across exchanges
    print(agg["freshest"]["name"]) # str, e.g. "Binance"
    print(agg["freshest"]["age"])  # float, seconds old

## Features

- 17 sources, no API keys needed
- Rejects stale data (older than 30s)
- Flags outliers, uses median of agreeing sources
- Rate-limit safe (per-source throttling + 429 backoff)
- Rolling volatility (σ, annualized, range %, up/down ticks)
- Cross-exchange spread tracking
- Optional live chart window
- Optional Discord alerts

<img width="1125" height="790" alt="image" src="https://github.com/user-attachments/assets/b9f9e89e-cc9e-4ee8-aeb8-c790126e7db9" />

<img width="1483" height="765" alt="image" src="https://github.com/user-attachments/assets/719ac588-58a0-4b26-a549-40fe6091eb33" />



## Install

Python 3.8+ required.

```bash
pip install matplotlib   # only needed for the chart window

python btcmain.py live
