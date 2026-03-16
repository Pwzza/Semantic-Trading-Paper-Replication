
import json
import sys
import os
from queue import Queue
from threading import Thread

sys.path.append(os.getcwd())

from fetcher.coordination import FetcherCoordinator
from fetcher.config import get_config

def main():
    print(">>> Starting Surgical Data Acquisition (Fixed Initialization)...")

    with open("vip_targets.json", "r") as f:
        targets = json.load(f)

    config = get_config()
    config.output_dirs.trade = "/content/drive/My Drive/Polymarket_Project/data/trades"
    config.output_dirs.price = "/content/drive/My Drive/Polymarket_Project/data/prices"
    config.workers.trade = 2
    config.workers.price = 2

    coord = FetcherCoordinator(config=config)

    # FIX: Initialize both Queues AND Fetchers
    coord._create_queues(use_swappable=True)
    coord._create_fetchers()  # <--- This was missing!

    print(f"Injecting {len(targets['trades'])} markets into Trade Queue...")
    for market_id in targets['trades']:
        coord._trade_market_queue.put(market_id)

    print(f"Injecting {len(targets['prices'])} tokens into Price Queue...")
    for token_data in targets['prices']:
        coord._price_token_queue.put(tuple(token_data))

    for _ in range(config.workers.trade):
        coord._trade_market_queue.put(None)
    for _ in range(config.workers.price):
        coord._price_token_queue.put(None)

    print(">>> Launching Workers...")

    trade_threads = []
    for i in range(config.workers.trade):
        t = Thread(target=coord._trade_fetcher._worker, args=(i, coord._trade_output_queue))
        t.start()
        trade_threads.append(t)

    price_threads = []
    for i in range(config.workers.price):
        t = Thread(target=coord._price_fetcher._worker, args=(i, coord._price_output_queue, None, None))
        t.start()
        price_threads.append(t)

    print(">>> Workers Running...")

    for t in trade_threads: t.join()
    for t in price_threads: t.join()

    coord._stop_persisters()
    print(">>> Acquisition Complete.")

if __name__ == "__main__":
    main()
