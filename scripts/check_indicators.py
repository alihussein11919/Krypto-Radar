import json
with open("/opt/airflow/data/analysis/indicators_2026-07-17.json") as f:
    data = json.load(f)
print(f"Indicators: {len(data)} coins")
if data:
    print(f"Keys: {list(data[0].keys())[:10]}")
    coin = data[0]
    print(f"Sample: coin_id={coin.get('coin_id')}, signal={coin.get('signal')}, RSI={coin.get('rsi_14')}")
