"""
Bitcoin Volatility Index (DVOL) Fetcher
Fetches DVOL data from Deribit API and BTC price for up to 3 years.
Supports 6M / 1Y / 3Y period tabs in the dashboard.
"""

import json
import requests
from datetime import datetime, timedelta

DATA_FILE = 'data.json'
FETCH_DAYS = 1095  # 3 years


def fetch_dvol_data():
    """Fetch DVOL from Deribit in yearly chunks (API limit ~1 year per call)."""
    print("Fetching DVOL data from Deribit...")

    end_time = datetime.utcnow()
    all_data = []

    # Fetch in 365-day chunks to cover 3 years
    for i in range(3):
        chunk_end = end_time - timedelta(days=365 * i)
        chunk_start = chunk_end - timedelta(days=365)

        url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
        params = {
            'currency': 'BTC',
            'resolution': '1D',
            'start_timestamp': int(chunk_start.timestamp() * 1000),
            'end_timestamp': int(chunk_end.timestamp() * 1000)
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            if 'result' in data and 'data' in data['result']:
                chunk = data['result']['data']
                all_data.extend(chunk)
                print(f"  Chunk {i+1}: {len(chunk)} points "
                      f"({chunk_start.strftime('%Y-%m-%d')} ~ {chunk_end.strftime('%Y-%m-%d')})")
            else:
                print(f"  Chunk {i+1}: no data")

        except Exception as e:
            print(f"  Chunk {i+1} error: {e}")

    # Deduplicate by timestamp
    seen = set()
    unique = []
    for item in all_data:
        ts = item[0]
        if ts not in seen:
            seen.add(ts)
            unique.append(item)

    unique.sort(key=lambda x: x[0])
    print(f"Total DVOL: {len(unique)} unique data points")
    return unique if unique else None


def fetch_btc_price_data():
    """Fetch BTC price from blockchain.com for 3 years."""
    print("Fetching BTC price data from blockchain.com...")

    url = "https://api.blockchain.info/charts/market-price"
    params = {
        'timespan': '3years',
        'format': 'json',
        'sampled': 'true'
    }

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        print(f"Got {len(data['values'])} price data points")
        return data['values']

    except Exception as e:
        print(f"Error: {e}")
        return None


def process_data(dvol_data, price_data):
    """Align DVOL and price data by date."""
    print("Processing data...")

    dvol_by_date = {}
    for item in dvol_data:
        timestamp = item[0]
        close = item[4]  # [timestamp, open, high, low, close]
        date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
        dvol_by_date[date] = close

    price_by_date = {}
    for item in price_data:
        date = datetime.utcfromtimestamp(item['x']).strftime('%Y-%m-%d')
        price_by_date[date] = item['y']

    common_dates = sorted(set(dvol_by_date.keys()) & set(price_by_date.keys()))

    print(f"DVOL dates: {len(dvol_by_date)}, Price dates: {len(price_by_date)}")
    print(f"Common dates: {len(common_dates)}")

    if not common_dates:
        return [], [], []

    dates = []
    btc_prices = []
    dvol_values = []

    for date in common_dates:
        dates.append(date)
        btc_prices.append(round(price_by_date[date], 2))
        dvol_values.append(round(dvol_by_date[date], 2))

    return dates, btc_prices, dvol_values


def main():
    print("=" * 50)
    print("Bitcoin DVOL Fetcher (3-Year)")
    print("=" * 50 + "\n")

    dvol_data = fetch_dvol_data()
    price_data = fetch_btc_price_data()

    if not dvol_data or not price_data:
        print("\nFailed to fetch data. Exiting.")
        return

    dates, btc_prices, dvol_values = process_data(dvol_data, price_data)

    if not dates:
        print("\nNo aligned data found. Exiting.")
        return

    current_price = btc_prices[-1]
    current_dvol = dvol_values[-1]
    expected_daily_move = current_dvol / 19.1  # sqrt(365) ≈ 19.1

    if current_dvol >= 60:
        status, status_en = "과변동성", "High Volatility"
    elif current_dvol <= 40:
        status, status_en = "저변동성", "Low Volatility"
    else:
        status, status_en = "보통", "Normal"

    print(f"\n{'─' * 40}")
    print(f"Date Range : {dates[0]} ~ {dates[-1]}")
    print(f"Data Points: {len(dates)}")
    print(f"BTC Price  : ${current_price:,.0f}")
    print(f"DVOL       : {current_dvol:.1f}%")
    print(f"Daily Move : ±{expected_daily_move:.2f}%")
    print(f"Status     : {status} ({status_en})")
    print(f"{'─' * 40}")

    output = {
        'dates': dates,
        'btc_prices': btc_prices,
        'dvol': dvol_values,
        'current_price': current_price,
        'current_dvol': current_dvol,
        'expected_daily_move': round(expected_daily_move, 2),
        'status': status,
        'status_en': status_en,
        'last_updated': datetime.utcnow().isoformat() + 'Z'
    }

    with open(DATA_FILE, 'w') as f:
        json.dump(output, f)

    import os
    size_kb = os.path.getsize(DATA_FILE) / 1024
    print(f"\nSaved to {DATA_FILE} ({size_kb:.0f} KB)")


if __name__ == '__main__':
    main()
