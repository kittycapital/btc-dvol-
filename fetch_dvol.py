"""
Bitcoin Volatility Index (DVOL) Fetcher
Fetches DVOL data from Deribit API and BTC price for 1 year.
"""

import json
import requests
from datetime import datetime, timedelta

DATA_FILE = 'data.json'


def fetch_dvol_data():
    print("Fetching DVOL data from Deribit...")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=365)
    
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    
    url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"
    params = {
        'currency': 'BTC',
        'resolution': '1D',
        'start_timestamp': start_timestamp,
        'end_timestamp': end_timestamp
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        if 'result' in data and 'data' in data['result']:
            raw_data = data['result']['data']
            print(f"Got {len(raw_data)} DVOL data points")
            return raw_data
        else:
            print("No data in response")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None


def fetch_btc_price_data():
    print("Fetching BTC price data from blockchain.com...")
    
    url = "https://api.blockchain.info/charts/market-price"
    params = {
        'timespan': '1year',
        'format': 'json',
        'sampled': 'true'
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        
        print(f"Got {len(data['values'])} price data points")
        return data['values']
    
    except Exception as e:
        print(f"Error: {e}")
        return None


def process_data(dvol_data, price_data):
    print("Processing data...")
    
    dvol_by_date = {}
    for item in dvol_data:
        timestamp = item[0]
        close = item[4]
        date = datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d')
        dvol_by_date[date] = close
    
    price_by_date = {}
    for item in price_data:
        date = datetime.utcfromtimestamp(item['x']).strftime('%Y-%m-%d')
        price_by_date[date] = item['y']
    
    common_dates = sorted(set(dvol_by_date.keys()) & set(price_by_date.keys()))
    
    print(f"Found {len(common_dates)} common dates")
    
    dates = []
    btc_prices = []
    dvol_values = []
    
    for date in common_dates:
        dates.append(date)
        btc_prices.append(round(price_by_date[date], 2))
        dvol_values.append(round(dvol_by_date[date], 2))
    
    return dates, btc_prices, dvol_values


def main():
    print("Starting Bitcoin DVOL fetch...\n")
    
    dvol_data = fetch_dvol_data()
    price_data = fetch_btc_price_data()
    
    if not dvol_data or not price_data:
        print("Failed to fetch data")
        return
    
    dates, btc_prices, dvol_values = process_data(dvol_data, price_data)
    
    if not dates:
        print("No aligned data")
        return
    
    current_price = btc_prices[-1]
    current_dvol = dvol_values[-1]
    expected_daily_move = current_dvol / 19.1
    
    print(f"\nCurrent Stats:")
    print(f"BTC Price: ${current_price:,.0f}")
    print(f"DVOL: {current_dvol:.1f}%")
    print(f"Expected Daily Move: {expected_daily_move:.2f}%")
    
    if current_dvol >= 60:
        status = "과변동성"
        status_en = "High Volatility"
    elif current_dvol <= 40:
        status = "저변동성"
        status_en = "Low Volatility"
    else:
        status = "보통"
        status_en = "Normal"
    
    print(f"Status: {status} ({status_en})")
    
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
        json.dump(output, f, indent=2)
    
    print(f"\nSaved to {DATA_FILE}")


if __name__ == '__main__':
    main()
