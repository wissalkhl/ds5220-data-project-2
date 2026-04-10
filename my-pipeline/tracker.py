import os
import boto3
import requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
from datetime import datetime, timezone

# Environment variables
BUCKET = os.environ.get("S3_BUCKET", "ds5220-project2")
REGION = os.environ.get("AWS_REGION", "us-east-1")
TABLE  = os.environ.get("DYNAMODB_TABLE", "crypto-tracking")

# Coins to track
COINS = ["bitcoin", "ethereum", "solana"]

# AWS clients
dynamodb = boto3.resource("dynamodb", region_name=REGION)
s3       = boto3.client("s3", region_name=REGION)
table    = dynamodb.Table(TABLE)

def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(COINS),
        "vs_currencies": "usd",
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_24hr_change": "true"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def save_to_dynamodb(data):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for coin in COINS:
        if coin not in data:
            continue
        d = data[coin]
        item = {
            "coin_id":    coin,
            "timestamp":  ts,
            "price_usd":  str(d.get("usd", 0)),
            "market_cap": str(d.get("usd_market_cap", 0)),
            "volume_24h": str(d.get("usd_24h_vol", 0)),
            "change_24h": str(d.get("usd_24h_change", 0))
        }
        table.put_item(Item=item)
        print(f"{coin} | price=${d.get('usd')} | change={d.get('usd_24h_change'):.2f}% | ts={ts}")

def fetch_history():
    records = []
    for coin in COINS:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("coin_id").eq(coin)
        )
        records.extend(response["Items"])
    return records

def generate_plot(records):
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["price_usd"] = df["price_usd"].astype(float)
    df = df.sort_values("timestamp")

    sns.set_theme(style="darkgrid")
    fig, ax = plt.subplots(figsize=(12, 6))

    for coin in COINS:
        subset = df[df["coin_id"] == coin]
        ax.plot(subset["timestamp"], subset["price_usd"], marker="o", markersize=3, label=coin.capitalize())

    ax.set_title("Crypto Prices Over Time (USD)", fontsize=16)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Price (USD)")
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d %H:%M"))
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("/tmp/plot.png", dpi=150)
    plt.close()
    print("Plot saved to /tmp/plot.png")

def generate_csv(records):
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values(["coin_id", "timestamp"])
    df.to_csv("/tmp/data.csv", index=False)
    print("CSV saved to /tmp/data.csv")

def upload_to_s3():
    s3.upload_file("/tmp/plot.png", BUCKET, "plot.png", ExtraArgs={"ContentType": "image/png"})
    s3.upload_file("/tmp/data.csv", BUCKET, "data.csv", ExtraArgs={"ContentType": "text/csv"})
    print(f"Uploaded plot.png and data.csv to s3://{BUCKET}/")

if __name__ == "__main__":
    print("Fetching crypto prices from CoinGecko...")
    data = fetch_prices()
    save_to_dynamodb(data)
    records = fetch_history()
    generate_plot(records)
    generate_csv(records)
    upload_to_s3()
    print("Done!")
