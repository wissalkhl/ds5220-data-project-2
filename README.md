# DS5220 Data Project 2 — Crypto Price Tracker

## Data Source
This pipeline tracks live cryptocurrency prices using the CoinGecko API (https://api.coingecko.com). CoinGecko provides free, no-key-required endpoints returning current prices, market cap, and 24-hour volume for any cryptocurrency. This pipeline tracks Bitcoin, Ethereum, and Solana prices in USD.

## Scheduled Process
A Kubernetes CronJob runs the containerized Python application every hour. On each run the application:
1. Fetches current price, market cap, 24-hour volume, and 24-hour percent change for Bitcoin, Ethereum, and Solana from the CoinGecko API
2. Writes each record to a DynamoDB table (crypto-tracking) with coin_id as the partition key and timestamp as the sort key
3. Queries the full history from DynamoDB
4. Generates a time-series plot of prices over time using matplotlib and seaborn
5. Uploads plot.png and data.csv to a public S3 static website bucket

## Output Data and Plot
- **plot.png** — a time-series line chart showing USD prices for Bitcoin, Ethereum, and Solana over the entire collection window (72+ hours)
- **data.csv** — a CSV file containing all collected records with columns: coin_id, timestamp, price_usd, market_cap, volume_24h, change_24h
