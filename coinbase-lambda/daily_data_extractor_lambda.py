"""
Coinbase Daily Data Extractor - Lambda Function
Extracts daily portfolio summary, market data, and hourly price data for the last 24 hours.
Runs daily at 2am UTC.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import boto3

from coinbase.rest import RESTClient


def get_secret(secret_name: str) -> Dict:
    """
    Retrieve a secret from AWS Secrets Manager.
    """
    try:
        secrets_client = boto3.client('secretsmanager')
        response = secrets_client.get_secret_value(SecretId=secret_name)
        
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
            return secret
        else:
            raise ValueError(f"Secret {secret_name} not found or not a string")
    except Exception as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise


def get_coinbase_client() -> RESTClient:
    """Initialize Coinbase Advanced API client using AWS Secrets Manager."""
    try:
        # Use the existing secret name
        secret_name = "coinbase/api-credentials"
        
        print(f"Retrieving Coinbase credentials from secret: {secret_name}")
        
        # Get credentials from Secrets Manager
        credentials = get_secret(secret_name)
        
        api_key = credentials.get('COINBASE_API_KEY')
        api_secret = credentials.get('COINBASE_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError(f"COINBASE_API_KEY and COINBASE_API_SECRET not found in secret {secret_name}")
        
        print("Successfully retrieved Coinbase credentials from Secrets Manager")
        
        client = RESTClient(api_key=api_key, api_secret=api_secret)
        return client
        
    except Exception as e:
        print(f"Error initializing Coinbase client: {e}")
        raise


def get_s3_client():
    """Initialize S3 client using native AWS SDK."""
    return boto3.client('s3')


def upload_json_to_s3(s3_client, data: Dict, bucket: str, key: str):
    """Upload JSON data to S3 using native AWS SDK."""
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(data, indent=2, default=str),
            ContentType='application/json'
        )
        print(f"Successfully uploaded to S3: s3://{bucket}/{key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        raise


def get_current_positions(client: RESTClient) -> Dict:
    """
    Extract current positions data for daily portfolio summary.
    Returns: Current portfolio positions with balances and market values.
    """
    try:
        print("Fetching current portfolio positions...")

        # Get portfolio summary
        portfolios_response = client.get_portfolios()
        portfolios_dict = portfolios_response.to_dict()
        portfolios = portfolios_dict.get("portfolios", [])

        if not portfolios:
            print("No portfolios found")
            return {"timestamp": datetime.now().isoformat(), "positions": []}

        portfolio_uuid = portfolios[0]["uuid"]
        portfolio_name = portfolios[0]["name"]

        print(f"Getting breakdown for portfolio: {portfolio_name} (UUID: {portfolio_uuid})")

        breakdown_response = client.get_portfolio_breakdown(portfolio_uuid=portfolio_uuid)
        breakdown_dict = breakdown_response.to_dict()

        # Extract current positions from spot_positions
        positions = []
        spot_positions = breakdown_dict["breakdown"]["spot_positions"]

        for position in spot_positions:
            # Use total_balance_crypto for quantity (includes staked assets) and total_balance_fiat for value
            quantity = float(position.get("total_balance_crypto", 0))
            current_value_gbp = float(position.get("total_balance_fiat", 0))

            # Include positions with any balance (even small amounts)
            if quantity > 0 or current_value_gbp > 0:
                position_data = {
                    "asset_symbol": position["asset"],
                    "asset_name": position.get("asset_name", position["asset"]),
                    "quantity": quantity,
                    "current_value_gbp": current_value_gbp,
                    "average_purchase_price": float(
                        position.get("average_entry_price", {}).get("value", 0)
                    ),
                    "unrealized_pnl": float(position.get("unrealized_pnl", 0)),
                    "percentage_allocation": float(position.get("allocation", 0)),
                    "last_updated": datetime.now().isoformat(),
                }
                positions.append(position_data)

        return {
            "timestamp": datetime.now().isoformat(),
            "portfolio_info": {
                "name": portfolio_name,
                "uuid": portfolio_uuid,
                "type": breakdown_dict["breakdown"]["portfolio"]["type"],
            },
            "positions": positions,
            "raw_breakdown": breakdown_dict,
        }

    except Exception as e:
        print(f"Error fetching current positions: {e}")
        raise


def get_hourly_price_data(client: RESTClient) -> Dict:
    """
    Extract hourly price data for the last 24 hours for all portfolio assets.
    Returns: Hourly price data for portfolio assets.
    """
    try:
        print("Fetching hourly price data for the last 24 hours...")

        # Get portfolio products from current portfolio
        portfolio_products = set()

        # Get current portfolio products
        print("Getting portfolio breakdown to identify products...")
        portfolios_response = client.get_portfolios()
        portfolios_dict = portfolios_response.to_dict()
        portfolios = portfolios_dict.get("portfolios", [])

        if portfolios:
            portfolio_uuid = portfolios[0]["uuid"]
            breakdown_response = client.get_portfolio_breakdown(portfolio_uuid=portfolio_uuid)
            breakdown_dict = breakdown_response.to_dict()
            spot_positions = breakdown_dict["breakdown"]["spot_positions"]
            balances = breakdown_dict["breakdown"]["portfolio_balances"]

            for position in spot_positions:
                if position["asset"] not in ["GBP", "EUR", "USD"]:
                    base_currency = position["asset"]
                    quote_currency = balances["total_balance"]["currency"]
                    product_id = f"{base_currency}-{quote_currency}"
                    portfolio_products.add(product_id)
                    print(f"Added product from portfolio: {product_id}")

        print(f"Found {len(portfolio_products)} unique products: {sorted(portfolio_products)}")

        # Filter out crypto-crypto pairs for price data (keep only fiat pairs)
        fiat_products = set()
        for product_id in portfolio_products:
            # Check if it's a fiat pair (ends with GBP, USD, EUR, etc.)
            if any(
                product_id.endswith(fiat)
                for fiat in ["-GBP", "-USD", "-EUR", "-USDC", "-USDT"]
            ):
                fiat_products.add(product_id)

        print(f"Filtered to {len(fiat_products)} fiat products for price data: {sorted(fiat_products)}")

        # Set date range to last 24 hours
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=24)

        all_hourly_prices = []

        for product_id in sorted(fiat_products):
            try:
                print(f"Fetching hourly price data for {product_id} from {start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}...")

                # Convert to timestamps
                start_timestamp = str(int(start_time.timestamp()))
                end_timestamp = str(int(end_time.timestamp()))

                # Get hourly candles data
                candles_response = client.get_candles(
                    product_id=product_id,
                    start=start_timestamp,
                    end=end_timestamp,
                    granularity="ONE_HOUR",
                )

                candles_dict = candles_response.to_dict()
                candles = candles_dict.get("candles", [])

                # Extract base asset from product_id (e.g., "BTC-GBP" -> "BTC")
                base_asset = product_id.split("-")[0]

                for candle in candles:
                    price_data = {
                        "asset_symbol": base_asset,
                        "product_id": product_id,
                        "date": candle["start"],
                        "open_price": float(candle["open"]),
                        "high_price": float(candle["high"]),
                        "low_price": float(candle["low"]),
                        "close_price": float(candle["close"]),
                        "volume": float(candle["volume"]),
                        "timestamp": candle["start"],
                        "hour": datetime.fromtimestamp(int(candle["start"])).strftime("%Y-%m-%d %H:00:00"),
                    }
                    all_hourly_prices.append(price_data)

                print(f"Retrieved {len(candles)} hourly candles for {product_id}")

            except Exception as e:
                print(f"Error fetching hourly price data for {product_id}: {e}")
                continue

        return {
            "timestamp": datetime.now().isoformat(),
            "date_range": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            },
            "summary": {
                "total_assets": len(fiat_products),
                "total_price_points": len(all_hourly_prices),
                "assets_processed": list(fiat_products),
            },
            "hourly_prices": all_hourly_prices,
        }

    except Exception as e:
        print(f"Error fetching hourly price data: {e}")
        raise


def get_market_data(client: RESTClient) -> Dict:
    """
    Extract market data for portfolio assets.
    Returns: Current market information and trends.
    """
    try:
        print("Fetching market data...")

        # Get portfolio products from current portfolio only
        portfolio_products = set()

        # Get current portfolio products
        print("Getting portfolio breakdown to identify products...")
        portfolios_response = client.get_portfolios()
        portfolios_dict = portfolios_response.to_dict()
        portfolios = portfolios_dict.get("portfolios", [])

        if portfolios:
            portfolio_uuid = portfolios[0]["uuid"]
            breakdown_response = client.get_portfolio_breakdown(portfolio_uuid=portfolio_uuid)
            breakdown_dict = breakdown_response.to_dict()
            spot_positions = breakdown_dict["breakdown"]["spot_positions"]
            balances = breakdown_dict["breakdown"]["portfolio_balances"]

            for position in spot_positions:
                if position["asset"] not in ["GBP", "EUR", "USD"]:
                    base_currency = position["asset"]
                    quote_currency = balances["total_balance"]["currency"]
                    product_id = f"{base_currency}-{quote_currency}"
                    portfolio_products.add(product_id)
                    print(f"Added product from portfolio: {product_id}")

        print(f"Found {len(portfolio_products)} unique products for market data: {sorted(portfolio_products)}")

        # Filter out crypto-crypto pairs for market data (keep only fiat pairs)
        fiat_products = set()
        for product_id in portfolio_products:
            # Check if it's a fiat pair (ends with GBP, USD, EUR, etc.)
            if any(
                product_id.endswith(fiat)
                for fiat in ["-GBP", "-USD", "-EUR", "-USDC", "-USDT"]
            ):
                fiat_products.add(product_id)

        print(f"Filtered to {len(fiat_products)} fiat products for market data: {sorted(fiat_products)}")

        asset_market_data = []

        for product_id in sorted(fiat_products):
            try:
                print(f"Fetching market data for {product_id}...")

                # Get product information (includes current price and price change)
                product_response = client.get_product(product_id=product_id)
                product_dict = product_response.to_dict()

                # Extract base asset from product_id
                base_asset = product_id.split("-")[0]

                # Get current price and price change from product data
                current_price = float(product_dict.get("price", 0))
                price_change_24h = float(product_dict.get("price_percentage_change_24h", 0))

                # Calculate absolute price change from percentage
                absolute_price_change = (
                    (current_price * price_change_24h / 100)
                    if current_price > 0 and price_change_24h != 0
                    else 0
                )

                # Get current price from most recent candle as backup
                end_time = datetime.now()
                start_time = end_time - timedelta(days=1)  # Get last 24 hours

                candles_response = client.get_candles(
                    product_id=product_id,
                    start=str(int(start_time.timestamp())),
                    end=str(int(end_time.timestamp())),
                    granularity="ONE_HOUR",
                )

                candles_dict = candles_response.to_dict()
                candles = candles_dict.get("candles", [])

                # Use candle price if product price is not available
                if current_price == 0 and candles:
                    latest_candle = candles[-1]
                    current_price = float(latest_candle["close"])

                # Try to get bid/ask from order book
                best_bid = 0
                best_ask = 0
                spread = 0
                spread_percentage = 0

                try:
                    book_response = client.get_product_book(product_id=product_id, level=1)
                    book_dict = book_response.to_dict()

                    # Extract bid/ask from order book
                    pricebook = book_dict.get("pricebook", {})
                    bids = pricebook.get("bids", [])
                    asks = pricebook.get("asks", [])

                    if bids:
                        best_bid = float(bids[0]["price"])
                    if asks:
                        best_ask = float(asks[0]["price"])

                    # Calculate spread
                    if best_bid > 0 and best_ask > 0:
                        spread = best_ask - best_bid
                        spread_percentage = spread / best_bid * 100

                    # Also try to get spread from the API response
                    spread_bps = book_dict.get("spread_bps", 0)
                    spread_absolute = book_dict.get("spread_absolute", 0)

                    # Use API spread if available and order book spread is 0
                    if spread == 0 and spread_absolute > 0:
                        spread = spread_absolute
                        if best_bid > 0:
                            spread_percentage = spread / best_bid * 100

                except Exception as book_error:
                    print(f"Order book not available for {product_id}, using current price: {book_error}")
                    best_bid = current_price
                    best_ask = current_price

                market_data = {
                    "asset_symbol": base_asset,
                    "product_id": product_id,
                    "display_name": product_dict.get("display_name", product_id),
                    "status": product_dict.get("status", "unknown"),
                    "quote_currency": product_dict.get("quote_currency_id", ""),
                    "base_currency": product_dict.get("base_currency_id", ""),
                    "current_price": current_price,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "bid_ask_spread": spread,
                    "spread_percentage": spread_percentage,
                    "volume_24h": float(product_dict.get("volume_24h", 0)),
                    "price_change_24h": absolute_price_change,
                    "price_change_percentage_24h": price_change_24h,
                    "last_updated": datetime.now().isoformat(),
                }

                asset_market_data.append(market_data)
                print(f"Successfully fetched market data for {product_id} - Price: {current_price}, Change: {price_change_24h}%, Spread: {spread}")

            except Exception as e:
                print(f"Error fetching market data for {product_id}: {e}")
                continue

        return {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_assets": len(asset_market_data),
                "assets_processed": [d["product_id"] for d in asset_market_data],
            },
            "asset_market_data": asset_market_data,
        }

    except Exception as e:
        print(f"Error fetching market data: {e}")
        raise


def save_daily_data_to_s3(
    positions_data: Dict, hourly_prices_data: Dict, market_data: Dict, environment: str
) -> str:
    """
    Save daily data to S3 using native AWS SDK.
    """
    try:
        s3_client = get_s3_client()
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'hashadar-personalfinance')
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_str = datetime.now().strftime("%Y%m%d")

        # Save daily portfolio summary
        daily_positions_key = f"{environment}/coinbase/raw/positions/daily/positions_{date_str}.json"
        upload_json_to_s3(s3_client, positions_data, bucket_name, daily_positions_key)

        # Save latest positions
        latest_positions_key = f"{environment}/coinbase/raw/positions/latest/positions.json"
        upload_json_to_s3(s3_client, positions_data, bucket_name, latest_positions_key)

        # Save hourly price data
        hourly_prices_key = f"{environment}/coinbase/raw/prices/hourly/hourly_prices_{date_str}.json"
        upload_json_to_s3(s3_client, hourly_prices_data, bucket_name, hourly_prices_key)

        # Save latest hourly prices
        latest_hourly_prices_key = f"{environment}/coinbase/raw/prices/hourly/latest/hourly_prices.json"
        upload_json_to_s3(s3_client, hourly_prices_data, bucket_name, latest_hourly_prices_key)

        # Save market data
        market_key = f"{environment}/coinbase/raw/market/daily/market_{date_str}.json"
        upload_json_to_s3(s3_client, market_data, bucket_name, market_key)

        # Save latest market data
        latest_market_key = f"{environment}/coinbase/raw/market/latest/market.json"
        upload_json_to_s3(s3_client, market_data, bucket_name, latest_market_key)

        return timestamp

    except Exception as e:
        print(f"Error saving daily data to S3: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for daily Coinbase data extraction.
    Extracts portfolio summary, market data, and hourly price data for the last 24 hours.
    """
    try:
        print("=== Starting Daily Coinbase Data Extraction ===")
        print(f"Extraction started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Lambda Request ID: {context.aws_request_id}")

        # Get environment from environment variables
        environment = os.environ.get('ENVIRONMENT', 'develop')

        # Initialize client
        print("Initializing Coinbase Advanced API client...")
        client = get_coinbase_client()

        # Extract all data types
        print("\n1. Extracting current positions...")
        positions_data = get_current_positions(client)

        print("\n2. Extracting hourly price data for last 24 hours...")
        hourly_prices_data = get_hourly_price_data(client)

        print("\n3. Extracting market data...")
        market_data = get_market_data(client)

        # Save to S3
        print("\n4. Saving daily data to S3...")
        timestamp = save_daily_data_to_s3(positions_data, hourly_prices_data, market_data, environment)

        print(f"\n=== Daily Coinbase Data Extraction Completed Successfully ===")
        print(f"Extraction completed at: {timestamp}")
        print(f"Data saved to S3 with timestamp: {timestamp}")

        result = {
            "status": "success",
            "timestamp": timestamp,
            "lambda_request_id": context.aws_request_id,
            "data_types": ["positions", "hourly_prices", "market"],
            "records_processed": {
                "positions": len(positions_data["positions"]),
                "hourly_prices": len(hourly_prices_data["hourly_prices"]),
                "market_assets": len(market_data["asset_market_data"]),
            },
        }

        return {
            'statusCode': 200,
            'body': json.dumps(result, indent=2),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }

    except Exception as e:
        print(f"\n!!! Daily data extraction failed with error: {e} !!!")
        error_result = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Daily data extraction failed',
                'timestamp': datetime.now().isoformat(),
                'lambda_request_id': context.aws_request_id
            }, indent=2),
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            }
        }
        return error_result 