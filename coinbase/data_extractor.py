"""
Coinbase Data Extractor - Lambda Function Equivalent
Extracts data from Coinbase Advanced API and saves to S3 raw folder.
"""

import sys
import os
import json
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coinbase.rest import RESTClient
from aws.connect_to_s3 import S3Helper


def get_coinbase_client() -> RESTClient:
    """Initialize Coinbase Advanced API client."""
    from configuration.secrets import COINBASE_API_KEY, COINBASE_API_SECRET

    client = RESTClient(api_key=COINBASE_API_KEY, api_secret=COINBASE_API_SECRET)
    return client


def get_current_positions(client: RESTClient) -> Dict:
    """
    Extract current positions data.
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

        print(
            f"Getting breakdown for portfolio: {portfolio_name} (UUID: {portfolio_uuid})"
        )

        breakdown_response = client.get_portfolio_breakdown(
            portfolio_uuid=portfolio_uuid
        )
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


def get_price_history(
    client: RESTClient, assets: List[str] = None, days_back: int = None
) -> Dict:
    """
    Extract historical price data for portfolio assets.
    Returns: Historical price data for all assets going back to April 1st, 2021.
    """
    try:
        print("Fetching price history...")

        # Get portfolio products from current portfolio only
        portfolio_products = set()

        # Get current portfolio products
        print("Getting portfolio breakdown to identify products...")
        portfolios_response = client.get_portfolios()
        portfolios_dict = portfolios_response.to_dict()
        portfolios = portfolios_dict.get("portfolios", [])

        if portfolios:
            portfolio_uuid = portfolios[0]["uuid"]
            breakdown_response = client.get_portfolio_breakdown(
                portfolio_uuid=portfolio_uuid
            )
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

        print(
            f"Found {len(portfolio_products)} unique products: {sorted(portfolio_products)}"
        )

        # Filter out crypto-crypto pairs for price history (keep only fiat pairs)
        fiat_products = set()
        for product_id in portfolio_products:
            # Check if it's a fiat pair (ends with GBP, USD, EUR, etc.)
            if any(
                product_id.endswith(fiat)
                for fiat in ["-GBP", "-USD", "-EUR", "-USDC", "-USDT"]
            ):
                fiat_products.add(product_id)

        print(
            f"Filtered to {len(fiat_products)} fiat products for price history: {sorted(fiat_products)}"
        )

        # Set date range to go back to April 1st, 2021
        end_date = datetime.now()
        start_date = datetime(2021, 4, 1)  # April 1st, 2021

        all_prices = []

        for product_id in sorted(fiat_products):
            try:
                print(
                    f"Fetching price history for {product_id} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}..."
                )

                # Calculate total days and number of chunks needed
                total_days = (end_date - start_date).days
                chunk_size = 349  # Stay under 350 candle limit
                num_chunks = (
                    total_days + chunk_size - 1
                ) // chunk_size  # Ceiling division

                print(
                    f"Total days: {total_days}, will fetch in {num_chunks} chunks of {chunk_size} days each"
                )

                product_prices = []

                for chunk in range(num_chunks):
                    chunk_start = start_date + timedelta(days=chunk * chunk_size)
                    chunk_end = min(
                        start_date + timedelta(days=(chunk + 1) * chunk_size), end_date
                    )

                    # Convert to timestamps
                    start_timestamp = str(int(chunk_start.timestamp()))
                    end_timestamp = str(int(chunk_end.timestamp()))

                    print(
                        f"  Chunk {chunk + 1}/{num_chunks}: {chunk_start.strftime('%Y-%m-%d')} to {chunk_end.strftime('%Y-%m-%d')}"
                    )

                    # Get candles data
                    candles_response = client.get_candles(
                        product_id=product_id,
                        start=start_timestamp,
                        end=end_timestamp,
                        granularity="ONE_DAY",
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
                        }
                        product_prices.append(price_data)

                    print(f"    Retrieved {len(candles)} candles for this chunk")

                all_prices.extend(product_prices)
                print(f"Total candles for {product_id}: {len(product_prices)}")

            except Exception as e:
                print(f"Error fetching price history for {product_id}: {e}")
                continue

        return {
            "timestamp": datetime.now().isoformat(),
            "date_range": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "summary": {
                "total_assets": len(fiat_products),
                "total_price_points": len(all_prices),
                "assets_processed": list(fiat_products),
            },
            "prices": all_prices,
        }

    except Exception as e:
        print(f"Error fetching price history: {e}")
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
            breakdown_response = client.get_portfolio_breakdown(
                portfolio_uuid=portfolio_uuid
            )
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

        print(
            f"Found {len(portfolio_products)} unique products for market data: {sorted(portfolio_products)}"
        )

        # Filter out crypto-crypto pairs for market data (keep only fiat pairs)
        fiat_products = set()
        for product_id in portfolio_products:
            # Check if it's a fiat pair (ends with GBP, USD, EUR, etc.)
            if any(
                product_id.endswith(fiat)
                for fiat in ["-GBP", "-USD", "-EUR", "-USDC", "-USDT"]
            ):
                fiat_products.add(product_id)

        print(
            f"Filtered to {len(fiat_products)} fiat products for market data: {sorted(fiat_products)}"
        )

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
                price_change_24h = float(
                    product_dict.get("price_percentage_change_24h", 0)
                )

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
                    book_response = client.get_product_book(
                        product_id=product_id, level=1
                    )
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
                    print(
                        f"Order book not available for {product_id}, using current price: {book_error}"
                    )
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
                print(
                    f"Successfully fetched market data for {product_id} - Price: {current_price}, Change: {price_change_24h}%, Spread: {spread}"
                )

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


def save_raw_data_to_s3(
    positions_data: Dict, prices_data: Dict, market_data: Dict, environment: str
) -> str:
    """
    Save all raw data to S3 following the strategy's folder structure.
    """
    try:
        s3_helper = S3Helper()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Save current positions
        positions_key = (
            f"{environment}/coinbase/raw/positions/positions_{timestamp}.json"
        )
        local_positions_file = f"temp_positions_{timestamp}.json"
        with open(local_positions_file, "w") as f:
            json.dump(positions_data, f, indent=2, default=str)
        s3_helper.upload_file_to_s3(local_positions_file, positions_key)
        print(f"Current positions saved to S3: {positions_key}")

        # Save latest positions
        latest_positions_key = (
            f"{environment}/coinbase/raw/positions/latest/positions.json"
        )
        s3_helper.upload_file_to_s3(local_positions_file, latest_positions_key)
        os.remove(local_positions_file)
        print(f"Latest positions saved to S3: {latest_positions_key}")

        # Save price history
        prices_key = f"{environment}/coinbase/raw/prices/prices_{timestamp}.json"
        local_prices_file = f"temp_prices_{timestamp}.json"
        with open(local_prices_file, "w") as f:
            json.dump(prices_data, f, indent=2, default=str)
        s3_helper.upload_file_to_s3(local_prices_file, prices_key)
        print(f"Price history saved to S3: {prices_key}")

        # Save latest prices
        latest_prices_key = f"{environment}/coinbase/raw/prices/latest/prices.json"
        s3_helper.upload_file_to_s3(local_prices_file, latest_prices_key)
        os.remove(local_prices_file)
        print(f"Latest prices saved to S3: {latest_prices_key}")

        # Save market data
        market_key = f"{environment}/coinbase/raw/market/market_{timestamp}.json"
        local_market_file = f"temp_market_{timestamp}.json"
        with open(local_market_file, "w") as f:
            json.dump(market_data, f, indent=2, default=str)
        s3_helper.upload_file_to_s3(local_market_file, market_key)
        print(f"Market data saved to S3: {market_key}")

        # Save latest market data
        latest_market_key = f"{environment}/coinbase/raw/market/latest/market.json"
        s3_helper.upload_file_to_s3(local_market_file, latest_market_key)
        os.remove(local_market_file)
        print(f"Latest market data saved to S3: {latest_market_key}")

        return timestamp

    except Exception as e:
        print(f"Error saving raw data to S3: {e}")
        raise


def main():
    """
    Main data extraction function - equivalent to Coinbase Data Fetcher Lambda.
    """
    print("=== Starting Coinbase Data Extraction ===")
    print(f"Extraction started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        from configuration.secrets import ENVIRONMENT

        # Initialize client
        print("Initializing Coinbase Advanced API client...")
        client = get_coinbase_client()

        # Extract all data types
        print("\n1. Extracting current positions...")
        positions_data = get_current_positions(client)

        print("\n2. Extracting price history...")
        prices_data = get_price_history(client)

        print("\n3. Extracting market data...")
        market_data = get_market_data(client)

        # Save to S3
        print("\n4. Saving raw data to S3...")
        timestamp = save_raw_data_to_s3(
            positions_data, prices_data, market_data, ENVIRONMENT
        )

        print(f"\n=== Coinbase Data Extraction Completed Successfully ===")
        print(f"Extraction completed at: {timestamp}")
        print(f"Data saved to S3 with timestamp: {timestamp}")

        return {
            "status": "success",
            "timestamp": timestamp,
            "data_types": ["positions", "prices", "market"],
            "records_processed": {
                "positions": len(positions_data["positions"]),
                "prices": len(prices_data["prices"]),
                "market_assets": len(market_data["asset_market_data"]),
            },
        }

    except Exception as e:
        print(f"\n!!! Data extraction failed with error: {e} !!!")
        raise


if __name__ == "__main__":
    main()
