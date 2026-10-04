# cat > ~/airflow-native/dags/lib/eod_data_downloader.py << 'EOF'
import csv
import requests
from airflow.sdk.exceptions import AirflowFailException

import logging
import pendulum

# Initialize logger for logging events
log = logging.getLogger(__name__)


def download_massive_eod_data_to_csv(MASSIVE_API_KEY, LOOKBACK_DAYS):
    """
    Downloads the Massive grouped daily (EOD) data and stores it as a CSV file.

    Arguments:
    - MASSIVE_API_KEY: API key for authentication with the Massive API.
    - LOOKBACK_DAYS: Number of days to look back to find the latest trading day with data.
    """

    # Ensure that the Massive API Key is provided
    if not MASSIVE_API_KEY:
        raise AirflowFailException("Missing Massive API Key in Airflow Variables.")

    MASSIVE_BASE_URL = 'https://api.massive.com'  # Base URL for Massive API
    EXCHANGE_TZ = 'America/New_York'  # Timezone for trading day resolution
    today = pendulum.now(EXCHANGE_TZ).date()  # Get today's date in the specified exchange timezone

    # Iterate through the past 'POLYGON_MAX_LOOKBACK_DAYS' days to find a valid trading day
    for i in range(LOOKBACK_DAYS):
        # Calculate the target date by subtracting i days from today
        trading_date = today - pendulum.duration(days=i)
        trading_date = trading_date.strftime("%Y-%m-%d")  # Format the date as "YYYY-MM-DD"
        url = f'{MASSIVE_BASE_URL}/v2/aggs/grouped/locale/us/market/stocks/{trading_date}'  # URL for the API request
        params = {"adjusted": "true", "include_otc": "false", "apiKey": MASSIVE_API_KEY}

        try:
            # Make the API request to fetch the grouped daily data
            r = requests.get(url, params=params, timeout=60)
            log.info("[polygon] %s -> %s", r.url, r.status_code)  # Log the request URL and response status
        except Exception as e:
            log.warning("[massive] request failed for %s: %s", trading_date, e)
            continue

        # If the response is successful and contains results, write to CSV
        if r.status_code == 200 and r.json().get("resultsCount", 0) > 0:
            log.info("Found valid trading data for date: %s", trading_date)

            data = r.json()
            results = data.get("results", [])

            # Define the CSV structure
            fields = ["T", "o", "h", "l", "c", "v"]
            header = ["symbol", "open", "high", "low", "close", "volume"]

            # Write the data to the CSV file
            out_path = f"/tmp/eod_{trading_date}.csv"  # Define a fixed path for storing the CSV file locally
            with open(out_path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["trade_date"] + header)  # Write header row
                for row in results:
                    w.writerow([trading_date] + [row.get(k, "") for k in fields])  # Write the trading day data

         # --- Inject dummy stocks with negative volumes (for simulation) ---
                dummy_rows = [
                    [trading_date, "AAPL_X", 192.3, 195.6, 191.8, 194.1, -1500000],
                    [trading_date, "GOOGL_X", 138.2, 140.5, 137.6, 139.8, -980000],
                    [trading_date, "MSFT_X", 410.5, 415.2, 409.1, 412.4, -760000],
                    [trading_date, "AMZN_X", 171.8, 175.0, 170.4, 174.2, -620000],
                    [trading_date, "TSLA_X", 252.9, 258.3, 251.7, 257.5, -840000],
                    [trading_date, "META_X", 465.7, 472.2, 463.8, 471.0, -540000],
                    [trading_date, "NFLX_X", 600.1, 610.8, 598.5, 609.2, -430000],
                    [trading_date, "NVDA_X", 1135.6, 1150.3, 1130.1, 1147.9, -890000],
                    [trading_date, "INTC_X", 43.2, 44.0, 42.9, 43.8, -350000],
                    [trading_date, "IBM_TEST", 185.7, 188.9, 184.8, 187.3, -270000],
                ]

                w.writerows(dummy_rows)
                log.warning(f"[Injected] Added {len(dummy_rows)} dummy stocks with negative volumes for testing.")



            # Return the date for further use (if needed)
            return trading_date
        else:
            log.info("No data for date: %s, trying previous day.", trading_date)

    # If no valid trading day is found within the lookback window, raise an exception
    raise AirflowFailException("No grouped-daily data found within lookback window")
