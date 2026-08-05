from pathlib import Path
import json
import time

import pandas as pd
import requests

API_URL = "https://open.er-api.com/v6/latest/USD"
STAGING_PATH = Path("staging") / "currency"

OUTPUT_FILE = STAGING_PATH / "fx_rates.csv"
LATEST_FILE = STAGING_PATH / "latest_rates.json"
STATUS_FILE = STAGING_PATH / "fx_status.json"

MAX_RETRIES = 3
RETRY_DELAY = 5


def get_exchange_rates():

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"\nAttempt {attempt}/{MAX_RETRIES}")
            response = requests.get(API_URL, timeout=10)
            response.raise_for_status()
            data = response.json()
            save_latest_rates(data)
            save_status(
                status="SUCCESS",
                reason="Live API data used."
            )
            print("Exchange rates fetched successfully.")
            return data
        except requests.exceptions.RequestException as e:
            print(f"\nAttempt {attempt} failed")
            print(e)
            if attempt < MAX_RETRIES:
                print(f"Retrying in {RETRY_DELAY} seconds...\n")
                time.sleep(RETRY_DELAY)
    print("\nAPI unavailable after all retries.")
    print("Trying to use previous successful exchange rates...")
    try:
        data = load_latest_rates()
    except FileNotFoundError:
        raise Exception(
            "Currency API failed and no cached exchange-rate file exists."
        )

    save_status(
        status="DEGRADED",
        reason="API unavailable. Previous successful exchange rates used."
    )
    return data



def save_latest_rates(data):
    STAGING_PATH.mkdir(parents=True, exist_ok=True)
    with open(LATEST_FILE, "w") as file:
        json.dump(data, file, indent=4)


def load_latest_rates():
    if not LATEST_FILE.exists():
        raise FileNotFoundError
    with open(LATEST_FILE, "r") as file:
        return json.load(file)


def save_status(status, reason):

    STAGING_PATH.mkdir(parents=True, exist_ok=True)
    payload = {
        "status": status,
        "reason": reason
    }
    with open(STATUS_FILE, "w") as file:
        json.dump(payload, file, indent=4)


def create_dataframe(data):
    rows = []
    for currency, rate in data["rates"].items():
        rows.append(
            {
                "base_currency": data["base_code"],
                "target_currency": currency,
                "exchange_rate": rate,
                "last_updated": data["time_last_update_utc"]
            }
        )
    return pd.DataFrame(rows)


def save_to_staging(df):
    STAGING_PATH.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved {len(df)} exchange rates")
    print(f"Location: {OUTPUT_FILE}")


def main():
    data = get_exchange_rates()
    df = create_dataframe(data)
    save_to_staging(df)


if __name__ == "__main__":
    main()