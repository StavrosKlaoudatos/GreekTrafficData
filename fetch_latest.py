import requests
import csv
import os
from io import StringIO

# URL of the Hellastron "latest" feed
URL = "https://hellastron.imet.gr/tollways/latest.csv"
# Data file path (relative to repository root)
OUT_FILE = os.path.join(os.path.dirname(__file__), 'toll_data.csv')


def fetch_latest():
    """
    Download the latest CSV from Hellastron and append any new rows to OUT_FILE.
    Each record is identified by the first 5 columns (motorway, date, hour, station, direction).
    """
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()

    # Read semicolon-separated values
    reader = csv.reader(StringIO(resp.text), delimiter=';')
    data = list(reader)

    # Ensure output file exists
    existing_keys = set()
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE, newline='') as f:
            for row in csv.reader(f):
                existing_keys.add(tuple(row[:5]))

    # Append new rows
    new_rows = [row for row in data if tuple(row[:5]) not in existing_keys]
    if new_rows:
        with open(OUT_FILE, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(new_rows)


if __name__ == '__main__':
    fetch_latest()
