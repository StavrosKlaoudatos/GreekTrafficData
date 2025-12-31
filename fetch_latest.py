import requests
import csv
import os
from io import StringIO
from datetime import datetime

# URL of the Hellastron "latest" feed
URL = "https://hellastron.imet.gr/tollways/latest.csv"
# Data file path (relative to repository root)
OUT_FILE = os.path.join(os.path.dirname(__file__), 'toll_data.csv')


def _try_parse_datetime(date_str, hour_str):
    """Try to parse common date+hour formats and return an ISO-like string.
    Falls back to the raw combined string if parsing fails.
    """
    s = f"{(date_str or '').strip()} {(hour_str or '').strip()}".strip()
    if not s:
        return s
    # Common formats seen in feeds: day/month/year and year-month-day, hours may be H or H:MM
    fmts = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H",
    ]
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            return dt.isoformat(sep=' ')
        except Exception:
            continue
    # If none matched, return the raw combined string
    return s


def _processed_row_from_raw(row, drop_right=False):
    """Return a processed row where date+hour are merged into one column.
    - Expects raw row where index 0=motorway, 1=date, 2=hour, 3=station, 4=direction (but is defensive).
    - If drop_right=True and the last column looks empty/'None', it will be removed.
    """
    if not row:
        return row
    r = list(row)
    # ensure at least indices exist
    date = r[1] if len(r) > 1 else ''
    hour = r[2] if len(r) > 2 else ''
    combined = _try_parse_datetime(date, hour)
    # replace date with combined
    if len(r) > 1:
        r[1] = combined
    else:
        r.insert(1, combined)
    # remove the original hour column if present
    if len(r) > 2:
        del r[2]
    # optionally drop trailing 'None' or empty column
    if drop_right and r:
        last = r[-1]
        if last in ('', 'None', 'none', None):
            r = r[:-1]
    return r


def _keys_from_existing_row(row):
    """Return a set of possible identity keys for an existing row.
    The identity is based on (motorway, date, hour, station, direction).
    Since older rows may have date and hour separate while newer rows will have them combined,
    we produce both variants when possible so duplicate detection works across formats.
    """
    keys = set()
    if not row:
        return keys
    r = list(row)
    # try the 'separate' form (if row long enough)
    if len(r) >= 5:
        try:
            keys.add(tuple(r[:5]))
        except Exception:
            pass
    # try the 'combined' form: datetime at index 1 -> split into date/hour and map station/direction
    if len(r) >= 4:
        motorway = r[0]
        dt_field = r[1]
        # split datetime-like into date and hour parts
        parts = str(dt_field).replace('T', ' ').split()
        date_part = parts[0] if parts else ''
        hour_part = parts[1] if len(parts) > 1 else ''
        station = r[2] if len(r) > 2 else ''
        direction = r[3] if len(r) > 3 else ''
        keys.add((motorway, date_part, hour_part, station, direction))
    return keys


def fetch_latest():
    """
    Download the latest CSV from Hellastron and append any new rows to OUT_FILE.
    For rows written from now on, date+hour will be rewritten into a single datetime column (index 1),
    and a trailing empty/'None' column will be dropped.
    Duplicate detection still uses the logical identity (motorway, date, hour, station, direction)
    so previously-stored rows (either format) won't be duplicated.
    """
    resp = requests.get(URL, timeout=30)
    resp.raise_for_status()

    # Read semicolon-separated values from the feed
    reader = csv.reader(StringIO(resp.text), delimiter=';')
    data = list(reader)
    if not data:
        return

    header = data[0]
    rows = data[1:]

    # Detect whether the feed has a trailing empty/None column (drop on write)
    drop_right = bool(header and header[-1] in ('', 'None', 'none'))

    # Prepare the processed header (date+hour -> datetime)
    proc_header = list(header)
    if len(proc_header) > 2:
        proc_header[1] = 'datetime'
        del proc_header[2]
    if drop_right and proc_header:
        if proc_header[-1] in ('', 'None', 'none'):
            proc_header = proc_header[:-1]

    # Build existing keys set using both possible formats so we detect duplicates across formats
    existing_keys = set()
    if os.path.exists(OUT_FILE):
        with open(OUT_FILE, newline='') as f:
            for r in csv.reader(f):
                existing_keys.update(_keys_from_existing_row(r))

    # Filter fetched rows for new ones (use the original feed key: first 5 cols if present)
    new_processed_rows = []
    for r in rows:
        key = None
        if len(r) >= 5:
            try:
                key = tuple(r[:5])
            except Exception:
                key = None
        # If key isn't present in existing_keys, this is new
        if key is None or key not in existing_keys:
            new_processed_rows.append(_processed_row_from_raw(r, drop_right=drop_right))

    if not new_processed_rows:
        return

    # If output file doesn't exist, write header first; otherwise append only the rows
    write_header = not os.path.exists(OUT_FILE)
    with open(OUT_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(proc_header)
        writer.writerows(new_processed_rows)


if __name__ == '__main__':
    fetch_latest()
