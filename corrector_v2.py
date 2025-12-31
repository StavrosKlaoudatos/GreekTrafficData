import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo
import re
from pathlib import Path

# Corrector v2: only touch rows that already have an ISO datetime with timezone
# (i.e. rows produced by the "other fetcher" examples you pasted).

HERE = os.path.dirname(__file__)
IN_FILE = os.path.join(HERE, 'toll_data.csv')


def _is_iso_with_tz(s: str) -> bool:
    if not s:
        return False
    # quick regex to detect strings like 2025-12-31 15:00:00+02:00
    return bool(re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$", s))


def _normalize_iso_with_tz(s: str) -> str:
    """Parse ISO-like datetime with tz and re-format as 'YYYY-MM-DD HH:MM:SS+HH:MM' in Europe/Athens.
    If parsing fails, return the original string.
    """
    try:
        # fromisoformat accepts the ' ' separator for date/time in Python 3.11+
        dt = datetime.fromisoformat(s)
    except Exception:
        # as a fallback, try replacing space with 'T'
        try:
            dt = datetime.fromisoformat(s.replace(' ', 'T'))
        except Exception:
            return s
    tz = ZoneInfo('Europe/Athens')
    # if dt has no tzinfo, assume Europe/Athens; otherwise convert to Europe/Athens
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    else:
        dt = dt.astimezone(tz)
    return dt.isoformat(sep=' ')


def correct_v2(in_path=IN_FILE):
    if not os.path.exists(in_path):
        print(f"Input file not found: {in_path}")
        return

    # read all rows
    with open(in_path, newline='') as f:
        rows = list(csv.reader(f))
    if not rows:
        print("No data in file")
        return

    header = rows[0]
    drop_right = bool(header and header[-1] in ('', 'None', 'none'))

    proc_header = list(header)
    if len(proc_header) > 2:
        proc_header[1] = 'datetime'
        del proc_header[2]
    if drop_right and proc_header and proc_header[-1] in ('', 'None', 'none'):
        proc_header = proc_header[:-1]

    out_rows = [proc_header]
    touched = 0

    for r in rows[1:]:
        # defensive copy
        rr = list(r)
        # drop trailing empty/None if present in source
        if drop_right and rr and rr[-1] in ('', 'None', 'none'):
            rr = rr[:-1]

        # Only consider rows that have at least 2 columns and the second looks like ISO+tz
        if len(rr) > 1 and _is_iso_with_tz(rr[1]):
            # normalize formatting and timezone
            new_dt = _normalize_iso_with_tz(rr[1])
            rr[1] = new_dt
            touched += 1

        out_rows.append(rr)

    if touched == 0:
        print("No matching rows found to correct (no ISO-with-tz datetimes detected).")
        return

    # backup original with timestamped filename
    stamp = datetime.now().strftime('%Y%m%dT%H%M%S')
    backup = Path(in_path).with_name(f"toll_data.backup.{stamp}.csv")
    os.replace(in_path, str(backup))
    print(f"Backed up original to {backup}")

    # write the corrected file to original path
    with open(in_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(out_rows)

    print(f"Wrote corrected file to {in_path} (touched {touched} rows)")


if __name__ == '__main__':
    correct_v2()
