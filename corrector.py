import csv
import os
from datetime import datetime
from zoneinfo import ZoneInfo

# Path to the CSV to correct (same as fetch_latest uses)
HERE = os.path.dirname(__file__)
IN_FILE = os.path.join(HERE, 'toll_data.csv')
BACKUP_FILE = os.path.join(HERE, f'toll_data.bak.csv')
OUT_FILE = os.path.join(HERE, 'toll_data_fixed.csv')


def _try_parse_datetime(date_str, hour_str):
    def _strip_angles(x):
        if x is None:
            return ''
        x = str(x).strip()
        return x.replace('<', '').replace('>', '').strip()

    date_clean = _strip_angles(date_str)
    hour_clean = _strip_angles(hour_str)
    s = f"{date_clean} {hour_clean}".strip()
    if not s:
        return s
    parts = s.split()
    if len(parts) == 2 and parts[1].isdigit():
        parts[1] = parts[1] + ':00'
        s = ' '.join(parts)

    fmts = [
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y %H",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H",
    ]
    tz = ZoneInfo("Europe/Athens")
    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            dt = dt.replace(tzinfo=tz)
            return dt.isoformat(sep=' ')
        except Exception:
            continue
    # loose attempt for YYYYMMDD
    try:
        date_part, hour_part = (parts[0], parts[1] if len(parts) > 1 else '0')
        if len(date_part) == 8 and date_part.isdigit():
            y = int(date_part[0:4]); m = int(date_part[4:6]); d = int(date_part[6:8])
            h = int(hour_part.split(':')[0]) if hour_part else 0
            dt = datetime(y, m, d, h, tzinfo=tz)
            return dt.isoformat(sep=' ')
    except Exception:
        pass
    return s


def _processed_row_from_raw(row, drop_right=False):
    if not row:
        return row
    r = list(row)
    date = r[1] if len(r) > 1 else ''
    hour = r[2] if len(r) > 2 else ''
    combined = _try_parse_datetime(date, hour)
    if len(r) > 1:
        r[1] = combined
    else:
        r.insert(1, combined)
    if len(r) > 2:
        del r[2]
    if drop_right and r:
        last = r[-1]
        if last in ('', 'None', 'none', None):
            r = r[:-1]
    return r


def correct_file(in_file=IN_FILE, out_file=OUT_FILE, backup_file=BACKUP_FILE):
    if not os.path.exists(in_file):
        print(f"Input file not found: {in_file}")
        return
    # detect drop_right by looking at header
    with open(in_file, newline='') as f:
        reader = list(csv.reader(f))
    if not reader:
        print("No data in file")
        return
    header = reader[0]
    drop_right = bool(header and header[-1] in ('', 'None', 'none'))
    # build processed header
    proc_header = list(header)
    if len(proc_header) > 2:
        proc_header[1] = 'datetime'
        del proc_header[2]
    if drop_right and proc_header and proc_header[-1] in ('', 'None', 'none'):
        proc_header = proc_header[:-1]

    rows = []
    for r in reader[1:]:
        rows.append(_processed_row_from_raw(r, drop_right=drop_right))

    # backup original
    try:
        os.replace(in_file, backup_file)
        print(f"Backed up original to {backup_file}")
    except Exception as e:
        print(f"Failed to backup original file: {e}")
        return

    # write corrected file
    with open(out_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(proc_header)
        writer.writerows(rows)
    # move fixed file to original path
    os.replace(out_file, in_file)
    print(f"Wrote corrected CSV to {in_file}")


if __name__ == '__main__':
    correct_file()
