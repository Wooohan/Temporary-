"""
FMCSA ActPendInsur - All With History → existing `insurance_history` table loader.

Source : https://data.transportation.gov/resource/qh9u-swkp.json   (Socrata API)
Target : public.insurance_history (schema unchanged)

What it does
------------
* Fetches every insurance-transaction row whose `trans_date` falls in the
  user-supplied MM/DD/YYYY range.
* Maps the 11 API columns onto the DB columns; `mod_col_1` in the API
  is the human field `ins_type_desc` (Socrata renames it on the wire).
* Cleans `.0` tails from numeric-looking varchar fields.
* `dot_number` (API text) → BIGINT (strips leading zeros).
* `mc_num` (BIGINT) is **derived** from `docket_number` — the numeric tail
  of the docket (e.g. "MC026752" → 26752, "MC1572973" → 1572973).  Non-MC
  prefixes (FF, MX, …) leave `mc_num` NULL.
* `effective_date` / `cancl_effective_date` (API MM/DD/YYYY text) → DB DATE.
* Idempotent: deletes every existing row in the trans_date slice before
  reinserting — re-running the same range replaces rather than duplicates.

USAGE
    !pip install requests psycopg2-binary
    main("08/14/2023", "08/17/2023",
         "postgresql://postgres:****@switchyard.proxy.rlwy.net:57301/railway")
"""

import math
import re
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras

# ============================================================================
#  USER SETTINGS  ──  edit these to point at your DB / change the date range
# ============================================================================
DATE_START = "08/14/2023"            # trans_date >= this (MM/DD/YYYY)
DATE_END   = "08/17/2023"            # trans_date <= this (MM/DD/YYYY)
DB_URL     = "postgresql://postgres:XZmPgLxtDkRnJpwgWACsSMvejgRuSlKJ@switchyard.proxy.rlwy.net:57301/railway"
# ============================================================================

SOCRATA_URL  = "https://data.transportation.gov/resource/qh9u-swkp.json"
PAGE_SIZE    = 50_000
INSERT_BATCH = 1000
TABLE        = "insurance_history"
DATES_PER_IN = 800   # how many MM/DD/YYYY values to pack into one SoQL IN clause


# ────────────────────────────────────────────────────────────────────────────
#  CLEANERS
# ────────────────────────────────────────────────────────────────────────────

def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def strip_dot_zero(v: Any) -> Optional[str]:
    """'1234.0' → '1234'.  Non-numeric strings returned unchanged."""
    s = _s(v)
    if s is None:
        return None
    if s.endswith(".0"):
        try:
            float(s)
            s = s[:-2]
        except ValueError:
            pass
    return s


def clean_bigint(v: Any) -> Optional[int]:
    """API integer-as-text → Python int.  Strips leading zeros / '.0' tails."""
    s = _s(v)
    if s is None:
        return None
    if s.endswith(".0"):
        s = s[:-2]
    try:
        return int(s)
    except ValueError:
        try:
            f = float(s)
            if math.isnan(f) or math.isinf(f):
                return None
            return int(f)
        except (ValueError, OverflowError):
            return None


_MC_TAIL = re.compile(r"^MC0*(\d+)$", re.IGNORECASE)

def extract_mc_num(docket: Optional[str]) -> Optional[int]:
    """'MC026752' → 26752, 'MC1572973' → 1572973, 'FF1234' → None."""
    if not docket:
        return None
    m = _MC_TAIL.match(docket.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def clean_us_date(v: Any) -> Optional[str]:
    """MM/DD/YYYY → 'YYYY-MM-DD' (for DATE columns).  Returns None on bad input."""
    s = _s(v)
    if s is None:
        return None
    # API gives 'MM/DD/YYYY'; sometimes ISO 'YYYY-MM-DD' shows up too.
    if "-" in s[:10]:
        try:
            y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
            if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            return None
        return None
    if "/" in s:
        try:
            m, d, y = s.split("/")
            mi, di, yi = int(m), int(d), int(y)
            if 1900 <= yi <= 2100 and 1 <= mi <= 12 and 1 <= di <= 31:
                return f"{yi:04d}-{mi:02d}-{di:02d}"
        except ValueError:
            return None
    return None


def truncate_varchar(value: Optional[str], maxlen: int) -> Optional[str]:
    """Defensive truncation so an oversized API value never breaks an insert."""
    if value is None:
        return None
    if len(value) <= maxlen:
        return value
    return value[:maxlen]


# Per-column widths from the live schema (\d insurance_history)
COL_WIDTHS = {
    "docket_number":     20,
    "ins_form_code":     10,
    "ins_type_desc":     50,
    "name_company":     100,
    "policy_no":         50,
    "trans_date":        15,
    "underl_lim_amount": 15,
    "max_cov_amount":    15,
}


# Order of columns we INSERT into (NOT `id` — it auto-increments)
INSERT_COLS = [
    "docket_number",
    "dot_number",
    "ins_form_code",
    "ins_type_desc",
    "name_company",
    "policy_no",
    "trans_date",
    "underl_lim_amount",
    "max_cov_amount",
    "effective_date",
    "cancl_effective_date",
    "mc_num",
]


def transform_row(api_row: Dict[str, Any]) -> Optional[Tuple]:
    """Convert one API row to a positional tuple for INSERT."""
    docket = strip_dot_zero(api_row.get("docket_number"))
    dot    = clean_bigint(api_row.get("dot_number"))
    if docket is None and dot is None:
        return None
    return (
        truncate_varchar(docket,                                                   COL_WIDTHS["docket_number"]),
        dot,
        truncate_varchar(strip_dot_zero(api_row.get("ins_form_code")),             COL_WIDTHS["ins_form_code"]),
        # API renames its `ins_type_desc` field to `mod_col_1` on the wire.
        truncate_varchar(_s(api_row.get("mod_col_1") or api_row.get("ins_type_desc")),
                         COL_WIDTHS["ins_type_desc"]),
        truncate_varchar(_s(api_row.get("name_company")),                          COL_WIDTHS["name_company"]),
        truncate_varchar(strip_dot_zero(api_row.get("policy_no")),                 COL_WIDTHS["policy_no"]),
        truncate_varchar(_s(api_row.get("trans_date")),                            COL_WIDTHS["trans_date"]),
        truncate_varchar(strip_dot_zero(api_row.get("underl_lim_amount")),         COL_WIDTHS["underl_lim_amount"]),
        truncate_varchar(strip_dot_zero(api_row.get("max_cov_amount")),            COL_WIDTHS["max_cov_amount"]),
        clean_us_date(api_row.get("effective_date")),
        clean_us_date(api_row.get("cancl_effective_date")),
        extract_mc_num(docket),
    )


# ────────────────────────────────────────────────────────────────────────────
#  SOCRATA FETCH
# ────────────────────────────────────────────────────────────────────────────

def fetch_count(session: requests.Session, where: str) -> int:
    r = session.get(SOCRATA_URL, params={"$select": "count(*)", "$where": where}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return int(data[0].get("count") or 0) if data else 0


def fetch_page(session, where, limit, offset):
    params = {"$where": where, "$limit": limit, "$offset": offset,
              "$order": "docket_number, policy_no, trans_date"}
    last_err = None
    for attempt in range(1, 5):
        try:
            r = session.get(SOCRATA_URL, params=params, timeout=180)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep = 2 ** attempt
            print(f"   fetch retry {attempt}/4 (sleep {sleep}s): {e}", flush=True)
            time.sleep(sleep)
    raise RuntimeError(f"Socrata fetch failed: {last_err}")


# trans_date on the API side is text MM/DD/YYYY.  A bare
# `trans_date >= '08/14/2023' AND trans_date <= '08/17/2023'` is a *lexical*
# string compare, so '08/14/2024' satisfies it too (because '4' > '3'),
# pulling in every year whose MM/DD falls in the window.  To filter by a
# real calendar range we enumerate every MM/DD/YYYY in [start, end] and
# use SoQL `trans_date IN (...)`.

def _parse_mdY(s: str) -> date:
    return datetime.strptime(s, "%m/%d/%Y").date()


def enumerate_dates(date_start: str, date_end: str) -> List[str]:
    d0 = _parse_mdY(date_start)
    d1 = _parse_mdY(date_end)
    if d1 < d0:
        raise ValueError(f"end {date_end} is before start {date_start}")
    out: List[str] = []
    cur = d0
    while cur <= d1:
        out.append(cur.strftime("%m/%d/%Y"))
        cur += timedelta(days=1)
    return out


def build_where(dates: List[str]) -> str:
    quoted = ", ".join("'" + d + "'" for d in dates)
    return f"trans_date IN ({quoted})"


def _iter_chunk(session: requests.Session, where: str, label: str):
    total = fetch_count(session, where)
    print(f"  [{label}] Socrata reports {total:,} rows", flush=True)
    if total == 0:
        return
    fetched = 0
    offset  = 0
    while fetched < total:
        page = fetch_page(session, where, PAGE_SIZE, offset)
        if not page:
            break
        for row in page:
            yield row
        fetched += len(page)
        offset  += len(page)
        if len(page) < PAGE_SIZE:
            break


def iter_rows(date_start: str, date_end: str):
    """Yield every API row whose trans_date is a real calendar date in [start, end]."""
    dates = enumerate_dates(date_start, date_end)
    print(f"Real-date range: {len(dates)} day(s) from {date_start} to {date_end}", flush=True)
    s = requests.Session()
    # Chunk the IN list so the URL stays under any reasonable size limit.
    for i in range(0, len(dates), DATES_PER_IN):
        chunk = dates[i:i + DATES_PER_IN]
        where = build_where(chunk)
        label = f"{chunk[0]}…{chunk[-1]}" if len(chunk) > 1 else chunk[0]
        yield from _iter_chunk(s, where, label)


# ────────────────────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────────────────────

def main(date_start: str, date_end: str, db_url: str) -> Dict[str, int]:
    t0 = time.time()
    print("=== FMCSA → insurance_history loader ===")
    print(f"Range: trans_date in [{date_start}, {date_end}]")

    transformed: List[Tuple] = []
    skipped = 0
    fetched_total = 0
    trans_dates_seen = set()
    for row in iter_rows(date_start, date_end):
        fetched_total += 1
        vals = transform_row(row)
        if vals is None:
            skipped += 1
            continue
        transformed.append(vals)
        if vals[6] is not None:
            trans_dates_seen.add(vals[6])

    if fetched_total == 0:
        return {"fetched": 0, "inserted": 0, "replaced": 0, "skipped": 0}

    print("\nConnecting to PostgreSQL …", flush=True)
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        # Idempotent slice replacement: delete existing rows in the trans_date
        # range, then insert the API rows.  Done in one transaction.
        replaced = 0
        if trans_dates_seen:
            cur.execute(
                f"DELETE FROM {TABLE} WHERE trans_date = ANY(%s)",
                (sorted(trans_dates_seen),),
            )
            replaced = cur.rowcount
            print(f"  removed {replaced:,} pre-existing rows in those trans_dates",
                  flush=True)

        cols_str = ", ".join(INSERT_COLS)
        insert_sql = f"INSERT INTO {TABLE} ({cols_str}) VALUES %s"
        inserted = 0
        for i in range(0, len(transformed), INSERT_BATCH):
            batch = transformed[i:i + INSERT_BATCH]
            psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=INSERT_BATCH)
            inserted += len(batch)
            if (i // INSERT_BATCH) % 10 == 0 and i > 0:
                print(f"  inserted {inserted:,}/{len(transformed):,}", flush=True)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.0f}s.")
    print(f"  fetched : {fetched_total:,}")
    print(f"  skipped : {skipped:,} (no docket or dot)")
    print(f"  replaced: {replaced:,} pre-existing rows")
    print(f"  inserted: {inserted:,} new rows")
    return {
        "fetched": fetched_total,
        "inserted": inserted,
        "replaced": replaced,
        "skipped": skipped,
    }


# ────────────────────────────────────────────────────────────────────────────
#  CLI / Colab entry point
# ────────────────────────────────────────────────────────────────────────────

def _running_in_notebook() -> bool:
    """True if executed inside a Jupyter / Colab / IPython kernel."""
    import sys
    return ("ipykernel" in sys.modules
            or "google.colab" in sys.modules
            or "IPython" in sys.modules)


if __name__ == "__main__":
    import os, sys
    if _running_in_notebook():
        # Inside Colab / Jupyter: argparse would choke on the kernel's
        # `-f kernel.json` arg, so we skip CLI parsing entirely and just use
        # the constants from the USER SETTINGS block at the top of this file.
        main(DATE_START, DATE_END, os.environ.get("DB_URL", DB_URL))
    else:
        import argparse
        ap = argparse.ArgumentParser(description="FMCSA Socrata → insurance_history loader")
        ap.add_argument("--start", default=DATE_START, help="MM/DD/YYYY (inclusive)")
        ap.add_argument("--end",   default=DATE_END,   help="MM/DD/YYYY (inclusive)")
        ap.add_argument("--db",    default=os.environ.get("DB_URL", DB_URL))
        args = ap.parse_args()
        main(args.start, args.end, args.db)
