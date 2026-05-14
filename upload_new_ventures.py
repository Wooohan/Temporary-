"""
FMCSA Company-Census → existing `new_ventures` table loader.

Source : https://data.transportation.gov/resource/az4n-8mr2.json   (Socrata API)
Target : public.new_ventures  (schema unchanged — every column is TEXT
         plus the `raw_data` JSONB column + scrape_date)

This script does NOT alter the table schema.  It maps Socrata API field
names to the legacy `new_ventures` column names, strips ".0" tails from
numeric-looking values, normalises dates to ISO `YYYY-MM-DD`, and stores
the full original API row in `raw_data` for traceability.

Columns that come from the FMCSA L_NEW_VENTURE / SAFER scrape and are NOT
present in the Socrata API are left NULL: common_stat, contract_stat,
broker_stat, *_app_pend, *_rev_pend, property_chk, passenger_chk,
hhg_chk, private_auth_chk, enterprise_chk, operating_status*,
bipd_req, cargo_req, bond_req, bipd_file, cargo_file, bond_file,
total_trucks, total_buses, total_pwr, arber, smartway,
tia*, phy_ups_store, mai_ups_store, phy_mail_box, mai_mail_box.

USAGE
    !pip install requests psycopg2-binary
    main("2026-04-01", "2026-05-31",
         "postgresql://postgres:****@switchyard.proxy.rlwy.net:57301/railway")
"""
import json
import math
import time
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import requests
import psycopg2
import psycopg2.extras

SOCRATA_URL  = "https://data.transportation.gov/resource/az4n-8mr2.json"
PAGE_SIZE    = 50_000
INSERT_BATCH = 500
TABLE        = "new_ventures"


# ────────────────────────────────────────────────────────────────────────────
#  CLEANERS  (light — `new_ventures` is all-text, so we just produce strings)
# ────────────────────────────────────────────────────────────────────────────

def _s(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def strip_dot_zero(v: Any) -> Optional[str]:
    """'7062949868.0' → '7062949868'.  Non-numeric strings are returned as-is."""
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


def clean_int_str(v: Any) -> Optional[str]:
    """Force '123' (no '.0').  Returns None for letter codes (e.g. fleetsize 'A')."""
    s = _s(v)
    if s is None:
        return None
    if s.endswith(".0"):
        s = s[:-2]
    try:
        return str(int(s))
    except ValueError:
        try:
            f = float(s)
            if math.isnan(f) or math.isinf(f):
                return None
            return str(int(f))
        except (ValueError, OverflowError):
            return None


def clean_date_iso(v: Any) -> Optional[str]:
    """Accepts 'YYYYMMDD', 'YYYYMMDD HHMM' or ISO 'YYYY-MM-DD' → 'YYYY-MM-DD' text."""
    s = _s(v)
    if s is None:
        return None
    if "-" in s[:10]:
        try:
            y, m, d = int(s[:4]), int(s[5:7]), int(s[8:10])
            if 1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= d <= 31:
                return f"{y:04d}-{m:02d}-{d:02d}"
        except ValueError:
            return None
        return None
    d = s.replace(" ", "")[:8]
    if len(d) < 8 or not d.isdigit():
        return None
    try:
        y, m, day = int(d[:4]), int(d[4:6]), int(d[6:8])
    except ValueError:
        return None
    if not (1900 <= y <= 2100 and 1 <= m <= 12 and 1 <= day <= 31):
        return None
    return f"{y:04d}-{m:02d}-{day:02d}"


# ────────────────────────────────────────────────────────────────────────────
#  FIELD MAPPING  Socrata field → (new_ventures column, cleaner)
# ────────────────────────────────────────────────────────────────────────────
# Cleaners:
#   's'    = string (strip + .0 tail-removal)
#   'int'  = integer-as-string ('1' not '1.0', letters → NULL)
#   'date' = ISO 'YYYY-MM-DD' text
#   'asis' = pass-through string (used for X-flag cargo columns, single chars)
MAP: List[Tuple[str, str, str]] = [
    # identity / dockets
    ("dot_number",                  "dot_number",          "int"),
    ("docket1prefix",               "prefix",              "s"),
    ("docket1",                     "docket_number",       "s"),
    ("status_code",                 "status_code",         "asis"),
    ("carship",                     "carship",             "asis"),
    ("carrier_operation",           "carrier_operation",   "asis"),
    ("legal_name",                  "name",                "s"),
    ("dba_name",                    "name_dba",            "s"),
    ("add_date",                    "add_date",            "date"),
    # physical address
    ("phy_street",                  "phy_str",             "s"),
    ("phy_city",                    "phy_city",            "s"),
    ("phy_state",                   "phy_st",              "s"),
    ("phy_zip",                     "phy_zip",             "s"),
    ("phy_country",                 "phy_country",         "s"),
    ("phy_cnty",                    "phy_cnty",            "s"),
    ("undeliv_phy",                 "phy_undeliv",         "asis"),
    # mailing address
    ("carrier_mailing_street",      "mai_str",             "s"),
    ("carrier_mailing_city",        "mai_city",            "s"),
    ("carrier_mailing_state",       "mai_st",              "s"),
    ("carrier_mailing_zip",         "mai_zip",             "s"),
    ("carrier_mailing_country",     "mai_country",         "s"),
    ("carrier_mailing_cnty",        "mai_cnty",            "s"),
    ("carrier_mailing_und_date",    "mai_undeliv",         "asis"),
    # contact
    ("phone",                       "phy_phone",           "s"),
    ("fax",                         "phy_fax",             "s"),
    ("cell_phone",                  "cell_phone",          "s"),
    ("email_address",               "email_address",       "s"),
    ("company_officer_1",           "company_officer_1",   "s"),
    ("company_officer_2",           "company_officer_2",   "s"),
    # cargo flags (Socrata `crgo_*` → bare names in new_ventures)
    ("crgo_genfreight",             "genfreight",          "asis"),
    ("crgo_household",              "household",           "asis"),
    ("crgo_metalsheet",             "metalsheet",          "asis"),
    ("crgo_motoveh",                "motorveh",            "asis"),
    ("crgo_drivetow",               "drivetow",            "asis"),
    ("crgo_logpole",                "logpole",             "asis"),
    ("crgo_bldgmat",                "bldgmat",             "asis"),
    ("crgo_mobilehome",             "mobilehome",          "asis"),
    ("crgo_machlrg",                "machlrg",             "asis"),
    ("crgo_produce",                "produce",             "asis"),
    ("crgo_liqgas",                 "liqgas",              "asis"),
    ("crgo_intermodal",             "intermodal",          "asis"),
    ("crgo_passengers",             "passengers",          "asis"),
    ("crgo_oilfield",               "oilfield",            "asis"),
    ("crgo_livestock",              "livestock",           "asis"),
    ("crgo_grainfeed",              "grainfeed",           "asis"),
    ("crgo_coalcoke",               "coalcoke",            "asis"),
    ("crgo_meat",                   "meat",                "asis"),
    ("crgo_garbage",                "garbage",             "asis"),
    ("crgo_usmail",                 "usmail",              "asis"),
    ("crgo_chem",                   "chem",                "asis"),
    ("crgo_drybulk",                "drybulk",             "asis"),
    ("crgo_coldfood",               "coldfood",            "asis"),
    ("crgo_beverages",              "beverages",           "asis"),
    ("crgo_paperprod",              "paperprod",           "asis"),
    ("crgo_utility",                "utility",             "asis"),
    ("crgo_farmsupp",               "farmsupp",            "asis"),
    ("crgo_construct",              "construct",           "asis"),
    ("crgo_waterwell",              "waterwell",           "asis"),
    ("crgo_cargoothr",              "cargoothr",           "asis"),
    ("crgo_cargoothr_desc",         "cargoothr_desc",      "s"),
    ("hm_ind",                      "hm_ind",              "asis"),
    # equipment (owned / term-leased / trip-leased) — all integer-as-string
    ("owntruck",                    "owntruck",            "int"),
    ("owntract",                    "owntract",            "int"),
    ("owntrail",                    "owntrail",            "int"),
    ("owncoach",                    "owncoach",            "int"),
    ("ownschool_1_8",               "ownschool_1_8",       "int"),
    ("ownschool_9_15",              "ownschool_9_15",      "int"),
    ("ownschool_16",                "ownschool_16",        "int"),
    ("ownbus_16",                   "ownbus_16",           "int"),
    ("ownvan_1_8",                  "ownvan_1_8",          "int"),
    ("ownvan_9_15",                 "ownvan_9_15",         "int"),
    ("ownlimo_1_8",                 "ownlimo_1_8",         "int"),
    ("ownlimo_9_15",                "ownlimo_9_15",        "int"),
    ("ownlimo_16",                  "ownlimo_16",          "int"),
    ("trmtruck",                    "trmtruck",            "int"),
    ("trmtract",                    "trmtract",            "int"),
    ("trmtrail",                    "trmtrail",            "int"),
    ("trmcoach",                    "trmcoach",            "int"),
    ("trmschool_1_8",               "trmschool_1_8",       "int"),
    ("trmschool_9_15",              "trmschool_9_15",      "int"),
    ("trmschool_16",                "trmschool_16",        "int"),
    ("trmbus_16",                   "trmbus_16",           "int"),
    ("trmvan_1_8",                  "trmvan_1_8",          "int"),
    ("trmvan_9_15",                 "trmvan_9_15",         "int"),
    ("trmlimo_1_8",                 "trmlimo_1_8",         "int"),
    ("trmlimo_9_15",                "trmlimo_9_15",        "int"),
    ("trmlimo_16",                  "trmlimo_16",          "int"),
    ("trptruck",                    "trptruck",            "int"),
    ("trptract",                    "trptract",            "int"),
    ("trptrail",                    "trptrail",            "int"),
    ("trpcoach",                    "trpcoach",            "int"),
    ("trpschool_1_8",               "trpschool_1_8",       "int"),
    ("trpschool_9_15",              "trpschool_9_15",      "int"),
    ("trpschool_16",                "trpschool_16",        "int"),
    ("trpbus_16",                   "trpbus_16",           "int"),
    ("trpvan_1_8",                  "trpvan_1_8",          "int"),
    ("trpvan_9_15",                 "trpvan_9_15",         "int"),
    ("trplimo_1_8",                 "trplimo_1_8",         "int"),
    ("trplimo_9_15",                "trplimo_9_15",        "int"),
    ("trplimo_16",                  "trplimo_16",          "int"),
    # fleet / driver totals
    ("fleetsize",                   "fleetsize",           "s"),         # letter codes preserved
    ("interstate_within_100_miles", "inter_within_100",    "int"),
    ("interstate_beyond_100_miles", "inter_beyond_100",    "int"),
    ("driver_inter_total",          "total_inter_drivers", "int"),
    ("intrastate_within_100_miles", "intra_within_100",    "int"),
    ("intrastate_beyond_100_miles", "intra_beyond_100",    "int"),
    ("total_intrastate_drivers",    "total_intra_drivers", "int"),
    ("total_drivers",               "total_drivers",       "int"),
    ("avg_drivers_leased_per_month","avg_tld",             "s"),
    ("total_cdl",                   "total_cdl",           "int"),
    # safety / reviews
    ("review_type",                 "review_type",         "asis"),
    ("review_id",                   "review_id",           "s"),
    ("review_date",                 "review_date",         "date"),
    ("recordable_crash_rate",       "recordable_crash_rate","s"),
    ("mcs150_mileage",              "mcs150_mileage",      "int"),
    ("mcs151_mileage",              "mcs151_mileage",      "int"),
    ("mcs150_mileage_year",         "mcs150_mileage_year", "int"),
    ("mcs150_date",                 "mcs150_date",         "date"),
    ("safety_rating",               "safety_rating",       "asis"),
    ("safety_rating_date",          "safety_rating_date",  "date"),
]

# Apply cleaner by name
def _clean(value: Any, kind: str) -> Optional[str]:
    if kind == "s":     return strip_dot_zero(value)
    if kind == "int":   return clean_int_str(value)
    if kind == "date":  return clean_date_iso(value)
    if kind == "asis":  return _s(value)
    return _s(value)


# Columns from the API row that we serialise into raw_data after light cleaning.
# (Keeps the full census record reproducible from the row.)
def _build_raw_data(row: Dict[str, Any]) -> str:
    out = {}
    for k, v in row.items():
        s = strip_dot_zero(v)
        if s is not None:
            out[k] = s
    return json.dumps(out, separators=(",", ":"))


# Final INSERT column order = MAP destinations + raw_data + scrape_date
INSERT_COLS = [dst for _, dst, _ in MAP] + ["raw_data", "scrape_date"]
ON_CONFLICT_UPDATE = ", ".join(
    f"{c} = EXCLUDED.{c}" for c in INSERT_COLS
) + ", updated_at = NOW()"


def transform_row(row: Dict[str, Any], scrape_date: str) -> Optional[Tuple]:
    dot = clean_int_str(row.get("dot_number"))
    if dot is None:
        return None
    out = [_clean(row.get(src), kind) for src, _, kind in MAP]
    out.append(_build_raw_data(row))     # raw_data
    out.append(scrape_date)               # scrape_date
    return tuple(out)


# ────────────────────────────────────────────────────────────────────────────
#  SOCRATA FETCH
# ────────────────────────────────────────────────────────────────────────────

def fetch_count(session: requests.Session, where: str) -> int:
    r = session.get(SOCRATA_URL, params={"$select": "count(*)", "$where": where}, timeout=60)
    r.raise_for_status()
    data = r.json()
    return int(data[0].get("count") or 0) if data else 0


def fetch_page(session, where, limit, offset):
    params = {"$where": where, "$limit": limit, "$offset": offset, "$order": "dot_number"}
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


def iter_rows(date_start: str, date_end: str, app_token: str = ""):
    where = f"add_date between '{date_start}T00:00:00' and '{date_end}T23:59:59'"
    s = requests.Session()
    if app_token:
        s.headers["X-App-Token"] = app_token
    total = fetch_count(s, where)
    print(f"Socrata reports {total:,} rows for add_date in [{date_start}, {date_end}].",
          flush=True)
    if total == 0:
        return
    fetched = 0
    offset = 0
    while fetched < total:
        page = fetch_page(s, where, PAGE_SIZE, offset)
        if not page:
            break
        for row in page:
            yield row
        fetched += len(page)
        offset += len(page)
        print(f"  fetched {fetched:,}/{total:,} rows", flush=True)
        if len(page) < PAGE_SIZE:
            break


# ────────────────────────────────────────────────────────────────────────────
#  MAIN
# ────────────────────────────────────────────────────────────────────────────

def main(date_start: str, date_end: str, db_url: str, app_token: str = "") -> Dict[str, int]:
    t0 = time.time()
    print("=== FMCSA → new_ventures loader ===")
    print(f"Range: {date_start} → {date_end}")

    scrape_date = date.today().isoformat()
    transformed = []
    skipped = 0
    fetched_total = 0
    for row in iter_rows(date_start, date_end, app_token):
        fetched_total += 1
        vals = transform_row(row, scrape_date)
        if vals is None:
            skipped += 1
            continue
        transformed.append(vals)

    if fetched_total == 0:
        return {"fetched": 0, "inserted": 0, "updated": 0, "skipped": 0}

    print("\nConnecting to PostgreSQL …", flush=True)
    conn = psycopg2.connect(db_url)
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cols_str = ", ".join(INSERT_COLS)
        insert_sql = (
            f"INSERT INTO {TABLE} ({cols_str}) VALUES %s "
            f"ON CONFLICT (dot_number, add_date) DO UPDATE SET {ON_CONFLICT_UPDATE}"
        )
        # Bulk insert via execute_values — ~30x faster than per-row INSERT
        # Reports total rows affected (inserted + updated combined).
        affected = 0
        for i in range(0, len(transformed), INSERT_BATCH):
            batch = transformed[i:i+INSERT_BATCH]
            psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=INSERT_BATCH)
            affected += cur.rowcount
            if (i // INSERT_BATCH) % 10 == 0 and i > 0:
                print(f"  processed {i+len(batch):,}/{len(transformed):,}", flush=True)
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
    print(f"  skipped : {skipped:,} (no dot_number)")
    print(f"  upserted: {affected:,} rows into {TABLE} (inserted + updated)")
    return {
        "fetched": fetched_total,
        "upserted": affected,
        "skipped": skipped,
    }


# ────────────────────────────────────────────────────────────────────────────
#  CLI
# ────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse, os
    ap = argparse.ArgumentParser(description="FMCSA Socrata → new_ventures loader")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end",   required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument(
        "--db",
        default=os.environ.get(
            "DB_URL",
            "postgresql://postgres:XZmPgLxtDkRnJpwgWACsSMvejgRuSlKJ@switchyard.proxy.rlwy.net:57301/railway",
        ),
    )
    ap.add_argument("--app-token", default=os.environ.get("SOCRATA_APP_TOKEN", ""))
    args = ap.parse_args()
    main(args.start, args.end, args.db, args.app_token)
