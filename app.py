import io
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

# ------------------------------
# Streamlit config
# ------------------------------
st.set_page_config(
    page_title="DEFRA BNG Metric Reader",
    page_icon="🌿",
    layout="wide"
)

# ------------------------------
# Utility: Normalisation helpers
# ------------------------------
def normalise_ws_name(name: str) -> str:
    return (name or "").strip().lower().replace(" ", "")

def clean_col(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    return s

def canon_col(s: str) -> str:
    """Lowercase, collapse spaces/punct for fuzzy matching."""
    s = clean_col(s).lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

# Some likely column name patterns across metric variants
CANDIDATE_HAB_COLS = [
    "habitat", "habitat_type", "bng_habitat", "broad_habitat", "feature",
]
CANDIDATE_LENGTH_COLS = [
    "length", "hedgerow_length", "total_length_m", "length_m", "length_(m)"
]
CANDIDATE_AREA_COLS = [
    "area", "total_area_ha", "area_ha", "ha"
]
CANDIDATE_DISTINCTIVENESS = [
    "distinctiveness", "distinctiveness_band"
]
CANDIDATE_CONDITION = [
    "condition"
]

# deficits/off-site requirement candidates (any negative is a requirement)
CANDIDATE_DEFICIT_COLS = [
    # common wordings
    "net_change", "change_in_units", "balance", "net_units",
    "net_total", "units_balance", "required_off_site",
    "off_site_requirement", "offsite_requirement", "off_site_units",
    "offsite_units", "units_shortfall", "shortfall",
]

def find_header_row(df: pd.DataFrame, required_any: List[str], max_scan: int = 30) -> Optional[int]:
    """
    Scan top rows to find the header row that contains at least ONE of required_any
    (after canonicalising).
    """
    required_any_canon = {canon_col(x) for x in required_any}
    for i in range(min(max_scan, len(df))):
        row = df.iloc[i].astype(str).tolist()
        canon = [canon_col(x) for x in row]
        if any(c in canon for c in required_any_canon):
            return i
    return None

def reheader(df_raw: pd.DataFrame, header_row: int) -> pd.DataFrame:
    headers = df_raw.iloc[header_row].astype(str).map(clean_col).tolist()
    out = df_raw.iloc[header_row+1:].copy()
    out.columns = headers
    out = out.loc[:, ~out.columns.duplicated()].copy()
    out = out.reset_index(drop=True)
    return out

def best_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    cols_canon = {canon_col(c): c for c in df.columns}
    for c in candidates:
        cc = canon_col(c)
        if cc in cols_canon:
            return cols_canon[cc]
    # fuzzy contains
    for c in df.columns:
        cc = canon_col(c)
        if any(canon_col(x) in cc for x in candidates):
            return c
    return None

def coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def is_deficit_series(s: pd.Series) -> pd.Series:
    # negative = requirement/shortfall
    return coerce_numeric(s) < 0

def extract_deficits(df: pd.DataFrame) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Try to find a single 'deficit/off-site' column. If not found, create a 'inferred_deficit'
    from the first numeric column that contains negatives.
    Returns (df_with_deficit_col, name_of_deficit_col)
    """
    # Try explicit candidates first
    for c in df.columns:
        cc = canon_col(c)
        if any(cc == canon_col(x) for x in CANDIDATE_DEFICIT_COLS):
            return df, c

    # Else, infer: find the first numeric-ish column with any negatives
    numericish = []
    for c in df.columns:
        ser = coerce_numeric(df[c])
        if ser.notna().any():
            numericish.append((c, ser))

    for c, ser in numericish:
        if (ser < 0).any():
            # treat this as the deficit column
            name = c
            return df, name

    return df, None

def tidy_numeric_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        ser = coerce_numeric(out[c])
        # keep numeric where appropriate; otherwise keep text
        if ser.notna().sum() >= max(2, int(0.2 * len(out))):
            out[c] = ser
    return out

def load_sheet(xls: pd.ExcelFile, sheet_name: str) -> Optional[pd.DataFrame]:
    try:
        raw = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
    except Exception:
        return None
    if raw is None or raw.empty:
        return None

    # look for header row containing any of our likely columns
    header_idx = find_header_row(
        raw,
        required_any=CANDIDATE_HAB_COLS + CANDIDATE_DEFICIT_COLS + CANDIDATE_AREA_COLS + CANDIDATE_LENGTH_COLS
    )
    if header_idx is None:
        # fallback: first row as header
        df = pd.read_excel(xls, sheet_name=sheet_name)
        return df

    df = reheader(raw, header_row=header_idx)
    return df

def find_sheet_name(xls: pd.ExcelFile, targets: List[str]) -> Optional[str]:
    existing = {normalise_ws_name(s): s for s in xls.sheet_names}
    for t in targets:
        key = normalise_ws_name(t)
        if key in existing:
            return existing[key]
    # loose match: contains
    for s in xls.sheet_names:
        sn = normalise_ws_name(s)
        if any(normalise_ws_name(t) in sn for t in targets):
            return s
    return None

# ------------------------------
# Parsing pipeline per category
# ------------------------------
def parse_trading_summary(
    xls: pd.ExcelFile,
    target_sheet_candidates: List[str],
    expected_type: str
) -> Tuple[Optional[pd.DataFrame], List[str]]:
    logs = []
    sheet = find_sheet_name(xls, target_sheet_candidates)
    if not sheet:
        logs.append(f"Could not find sheet matching {target_sheet_candidates}")
        return None, logs

    df = load_sheet(xls, sheet)
    if df is None or df.empty:
        logs.append(f"Sheet '{sheet}' is empty or unreadable.")
        return None, logs

    # Clean + normalise
    df.columns = [clean_col(c) for c in df.columns]
    df = df.dropna(how="all").reset_index(drop=True)
    df = tidy_numeric_cols(df)

    # Identify key columns
    hab_col = best_col(df, CANDIDATE_HAB_COLS) or "Habitat"
    if hab_col not in df.columns:
        logs.append(
            f"Couldn't confidently identify a habitat/feature column in '{sheet}'. "
            f"Showing all columns."
        )
        hab_col = df.columns[0]  # fall back to first column

    # Distinctiveness/Condition (optional)
    distinct_col = best_col(df, CANDIDATE_DISTINCTIVENESS)
    cond_col = best_col(df, CANDIDATE_CONDITION)

    # Area/Length (optional, depends on category)
    area_col = best_col(df, CANDIDATE_AREA_COLS)
    length_col = best_col(df, CANDIDATE_LENGTH_COLS)

    # Try to locate an explicit deficit/off-site column, else infer
    df, deficit_col = extract_deficits(df)

    # If we found a deficit col, filter rows that are negative
    offsite_df = None
    if deficit_col:
        mask = is_deficit_series(df[deficit_col])
        offsite_df = df.loc[mask].copy()
        if offsite_df.empty:
            logs.append(
                f"No negative/required rows found in '{sheet}' for column '{deficit_col}'."
            )
    else:
        logs.append(
            f"Could not find explicit off-site/deficit column in '{sheet}'. "
            f"Try mapping one of your numeric columns (e.g. Net change / Balance) to deficits."
        )
        offsite_df = pd.DataFrame(columns=df.columns)

    # Build a tidy “view” with useful columns if present
    keep_cols = [c for c in [hab_col, distinct_col, cond_col, area_col, length_col, deficit_col] if c and c in df.columns]
    if keep_cols:
        view = df[keep_cols].copy()
    else:
        view = df.copy()

    # Canonical output: add a standardised column label for the requirement
    if deficit_col and deficit_col in view.columns:
        view = view.rename(columns={deficit_col: "deficit_or_offsite_units"})
    else:
        # if missing, keep as-is
        view["deficit_or_offsite_units"] = pd.NA

    # Add a category tag so we can consolidate later
    view["category"] = expected_type  # "area_habitats" | "hedgerows" | "watercourses"

    # Sort by largest magnitude requirement first
    if "deficit_or_offsite_units" in view.columns:
        ser = coerce_numeric(view["deficit_or_offsite_units"])
        view = view.assign(_mag=ser.abs())
        view = view.sort_values("_mag", ascending=False).drop(columns=["_mag"])

    return view, logs

# ------------------------------
# UI
# ------------------------------
st.title("🌿 DEFRA BNG Metric Reader")
st.caption("Upload a DEFRA BNG Metric workbook (.xlsx). The app will find Trading Summary sheets and list any off-site requirements / deficits.")

with st.sidebar:
    st.header("Upload")
    file = st.file_uploader("Metric workbook (.xlsx)", type=["xlsx"])
    st.markdown("---")
    st.markdown("Tips:")
    st.markdown("- Make sure you export the *Trading Summary* tabs in your BNG Metric.")
    st.markdown("- This reader searches for negative balances or explicit *off-site requirement* columns.")
    st.markdown("- If your template uses bespoke column names, we can add a custom mapping.")

if not file:
    st.info("Upload a DEFRA BNG metric .xlsx to begin.")
    st.stop()

# Load workbook
try:
    xls = pd.ExcelFile(file)
except Exception as e:
    st.error(f"Could not open workbook: {e}")
    st.stop()

st.success("Workbook loaded.")

# Parse each category
tabs = st.tabs(["Area Habitats", "Hedgerows", "Watercourses", "Consolidated Off-site Requirements", "Diagnostics"])

area_df, area_logs = parse_trading_summary(
    xls,
    target_sheet_candidates=[
        "Trading Summary Area Habitats",
        "Area Habitats Trading Summary",
        "Area Trading Summary",
        "Trading Summary (Area Habitats)"
    ],
    expected_type="area_habitats"
)

hedge_df, hedge_logs = parse_trading_summary(
    xls,
    target_sheet_candidates=[
        "Trading Summary Hedgerows",
        "Hedgerows Trading Summary",
        "Hedgerow Trading Summary",
        "Trading Summary (Hedgerows)"
    ],
    expected_type="hedgerows"
)

water_df, water_logs = parse_trading_summary(
    xls,
    target_sheet_candidates=[
        "Trading Summary WaterCs",
        "Trading Summary Watercourses",
        "Watercourses Trading Summary",
        "Trading Summary (Watercourses)"
    ],
    expected_type="watercourses"
)

# --- Area tab
with tabs[0]:
    st.subheader("Trading Summary — Area Habitats")
    if area_df is not None and not area_df.empty:
        st.dataframe(area_df, use_container_width=True, height=420)
        csv = area_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Area Habitats (CSV)", csv, "area_habitats_offsite.csv", "text/csv")
    else:
        st.warning("No Area Habitat trading summary (or no deficits) detected.")
    if area_logs:
        with st.expander("Logs / Notes"):
            for line in area_logs:
                st.write("•", line)

# --- Hedgerows tab
with tabs[1]:
    st.subheader("Trading Summary — Hedgerows")
    if hedge_df is not None and not hedge_df.empty:
        st.dataframe(hedge_df, use_container_width=True, height=420)
        csv = hedge_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Hedgerows (CSV)", csv, "hedgerows_offsite.csv", "text/csv")
    else:
        st.warning("No Hedgerows trading summary (or no deficits) detected.")
    if hedge_logs:
        with st.expander("Logs / Notes"):
            for line in hedge_logs:
                st.write("•", line)

# --- Watercourses tab
with tabs[2]:
    st.subheader("Trading Summary — Watercourses")
    if water_df is not None and not water_df.empty:
        st.dataframe(water_df, use_container_width=True, height=420)
        csv = water_df.to_csv(index=False).encode("utf-8")
        st.download_button("Download Watercourses (CSV)", csv, "watercourses_offsite.csv", "text/csv")
    else:
        st.warning("No Watercourses trading summary (or no deficits) detected.")
    if water_logs:
        with st.expander("Logs / Notes"):
            for line in water_logs:
                st.write("•", line)

# --- Consolidated Off-site Requirements
with tabs[3]:
    st.subheader("Consolidated Off-site Requirements")
    parts = [x for x in [area_df, hedge_df, water_df] if isinstance(x, pd.DataFrame) and not x.empty]
    if parts:
        consolidated = pd.concat(parts, ignore_index=True)
        st.dataframe(consolidated, use_container_width=True, height=500)
        csv = consolidated.to_csv(index=False).encode("utf-8")
        st.download_button("Download Consolidated (CSV)", csv, "offsite_requirements_consolidated.csv", "text/csv")
    else:
        st.info("No off-site requirements identified across the three summaries.")

# --- Diagnostics
with tabs[4]:
    st.subheader("Diagnostics")
    st.write("### Sheets found")
    st.write(xls.sheet_names)

    st.write("### Raw preview (first 10 rows) of candidate sheets")
    for label, cands in [
        ("Area Habitats", ["Trading Summary Area Habitats", "Area Habitats Trading Summary", "Area Trading Summary", "Trading Summary (Area Habitats)"]),
        ("Hedgerows", ["Trading Summary Hedgerows", "Hedgerows Trading Summary", "Hedgerow Trading Summary", "Trading Summary (Hedgerows)"]),
        ("Watercourses", ["Trading Summary WaterCs", "Trading Summary Watercourses", "Watercourses Trading Summary", "Trading Summary (Watercourses)"]),
    ]:
        sheet = find_sheet_name(xls, cands)
        if sheet:
            try:
                raw = pd.read_excel(xls, sheet_name=sheet, header=None)
                st.write(f"**{label}:** `{sheet}`")
                st.dataframe(raw.head(10), use_container_width=True)
            except Exception as e:
                st.write(f"*Could not preview `{sheet}`:* {e}")
        else:
            st.write(f"*No sheet matched for {label}*")
