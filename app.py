# app.py — DEFRA BNG Metric Reader (robust band mapping from raw sheet)
# ---------------------------------------------------------------------
# What’s inside
# - Upload DEFRA BNG Metric (.xlsx)
# - Parse Trading Summaries for Area Habitats / Hedgerows / Watercourses
# - BROAD GROUP: taken from the column immediately to the right of Habitat (with safeguards)
# - DISTINCTIVENESS: mapped from the RAW sheet (no headers) by scanning section headers
#     • Detects 'Very High Distinctiveness' even when it’s in B11 (merged/subheading)
#     • Tracks active band across the sheet and assigns to exact habitat rows by name match
#     • Prioritises "Very High" before "High" to avoid false matches
# - Area Habitats trading rules:
#     • Very High: same habitat only
#     • High: same habitat only
#     • Medium: SAME Broad Group; distinctiveness ≥ Medium
#     • Low: ≥ Low; remaining Low surplus applied to Headline Area Unit Deficit
# - “Still needs mitigation OFF-SITE”: habitat residuals + Net Gain remainder
#     (NG remainder = Headline deficit after Low − sum(habitat residuals))

import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="DEFRA BNG Metric Reader", page_icon="🌿", layout="wide")

# ------------------------------
# Utilities
# ------------------------------
def clean_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def canon(s: str) -> str:
    s = clean_text(s).lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s

def coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def find_sheet(xls: pd.ExcelFile, targets: List[str]) -> Optional[str]:
    existing = {canon(s): s for s in xls.sheet_names}
    for t in targets:
        if canon(t) in existing:
            return existing[canon(t)]
    for s in xls.sheet_names:
        if any(canon(t) in canon(s) for t in targets):
            return s
    return None

def find_header_row(df: pd.DataFrame, within_rows: int = 80) -> Optional[int]:
    """Heuristic: header has 'Group' and on/off/project wording."""
    for i in range(min(within_rows, len(df))):
        row = " ".join([clean_text(x) for x in df.iloc[i].tolist()]).lower()
        if ("group" in row) and (("on-site" in row and "off-site" in row and "project" in row)
                                 or "project wide" in row or "project-wide" in row):
            return i
    return None

# ------------------------------
# Loaders
# ------------------------------
def load_raw_sheet(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    """Load sheet as raw (no headers)."""
    return pd.read_excel(xls, sheet_name=sheet, header=None)

def load_trading_df(xls: pd.ExcelFile, sheet: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Return (parsed table df, raw df)."""
    raw = load_raw_sheet(xls, sheet)
    hdr = find_header_row(raw)
    if hdr is None:
        df = pd.read_excel(xls, sheet_name=sheet)  # fallback
    else:
        headers = raw.iloc[hdr].map(clean_text).tolist()
        df = raw.iloc[hdr + 1:].copy()
        df.columns = headers
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.dropna(how="all").reset_index(drop=True)
    return df, raw

def col_like(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cols = {canon(c): c for c in df.columns}
    for c in cands:
        if canon(c) in cols:
            return cols[canon(c)]
    for k, v in cols.items():
        if any(canon(c) in k for c in cands):
            return v
    return None

# ------------------------------
# Broad Group resolution (right of Habitat)
# ------------------------------
def resolve_broad_group_col(df: pd.DataFrame, habitat_col: str, broad_col_guess: Optional[str]) -> Optional[str]:
    """
    Prefer the column immediately to the RIGHT of the habitat column.
    Falls back to any header that looks like Group/Broad habitat.
    Avoids numeric/unit-change columns.
    """
    try:
        h_idx = df.columns.get_loc(habitat_col)
        adj = df.columns[h_idx + 1] if h_idx + 1 < len(df.columns) else None
    except Exception:
        adj = None

    def looks_like_group(col: Optional[str]) -> bool:
        if col is None or col not in df.columns:
            return False
        name = canon(col)
        if any(k in name for k in ["group", "broad_habitat"]):
            return True
        ser = df[col].dropna()
        if ser.empty:
            return False
        numeric_ratio = pd.to_numeric(ser, errors="coerce").notna().mean()
        return numeric_ratio < 0.2

    if adj and looks_like_group(adj) and "unit_change" not in canon(adj):
        return adj
    if broad_col_guess and looks_like_group(broad_col_guess):
        return broad_col_guess
    if adj and "unit_change" not in canon(adj):
        return adj
    return broad_col_guess

# ------------------------------
# Distinctiveness from RAW (robust)
# ------------------------------
# Regex patterns (prioritise VERY HIGH before HIGH)
VH_PAT = re.compile(r"\bvery\s*high\b.*distinct", re.I)
H_PAT  = re.compile(r"\bhigh\b.*distinct", re.I)
M_PAT  = re.compile(r"\bmedium\b.*distinct", re.I)
L_PAT  = re.compile(r"\blow\b.*distinct", re.I)

def build_band_map_from_raw(raw: pd.DataFrame, habitats: List[str]) -> Dict[str, str]:
    """
    Scan the raw sheet (no headers) to follow section headers like:
      "Very High Distinctiveness", "High Distinctiveness", etc.
    Then map the ACTIVE band to rows whose cells match any of the target habitat names.

    Returns: { habitat_name_exact: band }
    """
    # Pre-clean habitats for faster matching
    target_set = {clean_text(h) for h in habitats if isinstance(h, str) and clean_text(h)}
    band_map: Dict[str, str] = {}

    active_band: Optional[str] = None
    max_scan_cols = min(8, raw.shape[1])  # scan first 8 cols to catch headings in early columns

    for r in range(len(raw)):
        # Build a line of text from the first 8 columns to detect section headers
        texts = []
        for c in range(max_scan_cols):
            val = raw.iat[r, c] if c < raw.shape[1] else None
            if isinstance(val, str) or (isinstance(val, float) and not pd.isna(val)):
                texts.append(clean_text(val))
        joined = " ".join([t for t in texts if t]).strip()

        if joined:
            # Prioritise Very High before High
            if VH_PAT.search(joined):
                active_band = "Very High"
            elif H_PAT.search(joined) and not VH_PAT.search(joined):
                active_band = "High"
            elif M_PAT.search(joined):
                active_band = "Medium"
            elif L_PAT.search(joined):
                active_band = "Low"

        # If we have an active band, try to assign it to any habitat name that appears in the row
        if active_band:
            # Check all cells in the row to see if they exactly equal a target habitat name
            for c in range(raw.shape[1]):
                val = raw.iat[r, c]
                if isinstance(val, str):
                    v = clean_text(val)
                    if v in target_set and v not in band_map:
                        band_map[v] = active_band

    return band_map

# ------------------------------
# Normalised requirements (generic)
# ------------------------------
def normalise_requirements(
    xls: pd.ExcelFile,
    sheet_candidates: List[str],
    category_label: str
) -> Tuple[pd.DataFrame, Dict[str, str], str]:
    sheet = find_sheet(xls, sheet_candidates) or ""
    if not sheet:
        return pd.DataFrame(columns=[
            "category", "habitat", "broad_group", "distinctiveness",
            "project_wide_change", "on_site_change"
        ]), {}, sheet

    df, raw = load_trading_df(xls, sheet)

    # Key columns
    habitat_col = col_like(df, "Habitat", "Feature")
    broad_col_guess = col_like(df, "Habitat group", "Broad habitat", "Group")
    proj_col = col_like(df, "Project-wide unit change", "Project wide unit change")
    ons_col  = col_like(df, "On-site unit change", "On site unit change")

    if not habitat_col or not proj_col:
        return pd.DataFrame(columns=[
            "category", "habitat", "broad_group", "distinctiveness",
            "project_wide_change", "on_site_change"
        ]), {}, sheet

    # Resolve Broad Group from the cell RIGHT of Habitat (with safeguards)
    broad_col = resolve_broad_group_col(df, habitat_col, broad_col_guess)

    # Keep only habitat rows
    df = df[~df[habitat_col].isna()]
    df = df[df[habitat_col].astype(str).str.strip() != ""].copy()

    # Numerics
    for c in [proj_col, ons_col]:
        if c in df.columns:
            df[c] = coerce_num(df[c])

    # DISTINCTIVENESS: build a band map from RAW and map back to parsed df habitats
    habitat_list = df[habitat_col].astype(str).map(clean_text).tolist()
    band_map = build_band_map_from_raw(raw, habitat_list)
    df["__distinctiveness__"] = df[habitat_col].astype(str).map(lambda x: band_map.get(clean_text(x), pd.NA))

    out = pd.DataFrame({
        "category": category_label,
        "habitat": df[habitat_col],
        "broad_group": df[broad_col] if (broad_col in df.columns) else pd.NA,
        "distinctiveness": df["__distinctiveness__"],
        "project_wide_change": df[proj_col],
        "on_site_change": df[ons_col] if ons_col in df.columns else pd.NA,
    })

    colmap = {
        "habitat": habitat_col,
        "broad_group": broad_col or "",
        "project_wide_change": proj_col,
        "on_site_change": ons_col or "",
        "distinctiveness_from_raw": "__distinctiveness__",
    }

    out = out.dropna(subset=["habitat"])
    return out.reset_index(drop=True), colmap, sheet

# ------------------------------
# Area Habitats trading rules + allocation
# ------------------------------
def can_offset_area(d_band: str, d_broad: str, d_hab: str,
                    s_band: str, s_broad: str, s_hab: str) -> bool:
    rank = {"Low": 1, "Medium": 2, "High": 3, "Very High": 4}
    rd = rank.get(str(d_band), 0)
    rs = rank.get(str(s_band), 0)

    d_broad = clean_text(d_broad)
    s_broad = clean_text(s_broad)
    d_hab = clean_text(d_hab)
    s_hab = clean_text(s_hab)

    if d_band == "Very High":
        return d_hab == s_hab  # exact habitat only
    if d_band == "High":
        return d_hab == s_hab  # exact habitat only
    if d_band == "Medium":
        # SAME BROAD GROUP and distinctiveness ≥ Medium
        return (d_broad != "" and d_broad == s_broad) and (rs >= rd)
    if d_band == "Low":
        # same or better distinctiveness (>=)
        return rs >= rd
    return False

def apply_area_offsets(area_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    data = area_df.copy()
    data["project_wide_change"] = coerce_num(data["project_wide_change"])
    deficits = data[data["project_wide_change"] < 0].copy()
    surpluses = data[data["project_wide_change"] > 0].copy()

    # Eligibility matrix
    elig_rows = []
    for _, d in deficits.iterrows():
        for _, s in surpluses.iterrows():
            if can_offset_area(str(d["distinctiveness"]), d.get("broad_group", ""),
                               d.get("habitat", ""), str(s["distinctiveness"]),
                               s.get("broad_group", ""), s.get("habitat", "")):
                elig_rows.append({
                    "deficit_habitat": clean_text(d.get("habitat","")),
                    "deficit_broad": clean_text(d.get("broad_group","")),
                    "deficit_band": d["distinctiveness"],
                    "deficit_units": abs(float(d["project_wide_change"])),
                    "surplus_habitat": clean_text(s.get("habitat","")),
                    "surplus_broad": clean_text(s.get("broad_group","")),
                    "surplus_band": s["distinctiveness"],
                    "surplus_units": float(s["project_wide_change"]),
                })
    elig_df = pd.DataFrame(elig_rows)

    # Greedy allocation
    band_rank = {"Low": 1, "Medium": 2, "High": 3, "Very High": 4}
    sur = surpluses.copy()
    sur["__remain__"] = sur["project_wide_change"].astype(float)

    remaining_records = []
    for _, d in deficits.iterrows():
        need = abs(float(d["project_wide_change"]))
        d_band = str(d["distinctiveness"])
        d_broad = d.get("broad_group", "")
        d_hab = d.get("habitat", "")

        elig_idx = [si for si, s in sur.iterrows()
                    if can_offset_area(d_band, d_broad, d_hab, str(s["distinctiveness"]),
                                       s.get("broad_group",""), s.get("habitat",""))
                    and sur.loc[si, "__remain__"] > 0]
        elig_idx = sorted(
            elig_idx,
            key=lambda sidx: (-band_rank.get(str(sur.loc[sidx, "distinctiveness"]), 0), -sur.loc[sidx, "__remain__"])
        )

        for sidx in elig_idx:
            use = min(need, sur.loc[sidx, "__remain__"])
            if use > 0:
                sur.loc[sidx, "__remain__"] -= use
                need -= use
            if need <= 1e-9:
                break

        if need > 1e-9:
            remaining_records.append({
                "habitat": clean_text(d_hab),
                "broad_group": clean_text(d_broad),
                "distinctiveness": d_band,
                "unmet_units_after_on_site_offset": round(need, 4)
            })

    # Surplus remaining by band
    surplus_remaining_by_band = sur.groupby("distinctiveness", dropna=False)["__remain__"].sum().reset_index()
    surplus_remaining_by_band = surplus_remaining_by_band.rename(columns={"distinctiveness": "band",
                                                                          "__remain__": "surplus_remaining_units"})

    return {
        "deficits": deficits.sort_values("project_wide_change"),
        "surpluses": surpluses.sort_values("project_wide_change", ascending=False),
        "eligibility": elig_df,
        "surplus_remaining_by_band": surplus_remaining_by_band,
        "residual_off_site": pd.DataFrame(remaining_records).sort_values(
            ["distinctiveness", "unmet_units_after_on_site_offset"],
            ascending=[False, False]
        ).reset_index(drop=True)
    }

# ------------------------------
# Headline Results parsing (for Area Habitat units)
# ------------------------------
def parse_headline_area_deficit(xls: pd.ExcelFile) -> Optional[float]:
    candidates = ["Headline Results", "Headline results", "Headline", "Results"]
    sheet = find_sheet(xls, candidates)
    if not sheet:
        return None
    hr = pd.read_excel(xls, sheet_name=sheet, header=None)

    # Try to find a header row with "Unit Deficit"
    header_row = None
    for i in range(min(80, len(hr))):
        row = " ".join([clean_text(x) for x in hr.iloc[i].tolist()]).lower()
        if "unit deficit" in row:
            header_row = i
            break
    if header_row is not None:
        hr2 = hr.iloc[header_row:].copy()
        hr2.columns = [clean_text(x) for x in hr2.iloc[0].tolist()]
        hr2 = hr2.iloc[1:]
        if "Unit Deficit" in hr2.columns:
            row_mask = hr2.apply(lambda r: "area habitat units" in " ".join([clean_text(v).lower() for v in r.tolist()]),
                                 axis=1)
            if row_mask.any():
                val = pd.to_numeric(hr2.loc[row_mask, "Unit Deficit"], errors="coerce")
                return float(val.dropna().iloc[0]) if not val.dropna().empty else None

    # Fallback: scan for "Area habitat units" row and take last numeric
    for i in range(len(hr)):
        row_vals = [clean_text(x) for x in hr.iloc[i].tolist()]
        if any("area habitat units" in v.lower() for v in row_vals):
            nums = pd.to_numeric(pd.Series(row_vals), errors="coerce").dropna()
            if not nums.empty:
                return float(nums.iloc[-1])
    return None

# ------------------------------
# UI
# ------------------------------
st.title("🌿 DEFRA BNG Metric Reader")
st.caption("Upload a DEFRA BNG Metric workbook (.xlsx). Extract normalised requirements. "
           "For Area Habitats, apply distinctiveness trading rules and use remaining Low surplus to reduce the Headline Area Unit Deficit.")

with st.sidebar:
    file = st.file_uploader("Upload DEFRA BNG Metric (.xlsx)", type=["xlsx"])
    st.markdown("---")
    st.markdown("**Area rules:**\n"
                "- Very High: same habitat only\n"
                "- High: same habitat only\n"
                "- Medium: same **broad group**; distinctiveness ≥ Medium\n"
                "- Low: same or better (≥); remaining Low applied to Headline Area Unit Deficit")

if not file:
    st.info("Upload a Metric workbook to begin.")
    st.stop()

try:
    xls = pd.ExcelFile(file)
except Exception as e:
    st.error(f"Could not open workbook: {e}")
    st.stop()

st.success("Workbook loaded.")
st.write("**Sheets detected:**", xls.sheet_names)

# Parse three categories
AREA_SHEETS = [
    "Trading Summary Area Habitats",
    "Area Habitats Trading Summary",
    "Area Trading Summary",
    "Trading Summary (Area Habitats)"
]
HEDGE_SHEETS = [
    "Trading Summary Hedgerows",
    "Hedgerows Trading Summary",
    "Hedgerow Trading Summary",
    "Trading Summary (Hedgerows)"
]
WATER_SHEETS = [
    "Trading Summary WaterCs",
    "Trading Summary Watercourses",
    "Watercourses Trading Summary",
    "Trading Summary (Watercourses)"
]

area_norm, area_map, area_sheet = normalise_requirements(xls, AREA_SHEETS, "Area Habitats")
hedge_norm, hedge_map, hedge_sheet = normalise_requirements(xls, HEDGE_SHEETS, "Hedgerows")
water_norm, water_map, water_sheet = normalise_requirements(xls, WATER_SHEETS, "Watercourses")

tabs = st.tabs(["Area Habitats", "Hedgerows", "Watercourses", "Exports"])

# ---- Area Habitats tab (with trading rules)
with tabs[0]:
    st.subheader("Trading Summary — Area Habitats")
    if area_norm.empty:
        st.warning("No Area Habitats trading summary detected.")
    else:
        st.caption(f"Source sheet: `{area_sheet or 'not found'}`")
        st.dataframe(area_norm, use_container_width=True, height=420)

        # Apply trading rules + allocations
        alloc = apply_area_offsets(area_norm)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Deficits (project-wide change < 0)**")
            if alloc["deficits"].empty:
                st.info("No deficits.")
            else:
                st.dataframe(alloc["deficits"][["habitat", "broad_group", "distinctiveness", "project_wide_change"]],
                             use_container_width=True, height=260)

            st.markdown("**Surpluses (project-wide change > 0)**")
            if alloc["surpluses"].empty:
                st.info("No surpluses.")
            else:
                st.dataframe(alloc["surpluses"][["habitat", "broad_group", "distinctiveness", "project_wide_change"]],
                             use_container_width=True, height=260)

        with col2:
            st.markdown("**Eligibility matrix (your rules)**")
            if alloc["eligibility"].empty:
                st.info("No eligible offsets.")
            else:
                st.dataframe(alloc["eligibility"], use_container_width=True, height=300)

            st.markdown("**Surplus remaining by band (after on-site offsets)**")
            st.dataframe(alloc["surplus_remaining_by_band"], use_container_width=True, height=160)

        # Apply remaining Low surplus to Headline Area Unit Deficit
        headline_def = parse_headline_area_deficit(xls)
        low_remaining = float(
            alloc["surplus_remaining_by_band"].loc[
                alloc["surplus_remaining_by_band"]["band"] == "Low", "surplus_remaining_units"
            ].sum() if not alloc["surplus_remaining_by_band"].empty else 0.0
        )
        applied_low_to_headline = min(headline_def, low_remaining) if headline_def is not None else None
        residual_headline_after_low = (headline_def - applied_low_to_headline) if headline_def is not None else None

        # Sum of habitat-level residuals (still off-site)
        residual_table = alloc["residual_off_site"].copy()
        sum_habitat_residuals = float(residual_table["unmet_units_after_on_site_offset"].sum()) if not residual_table.empty else 0.0

        # Remaining Net Gain to quote:
        # = (Headline Area Unit Deficit after Low) − (sum of habitat residuals)
        remaining_ng_to_quote = None
        if residual_headline_after_low is not None:
            remaining_ng_to_quote = max(residual_headline_after_low - sum_habitat_residuals, 0.0)

        st.markdown("**Headline Results — Low → Headline & remainder calculation**")
        st.write(pd.DataFrame([{
            "headline_area_unit_deficit": headline_def,
            "low_band_surplus_applied_to_headline": None if applied_low_to_headline is None else round(applied_low_to_headline, 4),
            "residual_headline_after_low": None if residual_headline_after_low is None else round(residual_headline_after_low, 4),
            "sum_habitat_residuals": round(sum_habitat_residuals, 4),
            "remaining_net_gain_to_quote": None if remaining_ng_to_quote is None else round(remaining_ng_to_quote, 4),
        }]))

        # Combined residuals table = habitat residuals + ONLY the NG remainder row (if > 0)
        combined_residual = residual_table.copy()
        if remaining_ng_to_quote is not None and remaining_ng_to_quote > 1e-9:
            headline_row = pd.DataFrame([{
                "habitat": "Net gain uplift (Area, residual after habitat-specific)",
                "broad_group": "—",
                "distinctiveness": "Net Gain",
                "unmet_units_after_on_site_offset": round(remaining_ng_to_quote, 4)
            }])
            combined_residual = pd.concat([combined_residual, headline_row], ignore_index=True)

        st.markdown("**Still needs mitigation OFF-SITE (after offsets + Low→Headline)**")
        if combined_residual.empty:
            st.success("No unmet units after on-site offsets and Low→Headline application.")
        else:
            st.dataframe(combined_residual, use_container_width=True, height=260)

        # Store for Exports tab
        st.session_state["combined_residual_area"] = combined_residual

        # Download for Area residuals
        st.download_button(
            "Download residual off-site (Area incl. NG remainder) — CSV",
            combined_residual.to_csv(index=False).encode("utf-8"),
            "area_residual_offsite_incl_ng_remainder.csv", "text/csv"
        )

# ---- Hedgerows (normalised only)
with tabs[1]:
    st.subheader("Trading Summary — Hedgerows (normalised)")
    st.caption(f"Source sheet: `{hedge_sheet or 'not found'}`")
    if hedge_norm.empty:
        st.info("No Hedgerows trading summary detected.")
    else:
        st.dataframe(hedge_norm, use_container_width=True, height=480)

# ---- Watercourses (normalised only)
with tabs[2]:
    st.subheader("Trading Summary — Watercourses (normalised)")
    st.caption(f"Source sheet: `{water_sheet or 'not found'}`")
    if water_norm.empty:
        st.info("No Watercourses trading summary detected.")
    else:
        st.dataframe(water_norm, use_container_width=True, height=480)

# ---- Exports (normalised requirements + consolidated)
with tabs[3]:
    st.subheader("Exports")

    # Normalised "requirements" across all three
    norm_concat = pd.concat(
        [df for df in [area_norm, hedge_norm, water_norm] if not df.empty],
        ignore_index=True
    ) if (not area_norm.empty or not hedge_norm.empty or not water_norm.empty) else pd.DataFrame(
        columns=["category", "habitat", "broad_group", "distinctiveness", "project_wide_change", "on_site_change"]
    )

    if norm_concat.empty:
        st.info("No normalised rows to export.")
    else:
        st.dataframe(norm_concat, use_container_width=True, height=420)

        # Requirements export (negative project_wide_change only)
        req_export = norm_concat.copy()
        req_export["required_offsite_units"] = req_export["project_wide_change"].apply(
            lambda x: abs(x) if pd.notna(x) and x < 0 else 0
        )
        req_export = req_export[req_export["required_offsite_units"] > 0].reset_index(drop=True)

        st.download_button("Download normalised requirements — CSV",
                           req_export.to_csv(index=False).encode("utf-8"),
                           "requirements_export.csv", "text/csv")
        st.download_button("Download normalised requirements — JSON",
                           req_export.to_json(orient="records", indent=2).encode("utf-8"),
                           "requirements_export.json", "application/json")

        # Residual-to-mitigate (Area) INCLUDING the NG remainder row
        combined_residual_area = st.session_state.get("combined_residual_area", pd.DataFrame())
        if not combined_residual_area.empty:
            st.download_button("Download residual to mitigate (Area incl. NG remainder) — CSV",
                               combined_residual_area.to_csv(index=False).encode("utf-8"),
                               "area_residual_to_mitigate_incl_ng_remainder.csv", "text/csv")
            st.download_button("Download residual to mitigate (Area incl. NG remainder) — JSON",
                               combined_residual_area.to_json(orient="records", indent=2).encode("utf-8"),
                               "area_residual_to_mitigate_incl_ng_remainder.json", "application/json")

st.caption("Hidden sheets are fine — we read by name. If your template wording shifts, we can widen the header/band patterns.")



