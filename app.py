# app.py — DEFRA BNG Metric Reader (Styled + Surplus Flag)
# - Supports .xlsx / .xlsm / .xlsb (no macros run)
# - Robust Headline Results parser (Unit Type table or derive from baseline/post)
# - Distinctiveness from raw (captures "Very High/High/Medium/Low Distinctiveness" headers)
# - Broad Group from the cell to the right of Habitat
# - Area trading rules + Medium-in-group + Low→Headline + Net Gain remainder
# - HERO card: "Still needs mitigation OFF-SITE (after offsets + Low→Headline)" front-and-center
# - NEW: Surplus overflow flag when Medium/High/VH cascades + Low→Headline leave overall surplus

import io
import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="DEFRA BNG Metric Reader", page_icon="🌿", layout="wide")

# -----------------------------------
# Global CSS
# -----------------------------------
st.markdown(
    """
    <style>
      .stApp { background: radial-gradient(1200px 600px at 0% -10%, rgba(120,200,160,.08), transparent),
                           radial-gradient(1200px 600px at 100% 110%, rgba(120,160,220,.08), transparent); }
      .block-container { padding-top: 2rem; padding-bottom: 2.5rem; }
      .hero-card {
        border-radius: 20px; padding: 1.2rem 1.2rem 1rem; margin: .2rem 0 1rem;
        background: var(--hero-bg, rgba(250,250,250,0.65)); backdrop-filter: blur(8px);
        border: 1px solid rgba(120,120,120,0.12); box-shadow: 0 6px 22px rgba(0,0,0,.08);
      }
      @media (prefers-color-scheme: dark) {
        .hero-card { --hero-bg: rgba(22,22,22,0.55); border-color: rgba(255,255,255,0.08); }
      }
      .hero-title { font-weight: 700; font-size: 1.15rem; margin: 0 0 .25rem 0; display: flex; align-items: center; gap: .5rem; }
      .hero-sub { opacity: .75; font-size: .92rem; margin-top: 0; }
      .kpi { display: grid; gap: .3rem; padding: .8rem 1rem; border-radius: 14px; border: 1px solid rgba(120,120,120,0.12); background: rgba(180,180,180,0.06); }
      .kpi .label { opacity: .75; font-size: .8rem; } .kpi .value { font-weight: 700; font-size: 1.2rem; }
      .exp-label { font-weight: 700; font-size: .98rem; }
      div[data-testid="stDataFrame"] { border-radius: 14px; overflow: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# -----------------------------------
# Workbook openers (xlsx / xlsm / xlsb) — macros NOT executed
# -----------------------------------
def open_metric_workbook(uploaded_file) -> pd.ExcelFile:
    data = uploaded_file.read() if hasattr(uploaded_file, "read") else uploaded_file
    name = getattr(uploaded_file, "name", "") or ""
    ext = os.path.splitext(name)[1].lower()

    if ext in [".xlsx", ".xlsm", ""]:
        try:
            return pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
        except Exception:
            pass
    if ext == ".xlsb":
        try:
            return pd.ExcelFile(io.BytesIO(data), engine="pyxlsb")
        except Exception:
            pass

    for eng in ("openpyxl", "pyxlsb"):
        try:
            return pd.ExcelFile(io.BytesIO(data), engine=eng)
        except Exception:
            continue

    raise RuntimeError("Could not open workbook. Try re-saving as .xlsx or .xlsm.")

# -----------------------------------
# Utilities
# -----------------------------------
def clean_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    s = str(x).strip()
    return re.sub(r"\s+", " ", s)

def canon(s: str) -> str:
    s = clean_text(s).lower().replace("–", "-").replace("—", "-")
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")

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
    for i in range(min(within_rows, len(df))):
        row = " ".join([clean_text(x) for x in df.iloc[i].tolist()]).lower()
        if ("group" in row) and (("on-site" in row and "off-site" in row and "project" in row)
                                 or "project wide" in row or "project-wide" in row):
            return i
    return None

def col_like(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cols = {canon(c): c for c in df.columns}
    for c in cands:
        if canon(c) in cols: return cols[canon(c)]
    for k, v in cols.items():
        if any(canon(c) in k for c in cands): return v
    return None

# -----------------------------------
# Trading Summary loaders
# -----------------------------------
def load_raw_sheet(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    return pd.read_excel(xls, sheet_name=sheet, header=None)

def load_trading_df(xls: pd.ExcelFile, sheet: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    raw = load_raw_sheet(xls, sheet)
    hdr = find_header_row(raw)
    if hdr is None:
        df = pd.read_excel(xls, sheet_name=sheet)  # fallback
    else:
        headers = raw.iloc[hdr].map(clean_text).tolist()
        df = raw.iloc[hdr + 1:].copy(); df.columns = headers
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.dropna(how="all").reset_index(drop=True)
    return df, raw

# -----------------------------------
# Broad Group (right of Habitat)
# -----------------------------------
def resolve_broad_group_col(df: pd.DataFrame, habitat_col: str, broad_col_guess: Optional[str]) -> Optional[str]:
    try:
        h_idx = df.columns.get_loc(habitat_col)
        adj = df.columns[h_idx + 1] if h_idx + 1 < len(df.columns) else None
    except Exception:
        adj = None

    def looks_like_group(col: Optional[str]) -> bool:
        if not col or col not in df.columns: return False
        name = canon(col)
        if any(k in name for k in ["group", "broad_habitat"]): return True
        ser = df[col].dropna()
        if ser.empty: return False
        return pd.to_numeric(ser, errors="coerce").notna().mean() < 0.2

    if adj and looks_like_group(adj) and "unit_change" not in canon(adj): return adj
    if broad_col_guess and looks_like_group(broad_col_guess): return broad_col_guess
    if adj and "unit_change" not in canon(adj): return adj
    return broad_col_guess

# -----------------------------------
# Distinctiveness tagging from RAW
# -----------------------------------
VH_PAT = re.compile(r"\bvery\s*high\b.*distinct", re.I)
H_PAT  = re.compile(r"\bhigh\b.*distinct", re.I)
M_PAT  = re.compile(r"\bmedium\b.*distinct", re.I)
L_PAT  = re.compile(r"\blow\b.*distinct", re.I)

def build_band_map_from_raw(raw: pd.DataFrame, habitats: List[str]) -> Dict[str, str]:
    target_set = {clean_text(h) for h in habitats if isinstance(h, str) and clean_text(h)}
    band_map: Dict[str, str] = {}
    active_band: Optional[str] = None
    max_scan_cols = min(8, raw.shape[1])

    for r in range(len(raw)):
        texts = []
        for c in range(max_scan_cols):
            val = raw.iat[r, c] if c < raw.shape[1] else None
            if isinstance(val, str) or (isinstance(val, float) and not pd.isna(val)):
                texts.append(clean_text(val))
        joined = " ".join([t for t in texts if t]).strip()

        if joined:
            if VH_PAT.search(joined): active_band = "Very High"
            elif H_PAT.search(joined) and not VH_PAT.search(joined): active_band = "High"
            elif M_PAT.search(joined): active_band = "Medium"
            elif L_PAT.search(joined): active_band = "Low"

        if active_band:
            for c in range(raw.shape[1]):
                val = raw.iat[r, c]
                if isinstance(val, str):
                    v = clean_text(val)
                    if v in target_set and v not in band_map:
                        band_map[v] = active_band
    return band_map

# -----------------------------------
# Normalise Trading Summary
# -----------------------------------
def normalise_requirements(
    xls: pd.ExcelFile,
    sheet_candidates: List[str],
    category_label: str
) -> Tuple[pd.DataFrame, Dict[str, str], str]:
    sheet = find_sheet(xls, sheet_candidates) or ""
    if not sheet:
        return pd.DataFrame(columns=[
            "category","habitat","broad_group","distinctiveness","project_wide_change","on_site_change"
        ]), {}, sheet

    df, raw = load_trading_df(xls, sheet)
    habitat_col = col_like(df, "Habitat", "Feature")
    broad_col_guess = col_like(df, "Habitat group", "Broad habitat", "Group")
    proj_col = col_like(df, "Project-wide unit change", "Project wide unit change")
    ons_col  = col_like(df, "On-site unit change", "On site unit change")
    if not habitat_col or not proj_col:
        return pd.DataFrame(columns=[
            "category","habitat","broad_group","distinctiveness","project_wide_change","on_site_change"
        ]), {}, sheet

    broad_col = resolve_broad_group_col(df, habitat_col, broad_col_guess)
    df = df[~df[habitat_col].isna()]
    df = df[df[habitat_col].astype(str).str.strip() != ""].copy()
    for c in [proj_col, ons_col]:
        if c in df.columns: df[c] = coerce_num(df[c])

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
    return out.reset_index(drop=True), colmap, sheet

# -----------------------------------
# Area trading rules + allocation
# -----------------------------------
def can_offset_area(d_band: str, d_broad: str, d_hab: str,
                    s_band: str, s_broad: str, s_hab: str) -> bool:
    rank = {"Low":1, "Medium":2, "High":3, "Very High":4}
    rd = rank.get(str(d_band), 0); rs = rank.get(str(s_band), 0)
    d_broad = clean_text(d_broad); s_broad = clean_text(s_broad)
    d_hab = clean_text(d_hab); s_hab = clean_text(s_hab)

    if d_band == "Very High": return d_hab == s_hab
    if d_band == "High":      return d_hab == s_hab
    if d_band == "Medium":    return (d_broad != "" and d_broad == s_broad) and (rs >= rd)
    if d_band == "Low":       return rs >= rd
    return False

def apply_area_offsets(area_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    data = area_df.copy()
    data["project_wide_change"] = coerce_num(data["project_wide_change"])
    deficits = data[data["project_wide_change"] < 0].copy()
    surpluses = data[data["project_wide_change"] > 0].copy()

    elig_rows = []
    for _, d in deficits.iterrows():
        for _, s in surpluses.iterrows():
            if can_offset_area(str(d["distinctiveness"]), d.get("broad_group",""), d.get("habitat",""),
                               str(s["distinctiveness"]), s.get("broad_group",""), s.get("habitat","")):
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

    band_rank = {"Low":1, "Medium":2, "High":3, "Very High":4}
    sur = surpluses.copy(); sur["__remain__"] = sur["project_wide_change"].astype(float)

    remaining_records = []
    for _, d in deficits.iterrows():
        need = abs(float(d["project_wide_change"]))
        d_band = str(d["distinctiveness"]); d_broad = d.get("broad_group",""); d_hab = d.get("habitat","")

        elig_idx = [si for si, s in sur.iterrows()
                    if can_offset_area(d_band, d_broad, d_hab,
                                       str(s["distinctiveness"]), s.get("broad_group",""), s.get("habitat",""))
                    and sur.loc[si,"__remain__"] > 0]
        elig_idx = sorted(elig_idx, key=lambda i: (-band_rank.get(str(sur.loc[i,"distinctiveness"]),0),
                                                   -sur.loc[i,"__remain__"]))
        for i in elig_idx:
            use = min(need, sur.loc[i,"__remain__"])
            if use > 0:
                sur.loc[i,"__remain__"] -= use; need -= use
            if need <= 1e-9: break

        if need > 1e-9:
            remaining_records.append({
                "habitat": clean_text(d_hab),
                "broad_group": clean_text(d_broad),
                "distinctiveness": d_band,
                "unmet_units_after_on_site_offset": round(need, 4)
            })

    surplus_remaining_by_band = sur.groupby("distinctiveness", dropna=False)["__remain__"] \
                                   .sum().reset_index() \
                                   .rename(columns={"distinctiveness":"band","__remain__":"surplus_remaining_units"})

    return {
        "deficits": deficits.sort_values("project_wide_change"),
        "surpluses": surpluses.sort_values("project_wide_change", ascending=False),
        "eligibility": elig_df,
        "surplus_remaining_by_band": surplus_remaining_by_band,
        "residual_off_site": pd.DataFrame(remaining_records).sort_values(
            ["distinctiveness","unmet_units_after_on_site_offset"], ascending=[False, False]
        ).reset_index(drop=True)
    }

# -----------------------------------
# Headline Results parser (Area habitat units → Unit Deficit)
# -----------------------------------
def parse_headline_area_deficit(xls: pd.ExcelFile) -> Optional[float]:
    """
    'Headline Results' → Unit Type table: Area habitat units → Unit Deficit (or Shortfall).
    Fallback: derive from on/off-site baseline & post-intervention totals.
    """
    SHEET_NAME = "Headline Results"

    def clean(s):
        if s is None or (isinstance(s, float) and pd.isna(s)): return ""
        return re.sub(r"\s+", " ", str(s).strip())

    def last_numeric_in_row(row) -> Optional[float]:
        ser = pd.Series(row).map(lambda x: re.sub(r"[✓▲^]", "", str(x)) if isinstance(x, str) else x)
        nums = pd.to_numeric(ser, errors="coerce").dropna()
        return float(nums.iloc[-1]) if not nums.empty else None

    try:
        raw = pd.read_excel(xls, sheet_name=SHEET_NAME, header=None)
    except Exception:
        return None

    header_idx = None
    for i in range(min(200, len(raw))):
        txt = " ".join([clean(x).lower() for x in raw.iloc[i].tolist()])
        if "unit type" in txt and (("unit deficit" in txt) or ("shortfall" in txt) or ("deficit" in txt)):
            header_idx = i; break

    if header_idx is not None:
        df = raw.iloc[header_idx:].copy()
        df.columns = [clean(x) for x in df.iloc[0].tolist()]
        df = df.iloc[1:].reset_index(drop=True)
        stop_at = None
        for r in range(len(df)):
            if " ".join([clean(v) for v in df.iloc[r].tolist()]) == "":
                stop_at = r; break
        if stop_at is not None: df = df.iloc[:stop_at].copy()

        norm = {re.sub(r"[^a-z0-9]+","_", c.lower()).strip("_"): c for c in df.columns}
        unit_col = next((norm[k] for k in ["unit_type","type","unit"] if k in norm), None)
        deficit_col = next((norm[k] for k in ["unit_deficit","units_deficit","deficit","shortfall","unit_shortfall","deficit_units"] if k in norm), None)
        if deficit_col is None:
            for col in df.columns:
                if re.search(r"(deficit|shortfall)", col, re.I): deficit_col = col; break

        def is_area_row(row) -> bool:
            if unit_col:
                val = clean(row.get(unit_col, "")).lower()
                if re.search(r"\barea\s*habitat\s*units\b", val): return True
            return re.search(r"\barea\s*habitat\s*units\b", " ".join([clean(v).lower() for v in row.tolist()])) is not None

        mask = df.apply(is_area_row, axis=1)
        if mask.any():
            row = df.loc[mask].iloc[0]
            if deficit_col:
                v = pd.to_numeric(row.get(deficit_col), errors="coerce")
                if pd.notna(v): return float(v)
            ln = last_numeric_in_row(row.tolist())
            if ln is not None: return ln

    # Fallback derive
    vals = {}
    for i in range(len(raw)):
        line = " ".join([clean(x).lower() for x in raw.iloc[i].tolist()])
        if re.search(r"\bon[-\s]?site\b.*baseline.*habitat units", line):
            vals["on_b"] = last_numeric_in_row(raw.iloc[i].tolist())
        elif re.search(r"\boff[-\s]?site\b.*baseline.*habitat units", line):
            vals["off_b"] = last_numeric_in_row(raw.iloc[i].tolist())
        elif re.search(r"\bon[-\s]?site\b.*post[-\s]?intervention.*habitat units", line):
            vals["on_p"] = last_numeric_in_row(raw.iloc[i].tolist())
        elif re.search(r"\boff[-\s]?site\b.*post[-\s]?intervention.*habitat units", line):
            vals["off_p"] = last_numeric_in_row(raw.iloc[i].tolist())

    if any(k in vals for k in ["on_b","off_b","on_p","off_p"]):
        on_b  = vals.get("on_b")  or 0.0
        off_b = vals.get("off_b") or 0.0
        on_p  = vals.get("on_p")  or 0.0
        off_p = vals.get("off_p") or 0.0
        baseline_total = on_b + off_b
        post_total     = on_p + off_p
        net_change     = post_total - baseline_total
        required_10pc  = 0.10 * baseline_total
        return float(max(required_10pc - net_change, 0.0))

    return None

# -----------------------------------
# UI
# -----------------------------------
st.title("🌿 DEFRA BNG Metric Reader")

with st.sidebar:
    file = st.file_uploader("Upload DEFRA BNG Metric (.xlsx / .xlsm / .xlsb)", type=["xlsx", "xlsm", "xlsb"])
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
    xls = open_metric_workbook(file)
except Exception as e:
    st.error(f"Could not open workbook: {e}")
    st.stop()

st.success("Workbook loaded.")
st.write("**Sheets detected:**", xls.sheet_names)

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

# -----------------------------------
# Area Habitats tab (HERO + expanders + surplus flag)
# -----------------------------------
with tabs[0]:
    st.subheader("Trading Summary — Area Habitats")
    if area_norm.empty:
        st.warning("No Area Habitats trading summary detected.")
    else:
        st.caption(f"Source sheet: `{area_sheet or 'not found'}`")

        alloc = apply_area_offsets(area_norm)
        headline_def = parse_headline_area_deficit(xls)

        low_remaining = float(
            alloc["surplus_remaining_by_band"]
            .loc[alloc["surplus_remaining_by_band"]["band"] == "Low", "surplus_remaining_units"]
            .sum() if not alloc["surplus_remaining_by_band"].empty else 0.0
        )
        applied_low_to_headline = min(headline_def, low_remaining) if headline_def is not None else None
        residual_headline_after_low = (headline_def - applied_low_to_headline) if headline_def is not None else None

        residual_table = alloc["residual_off_site"].copy()
        sum_habitat_residuals = float(residual_table["unmet_units_after_on_site_offset"].sum()) if not residual_table.empty else 0.0

        remaining_ng_to_quote = None
        if residual_headline_after_low is not None:
            remaining_ng_to_quote = max(residual_headline_after_low - sum_habitat_residuals, 0.0)

        combined_residual = residual_table.copy()
        if remaining_ng_to_quote is not None and remaining_ng_to_quote > 1e-9:
            combined_residual = pd.concat([
                combined_residual,
                pd.DataFrame([{
                    "habitat": "Net gain uplift (Area, residual after habitat-specific)",
                    "broad_group": "—",
                    "distinctiveness": "Net Gain",
                    "unmet_units_after_on_site_offset": round(remaining_ng_to_quote, 4)
                }])
            ], ignore_index=True)

        # KPIs for hero
        k_units = round(float(combined_residual["unmet_units_after_on_site_offset"].sum()) if not combined_residual.empty else 0.0, 4)
        k_rows  = len(combined_residual) if not combined_residual.empty else 0
        k_ng    = round(float(remaining_ng_to_quote or 0.0), 4)

        # --- NEW: detect cascade surplus after meeting all deficits + 10% NG ---
        surplus_by_band = alloc["surplus_remaining_by_band"].copy()
        applied_low = float(applied_low_to_headline or 0.0)
        if not surplus_by_band.empty:
            mask_low = surplus_by_band["band"] == "Low"
            if mask_low.any():
                surplus_by_band.loc[mask_low, "surplus_remaining_units"] = (
                    surplus_by_band.loc[mask_low, "surplus_remaining_units"] - applied_low
                ).clip(lower=0)
        overall_surplus_after_all = float(surplus_by_band["surplus_remaining_units"].sum()) if not surplus_by_band.empty else 0.0

        overflow_happened = (
            (combined_residual.empty or
             (len(combined_residual) == 1 and combined_residual["distinctiveness"].iloc[0] == "Net Gain" and
              combined_residual["unmet_units_after_on_site_offset"].iloc[0] <= 1e-9))
            and (remaining_ng_to_quote is not None and remaining_ng_to_quote <= 1e-9)
            and (overall_surplus_after_all > 1e-9)
        )

        # ---- HERO CARD ----
        st.markdown('<div class="hero-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="hero-title">🧮 Still needs mitigation OFF-SITE (after offsets + Low→Headline)</div>'
            '<div class="hero-sub">This is what you need to source or quote for.</div>',
            unsafe_allow_html=True
        )

        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown('<div class="kpi"><div class="label">Total units to mitigate</div>'
                        f'<div class="value">{k_units}</div></div>', unsafe_allow_html=True)
        with c2:
            st.markdown('<div class="kpi"><div class="label">Line items</div>'
                        f'<div class="value">{k_rows}</div></div>', unsafe_allow_html=True)
        with c3:
            st.markdown('<div class="kpi"><div class="label">NG remainder (10%)</div>'
                        f'<div class="value">{k_ng}</div></div>', unsafe_allow_html=True)

        # SURPLUS FLAG (if applicable)
        if overflow_happened:
            st.success("🎉 **Overall surplus after meeting all Area + 10% Net Gain**")
            st.write(pd.DataFrame([
                {"surplus_units_total": round(overall_surplus_after_all, 4)}
            ]))
            st.write(
                surplus_by_band.rename(columns={
                    "band": "distinctiveness_band",
                    "surplus_remaining_units": "surplus_units_after_allocation_and_NG"
                })
            )
        # --- One-line, always-visible total overall surplus (only when it actually exists) ---
        if overflow_happened:
            total_overall_surplus_row = pd.DataFrame([{
                "label": "Total overall surplus (after all allocations & 10% NG)",
                "units": round(overall_surplus_after_all, 4)
            }])
            st.write(total_overall_surplus_row)
            # keep for exports
            st.session_state["total_overall_surplus_area"] = float(round(overall_surplus_after_all, 4))
        else:
            st.session_state["total_overall_surplus_area"] = 0.0

        
        # Headline table
        st.dataframe(combined_residual, use_container_width=True, height=260)

        # Downloads
        cdl1, cdl2, _ = st.columns([1,1,3])
        with cdl1:
            st.download_button("⬇️ Download CSV",
                combined_residual.to_csv(index=False).encode("utf-8"),
                "area_residual_offsite_incl_ng_remainder.csv", "text/csv")
        with cdl2:
            st.download_button("⬇️ Download JSON",
                combined_residual.to_json(orient="records", indent=2).encode("utf-8"),
                "area_residual_offsite_incl_ng_remainder.json", "application/json")
        st.markdown('</div>', unsafe_allow_html=True)  # end hero-card

        # Save for exports
        st.session_state["combined_residual_area"] = combined_residual

        # ---- Expanders ----
        with st.expander("🔎 Headline calculation details", expanded=False):
            st.markdown('<span class="exp-label">Low → Headline & remainder</span>', unsafe_allow_html=True)
            st.write(pd.DataFrame([{
                "headline_area_unit_deficit": headline_def,
                "low_band_surplus_applied_to_headline": None if applied_low_to_headline is None else round(applied_low_to_headline, 4),
                "residual_headline_after_low": None if residual_headline_after_low is None else round(residual_headline_after_low, 4),
                "sum_habitat_residuals": round(sum_habitat_residuals, 4),
                "remaining_net_gain_to_quote": None if remaining_ng_to_quote is None else round(remaining_ng_to_quote, 4),
            }]))

        with st.expander("📉 Deficits (project-wide change < 0)", expanded=False):
            if alloc["deficits"].empty:
                st.info("No deficits.")
            else:
                st.dataframe(alloc["deficits"][["habitat","broad_group","distinctiveness","project_wide_change"]],
                             use_container_width=True, height=300)

        with st.expander("📈 Surpluses (project-wide change > 0)", expanded=False):
            if alloc["surpluses"].empty:
                st.info("No surpluses.")
            else:
                st.dataframe(alloc["surpluses"][["habitat","broad_group","distinctiveness","project_wide_change"]],
                             use_container_width=True, height=300)

        with st.expander("🔗 Eligibility matrix (your trading rules)", expanded=False):
            if alloc["eligibility"].empty:
                st.info("No eligible offsets.")
            else:
                st.dataframe(alloc["eligibility"], use_container_width=True, height=360)

        with st.expander("🧮 Surplus remaining by band (after on-site offsets)", expanded=False):
            st.dataframe(alloc["surplus_remaining_by_band"], use_container_width=True, height=220)

        with st.expander("📋 Normalised input table (Area Habitats)", expanded=False):
            st.dataframe(area_norm, use_container_width=True, height=420)

# -----------------------------------
# Hedgerows
# -----------------------------------
with tabs[1]:
    st.subheader("Hedgerows")
    if hedge_norm.empty:
        st.info("No Hedgerows trading summary detected.")
    else:
        with st.expander("📋 Normalised table — Hedgerows", expanded=True):
            st.caption(f"Source sheet: `{hedge_sheet or 'not found'}`")
            st.dataframe(hedge_norm, use_container_width=True, height=480)

# -----------------------------------
# Watercourses
# -----------------------------------
with tabs[2]:
    st.subheader("Watercourses")
    if water_norm.empty:
        st.info("No Watercourses trading summary detected.")
    else:
        with st.expander("📋 Normalised table — Watercourses", expanded=True):
            st.caption(f"Source sheet: `{water_sheet or 'not found'}`")
            st.dataframe(water_norm, use_container_width=True, height=480)

# -----------------------------------
# Exports
# -----------------------------------
with tabs[3]:
    st.subheader("Exports")
    norm_concat = pd.concat(
        [df for df in [area_norm, hedge_norm, water_norm] if not df.empty],
        ignore_index=True
    ) if (not area_norm.empty or not hedge_norm.empty or not water_norm.empty) else pd.DataFrame(
        columns=["category", "habitat", "broad_group", "distinctiveness", "project_wide_change", "on_site_change"]
    )

    if norm_concat.empty:
        st.info("No normalised rows to export.")
    else:
        with st.expander("📦 Normalised requirements (all categories)"):
            st.dataframe(norm_concat, use_container_width=True, height=420)

        req_export = norm_concat.copy()
        req_export["required_offsite_units"] = req_export["project_wide_change"].apply(
            lambda x: abs(x) if pd.notna(x) and x < 0 else 0
        )
        req_export = req_export[req_export["required_offsite_units"] > 0].reset_index(drop=True)

        cA, cB = st.columns(2)
        with cA:
            st.download_button("⬇️ Download normalised requirements — CSV",
                               req_export.to_csv(index=False).encode("utf-8"),
                               "requirements_export.csv", "text/csv")
        with cB:
            st.download_button("⬇️ Download normalised requirements — JSON",
                               req_export.to_json(orient="records", indent=2).encode("utf-8"),
                               "requirements_export.json", "application/json")

        combined_residual_area = st.session_state.get("combined_residual_area", pd.DataFrame())
        if not combined_residual_area.empty:
            st.markdown("---")
            st.markdown("**Residual to mitigate (Area incl. NG remainder)**")
            c1, c2 = st.columns(2)
            with c1:
                st.download_button("⬇️ Download residual (CSV)",
                                   combined_residual_area.to_csv(index=False).encode("utf-8"),
                                   "area_residual_to_mitigate_incl_ng_remainder.csv", "text/csv")
            with c2:
                st.download_button("⬇️ Download residual (JSON)",
                                   combined_residual_area.to_json(orient="records", indent=2).encode("utf-8"),
                                   "area_residual_to_mitigate_incl_ng_remainder.json", "application/json")

st.caption("Tip: The headline card is the number you quote. Everything else sits below for audit/QA.")






