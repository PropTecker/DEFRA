# app.py — DEFRA BNG Metric Reader (Flows + NG-in-Matrix + Explainer)
# - .xlsx / .xlsm / .xlsb (no macros run)
# - Robust Headline Results parser (table or derive)
# - Distinctiveness from raw section headers
# - Broad Group from the cell to the right of Habitat
# - Area trading rules + flows ledger (who mitigates whom, how much)
# - Low→Headline recorded as flows into the same matrix (so NG coverage is visible)
# - Hero card + KPIs + surplus flag + maths explainer

import io
import os
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

st.set_page_config(page_title="DEFRA BNG Metric Reader", page_icon="🌿", layout="wide")

# ---------------- CSS ----------------
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
      .explain-card{
        border-radius:16px; padding:14px 16px; margin:0 0 12px 0;
        background: var(--explain-bg, rgba(255,255,255,0.65));
        border:1px solid rgba(120,120,120,0.12);
        box-shadow: 0 3px 14px rgba(0,0,0,.06);
        backdrop-filter: blur(6px);
      }
      @media (prefers-color-scheme: dark){
        .explain-card{ --explain-bg: rgba(24,24,24,0.55); border-color: rgba(255,255,255,0.08); }
      }
      .explain-card h4{ margin:0 0 .25rem 0; font-weight:700; }
      .explain-card p{ margin:.25rem 0; }
      .explain-card ul{ margin:.4rem 0 .2rem 1.2rem; }
      .explain-kv{ opacity:.85; font-size:.92rem; }
      .explain-kv code{ font-weight:700; }
      div[data-testid="stDataFrame"] { border-radius: 14px; overflow: hidden; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ------------- open workbook -------------
def open_metric_workbook(uploaded_file) -> pd.ExcelFile:
    data = uploaded_file.read() if hasattr(uploaded_file, "read") else uploaded_file
    name = getattr(uploaded_file, "name", "") or ""
    ext = os.path.splitext(name)[1].lower()
    if ext in [".xlsx", ".xlsm", ""]:
        try: return pd.ExcelFile(io.BytesIO(data), engine="openpyxl")
        except Exception: pass
    if ext == ".xlsb":
        try: return pd.ExcelFile(io.BytesIO(data), engine="pyxlsb")
        except Exception: pass
    for eng in ("openpyxl", "pyxlsb"):
        try: return pd.ExcelFile(io.BytesIO(data), engine=eng)
        except Exception: continue
    raise RuntimeError("Could not open workbook. Try re-saving as .xlsx or .xlsm.")

# ------------- utils -------------
def clean_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    return re.sub(r"\s+", " ", str(x).strip())

def canon(s: str) -> str:
    s = clean_text(s).lower().replace("–","-").replace("—","-")
    return re.sub(r"[^a-z0-9]+","_", s).strip("_")

def coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def find_sheet(xls: pd.ExcelFile, targets: List[str]) -> Optional[str]:
    existing = {canon(s): s for s in xls.sheet_names}
    for t in targets:
        if canon(t) in existing: return existing[canon(t)]
    for s in xls.sheet_names:
        if any(canon(t) in canon(s) for t in targets): return s
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

# ------------- loaders -------------
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

# ------------- broad group from right -------------
def resolve_broad_group_col(df: pd.DataFrame, habitat_col: str, broad_col_guess: Optional[str]) -> Optional[str]:
    try:
        h_idx = df.columns.get_loc(habitat_col)
        adj = df.columns[h_idx + 1] if h_idx + 1 < len(df.columns) else None
    except Exception:
        adj = None
    def looks_like_group(col: Optional[str]) -> bool:
        if not col or col not in df.columns: return False
        name = canon(col)
        if any(k in name for k in ["group","broad_habitat"]): return True
        ser = df[col].dropna()
        if ser.empty: return False
        return pd.to_numeric(ser, errors="coerce").notna().mean() < 0.2
    if adj and looks_like_group(adj) and "unit_change" not in canon(adj): return adj
    if broad_col_guess and looks_like_group(broad_col_guess): return broad_col_guess
    if adj and "unit_change" not in canon(adj): return adj
    return broad_col_guess

# ------------- distinctiveness from raw headers -------------
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

# ------------- normalise (generic) -------------
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
        "habitat": habitat_col, "broad_group": broad_col or "",
        "project_wide_change": proj_col, "on_site_change": ons_col or "",
        "distinctiveness_from_raw": "__distinctiveness__",
    }
    return out.reset_index(drop=True), colmap, sheet

# ------------- area trading rules -------------
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
    """
    Apply rules AND record actual flows between habitats.
    Returns:
      - allocation_flows: rows of (deficit -> surplus used)
      - residual_off_site: unmet per deficit
      - surplus_remaining_by_band: aggregates after offsets
      - surplus_after_offsets_detail: per-surplus remaining units (for Low→Headline allocation)
    """
    data = area_df.copy()
    data["project_wide_change"] = coerce_num(data["project_wide_change"])
    deficits = data[data["project_wide_change"] < 0].copy()
    surpluses = data[data["project_wide_change"] > 0].copy()

    # Working copy to track remaining
    sur = surpluses.copy()
    sur["__remain__"] = sur["project_wide_change"].astype(float)

    band_rank = {"Low": 1, "Medium": 2, "High": 3, "Very High": 4}
    flow_rows = []

    for _, d in deficits.iterrows():
        need = abs(float(d["project_wide_change"]))
        d_band  = str(d["distinctiveness"])
        d_broad = clean_text(d.get("broad_group",""))
        d_hab   = clean_text(d.get("habitat",""))
        elig_idx = [si for si, s in sur.iterrows()
                    if can_offset_area(d_band, d_broad, d_hab,
                                       str(s["distinctiveness"]), clean_text(s.get("broad_group","")),
                                       clean_text(s.get("habitat","")))
                    and sur.loc[si,"__remain__"] > 0]
        elig_idx = sorted(elig_idx,
                          key=lambda i: (-band_rank.get(str(sur.loc[i,"distinctiveness"]),0),
                                         -sur.loc[i,"__remain__"]))
        for i in elig_idx:
            if need <= 1e-9: break
            give = min(need, float(sur.loc[i,"__remain__"]))
            if give <= 0: continue
            flow_rows.append({
                "deficit_habitat": d_hab,
                "deficit_broad": d_broad,
                "deficit_band": d_band,
                "surplus_habitat": clean_text(sur.loc[i,"habitat"]),
                "surplus_broad": clean_text(sur.loc[i,"broad_group"]),
                "surplus_band": str(sur.loc[i,"distinctiveness"]),
                "units_transferred": round(give, 6),
                "flow_type": "habitat→habitat"
            })
            sur.loc[i,"__remain__"] -= give
            need -= give

    # Residual unmet deficits
    remaining_records = []
    got_by_deficit = {}
    for r in flow_rows:
        key = (r["deficit_habitat"], r["deficit_broad"], r["deficit_band"])
        got_by_deficit[key] = got_by_deficit.get(key, 0.0) + r["units_transferred"]
    for _, d in deficits.iterrows():
        key = (clean_text(d.get("habitat","")), clean_text(d.get("broad_group","")), str(d["distinctiveness"]))
        original_need = abs(float(d["project_wide_change"]))
        received = got_by_deficit.get(key, 0.0)
        unmet = max(original_need - received, 0.0)
        if unmet > 1e-9:
            remaining_records.append({
                "habitat": key[0],
                "broad_group": key[1],
                "distinctiveness": key[2],
                "unmet_units_after_on_site_offset": round(unmet, 6)
            })

    surplus_remaining_by_band = sur.groupby("distinctiveness", dropna=False)["__remain__"] \
                                   .sum().reset_index() \
                                   .rename(columns={"distinctiveness":"band","__remain__":"surplus_remaining_units"})

    # detail table (needed for Low→Headline allocation)
    surplus_after_offsets_detail = sur.rename(columns={"__remain__":"surplus_remaining_units"})[
        ["habitat","broad_group","distinctiveness","surplus_remaining_units"]
    ].copy()

    return {
        "deficits": deficits.sort_values("project_wide_change"),
        "surpluses": surpluses.sort_values("project_wide_change", ascending=False),
        "allocation_flows": pd.DataFrame(flow_rows) if flow_rows else pd.DataFrame(
            columns=["deficit_habitat","deficit_broad","deficit_band",
                     "surplus_habitat","surplus_broad","surplus_band",
                     "units_transferred","flow_type"]
        ),
        "surplus_remaining_by_band": surplus_remaining_by_band,
        "surplus_after_offsets_detail": surplus_after_offsets_detail,
        "residual_off_site": pd.DataFrame(remaining_records).sort_values(
            ["distinctiveness","unmet_units_after_on_site_offset"], ascending=[False, False]
        ).reset_index(drop=True)
    }

# ------------- headline parser (Area Unit Deficit) -------------
def parse_headline_area_deficit(xls: pd.ExcelFile) -> Optional[float]:
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
    # derive fallback
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

# ------------- explainer builder -------------
def build_area_explanation(
    alloc: Dict[str, pd.DataFrame],
    headline_def: Optional[float],
    low_available: float,
    applied_low_to_headline: Optional[float],
    residual_headline_after_low: Optional[float],
    remaining_ng_to_quote: Optional[float],
    ng_flow_rows: List[dict]
) -> str:
    lines = []

    flows = alloc.get("allocation_flows", pd.DataFrame())
    if not flows.empty:
        lines.append("**On-site offsets applied (by trading rules):**")
        for (dh, db, dband), grp in flows.groupby(["deficit_habitat","deficit_broad","deficit_band"], dropna=False):
            total = grp["units_transferred"].sum()
            bullet = f"- **{dh}** ({dband}{', ' + db if db else ''}) — deficit reduced by **{total:.4f}** units via:"
            sub = []
            for _, r in grp.sort_values("units_transferred", ascending=False).iterrows():
                sub.append(f"    - {r['surplus_habitat']} ({r['surplus_band']}{', ' + r['surplus_broad'] if r['surplus_broad'] else ''}) → **{r['units_transferred']:.4f}**")
            lines.append(bullet)
            lines.extend(sub)
    else:
        lines.append("**On-site offsets applied:** none matched by trading rules.")

    residuals = alloc.get("residual_off_site", pd.DataFrame())
    if not residuals.empty:
        lines.append("\n**Habitat-specific residuals still to mitigate off-site:**")
        for _, r in residuals.iterrows():
            lines.append(f"- {r['habitat']} ({r['distinctiveness']}{', ' + str(r['broad_group']) if pd.notna(r['broad_group']) and str(r['broad_group']).strip() else ''}) → **{float(r['unmet_units_after_on_site_offset']):.4f}** units")
    else:
        lines.append("\n**Habitat-specific residuals:** none remain after on-site offsets.")

    H = 0.0 if headline_def is None else float(headline_def)
    used_low = 0.0 if applied_low_to_headline is None else float(applied_low_to_headline)
    R = 0.0 if residual_headline_after_low is None else float(residual_headline_after_low)
    NG = 0.0 if remaining_ng_to_quote is None else float(remaining_ng_to_quote)

    lines.append(
        f"\n**Headline (10% Net Gain):** requirement **{H:.4f}** units. "
        f"Available Low surplus **{low_available:.4f}** → applied **{used_low:.4f}**, leaving **{R:.4f}**."
    )

    if ng_flow_rows:
        lines.append("  - Low surplus used against Headline came from:")
        for r in ng_flow_rows:
            lines.append(f"    - {r['surplus_habitat']} ({r['surplus_band']}{', ' + r['surplus_broad'] if r['surplus_broad'] else ''}) → **{r['units_transferred']:.4f}**")

    if NG > 1e-9:
        lines.append(f"**Net Gain remainder to quote (after habitat residuals):** **{NG:.4f}** units.")
    else:
        lines.append("**Net Gain remainder:** fully covered (no additional NG units to buy).")

    return "\n".join(lines)

# ---------- Banded Sankey: VH → High → Medium → Low → Net Gain ----------
# ---------- Banded Sankey with dual NG tabs: VH → High → Medium → Low → Net Gain ----------
import plotly.graph_objects as go

_BAND_RGB = {
    "Very High": (123, 31, 162),
    "High":      (211, 47, 47),
    "Medium":    (25, 118, 210),
    "Low":       (56, 142, 60),
    "Net Gain":  (69, 90, 100),
    "Other":     (120, 120, 120),
}
BAND_ORDER = ["Very High", "High", "Medium", "Low", "Net Gain"]

def _rgb(band: str) -> str:
    r,g,b = _BAND_RGB.get(str(band), _BAND_RGB["Other"])
    return f"rgb({r},{g},{b})"

def _rgba(band: str, a: float = 0.65) -> str:
    r,g,b = _BAND_RGB.get(str(band), _BAND_RGB["Other"])
    a = min(max(a, 0.0), 1.0)
    return f"rgba({r},{g},{b},{a})"

def _band_xpos() -> dict:
    return {b: 0.05 + i*(0.90/(len(BAND_ORDER)-1)) for i,b in enumerate(BAND_ORDER)}

def _even_y(n: int, offset: float = 0.0) -> list[float]:
    if n <= 0: return []
    ys = [i/(n+1) for i in range(1, n+1)]
    return [min(max(y+offset, 0.03), 0.97) for y in ys]

def build_sankey_banded_with_dual_ng(
    flows_matrix: pd.DataFrame,         # includes habitat→habitat and Low→Headline rows
    residual_table: pd.DataFrame | None,# alloc["residual_off_site"]
    remaining_ng_to_quote: float | None,# headline remainder (>0)
    deficit_table: pd.DataFrame,        # alloc["deficits"] (original negative project_wide_change rows)
    min_link: float = 1e-4
) -> go.Figure:
    """
    Shows every deficit habitat, with:
      - Surplus→Deficit links (colored by surplus band)
      - From each Deficit: outflow to 'NG — deductions' (amount *covered on-site*)
      - From each Deficit: outflow to 'Total NG (to source)' (any unmet residual)
      - For Headline: Low→'D: Net gain uplift (Headline)' as usual; then split covered→NG-deductions, remainder→Total NG
    """
    # ---- Prepare coverage per deficit ----
    f = flows_matrix.copy() if flows_matrix is not None else pd.DataFrame()
    f["units_transferred"] = pd.to_numeric(f.get("units_transferred"), errors="coerce").fillna(0.0)

    # Sum coverage received by each deficit from all surpluses (habitat→habitat + low→headline)
    cov = (
        f.groupby(["deficit_habitat","deficit_band"], dropna=False)["units_transferred"]
         .sum().reset_index().rename(columns={"units_transferred":"covered_units"})
    )

    # Original need per deficit (abs of negative change)
    dtab = deficit_table.copy() if deficit_table is not None else pd.DataFrame(columns=["habitat","distinctiveness","project_wide_change"])
    dtab["need_units"] = dtab["project_wide_change"].abs()
    need = dtab.groupby(["habitat","distinctiveness"], dropna=False)["need_units"].sum().reset_index()

    # Merge to get per-deficit: need, covered, residual (= need - covered, floored at 0)
    df_cov = need.merge(
        cov, how="left",
        left_on=["habitat","distinctiveness"],
        right_on=["deficit_habitat","deficit_band"]
    )
    df_cov["covered_units"] = pd.to_numeric(df_cov["covered_units"], errors="coerce").fillna(0.0)
    df_cov["residual_units"] = (df_cov["need_units"] - df_cov["covered_units"]).clip(lower=0.0)

    # Residuals sanity: if residual_table provided, prefer that for residual habitat values
    residual_map = {}
    if residual_table is not None and not residual_table.empty:
        for _, row in residual_table.iterrows():
            residual_map[f"D: {row['habitat']}"] = float(pd.to_numeric(row["unmet_units_after_on_site_offset"], errors="coerce") or 0.0)

    # ---- Aggregate identical surplus→deficit pairs for the diagram ----
    agg = (
        f.groupby(["deficit_habitat","deficit_band","surplus_habitat","surplus_band"], dropna=False)
         ["units_transferred"].sum().reset_index()
    )
    agg = agg[agg["units_transferred"] > min_link]

    # ---- Build node lists in banded layout ----
    bands_x = _band_xpos()
    surplus_nodes_by_band = {b: [] for b in BAND_ORDER}
    deficit_nodes_by_band = {b: [] for b in BAND_ORDER}

    # Collect nodes from flows
    for _, r in agg.iterrows():
        d_lab = f"D: {r['deficit_habitat']}"
        s_lab = f"S: {r['surplus_habitat']}"
        d_band = str(r["deficit_band"]) if pd.notna(r["deficit_band"]) else "Other"
        s_band = str(r["surplus_band"]) if pd.notna(r["surplus_band"]) else "Other"
        if d_band not in BAND_ORDER: d_band = "Other"
        if s_band not in BAND_ORDER: s_band = "Other"
        if d_lab not in deficit_nodes_by_band[d_band]:
            deficit_nodes_by_band[d_band].append(d_lab)
        if s_lab not in surplus_nodes_by_band[s_band]:
            surplus_nodes_by_band[s_band].append(s_lab)

    # Also include any deficit that had no coverage at all (so it can flow to Total NG)
    for _, r in df_cov.iterrows():
        d_lab = f"D: {r['habitat']}"
        d_band = str(r["distinctiveness"]) if pd.notna(r["distinctiveness"]) else "Other"
        if d_band not in BAND_ORDER: d_band = "Other"
        if d_lab not in sum(deficit_nodes_by_band.values(), []):
            deficit_nodes_by_band[d_band].append(d_lab)

    # Ensure the Headline deficit node exists if Low→Headline or NG remainder present
    include_ng_pool = (remaining_ng_to_quote or 0.0) > min_link
    headline_label = "D: Net gain uplift (Headline)"
    if (headline_label in (f"D: {h}" for h in cov.get("deficit_habitat", []))) or include_ng_pool:
        if headline_label not in deficit_nodes_by_band["Net Gain"]:
            deficit_nodes_by_band["Net Gain"].append(headline_label)

    # Build node arrays with fixed positions; surplus at x-0.04, deficit at x+0.04
    labels, colors, xs, ys, node_index = [], [], [], [], {}

    for band in BAND_ORDER:
        x_center = bands_x[band]
        # Surplus nodes
        sx = x_center - 0.04
        s_nodes = surplus_nodes_by_band[band]
        s_ys = _even_y(len(s_nodes), offset=-0.05 if band in ("Low","Net Gain") else 0.0)
        for i, lab in enumerate(s_nodes):
            labels.append(lab); colors.append(_rgb(band)); xs.append(sx); ys.append(s_ys[i])
            node_index[lab] = len(labels) - 1

        # Deficit nodes
        dx = x_center + 0.04
        d_nodes = deficit_nodes_by_band[band]
        d_ys = _even_y(len(d_nodes), offset=0.0)
        for i, lab in enumerate(d_nodes):
            labels.append(lab); colors.append(_rgb(band)); xs.append(dx); ys.append(d_ys[i])
            node_index[lab] = len(labels) - 1

    # Two NG sinks (always on far right)
    ng_deductions = "Net Gain — deductions (covered on-site)"
    total_ng_sink = "Total Net Gain (to source)"

    def add_sink(label: str, band: str, y: float) -> int:
        if label not in node_index:
            labels.append(label); colors.append(_rgb(band)); xs.append(0.98); ys.append(y)
            node_index[label] = len(labels) - 1
        return node_index[label]

    ng_deductions_idx = add_sink(ng_deductions, "Net Gain", 0.88)
    total_ng_idx      = add_sink(total_ng_sink,  "Net Gain", 0.96)

    # ---- Links ----
    sources, targets, values, lcolors = [], [], [], []

    # (A) Surplus → Deficit (colored by source band)
    for _, r in agg.iterrows():
        s_lab = f"S: {r['surplus_habitat']}"
        d_lab = f"D: {r['deficit_habitat']}"
        val   = float(r["units_transferred"])
        if val <= min_link: continue
        if s_lab in node_index and d_lab in node_index:
            sources.append(node_index[s_lab]); targets.append(node_index[d_lab]); values.append(val)
            lcolors.append(_rgba(str(r["surplus_band"]), 0.7))

    # (B) From each Deficit: split to NG-deductions (covered) and Total-NG (residual)
    for _, r in df_cov.iterrows():
        d_lab = f"D: {r['habitat']}"
        covered = float(r["covered_units"])
        residual_est = float(r["residual_units"])

        # Trust residual_table if present for habitat residuals
        if d_lab in residual_map:
            residual = float(residual_map[d_lab])
            # guard against tiny rounding differences
            covered = max(float(r["need_units"]) - residual, 0.0)
        else:
            residual = residual_est

        if d_lab in node_index:
            if covered > min_link:
                sources.append(node_index[d_lab]); targets.append(ng_deductions_idx); values.append(covered)
                lcolors.append("rgba(100,149,237,0.45)")  # calm blue for “accounted/deducted”
            if residual > min_link:
                sources.append(node_index[d_lab]); targets.append(total_ng_idx); values.append(residual)
                lcolors.append("rgba(120,120,120,0.6)")   # neutral grey for off-site to source

    # (C) NG remainder (Headline after Low/offsets) → Total NG
    if include_ng_pool and (remaining_ng_to_quote or 0.0) > min_link:
        if headline_label in node_index:
            sources.append(node_index[headline_label]); targets.append(total_ng_idx)
            values.append(float(remaining_ng_to_quote))
            lcolors.append(_rgba("Net Gain", 0.8))

    if not values:
        fig = go.Figure()
        fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=400)
        fig.add_annotation(text="No flows to display", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        return fig

    fig = go.Figure(data=[go.Sankey(
        arrangement="snap",
        node=dict(
            pad=15, thickness=20,
            line=dict(width=0.5, color="rgba(120,120,120,0.3)"),
            label=labels, color=colors, x=xs, y=ys
        ),
        link=dict(source=sources, target=targets, value=values, color=lcolors)
    )])
    fig.update_layout(margin=dict(l=10, r=10, t=10, b=10), height=620)
    return fig








# ---------------- UI ----------------
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

# ---------- AREA ----------
with tabs[0]:
    st.subheader("Trading Summary — Area Habitats")
    if area_norm.empty:
        st.warning("No Area Habitats trading summary detected.")
    else:
        st.caption(f"Source sheet: `{area_sheet or 'not found'}`")

        # 1) On-site offsets between habitats (flows)
        alloc = apply_area_offsets(area_norm)

        # 2) Headline deficit & Low→Headline
        headline_def = parse_headline_area_deficit(xls)

        # Prepare per-surplus remaining detail to pull Low into Headline as explicit flows
        surplus_detail = alloc["surplus_after_offsets_detail"].copy()
        surplus_detail["surplus_remaining_units"] = coerce_num(surplus_detail["surplus_remaining_units"]).fillna(0.0)

        # Low surplus total available
        low_remaining = float(surplus_detail.loc[surplus_detail["distinctiveness"]=="Low","surplus_remaining_units"].sum())

        # Amount to apply from Low onto Headline
        applied_low_to_headline = min(headline_def or 0.0, low_remaining) if headline_def is not None else 0.0
        residual_headline_after_low = (headline_def - applied_low_to_headline) if headline_def is not None else None

        # Generate explicit NG coverage flows from Low surpluses (largest-first)
        ng_flow_rows = []
        if applied_low_to_headline and applied_low_to_headline > 1e-9:
            low_items = surplus_detail[surplus_detail["distinctiveness"]=="Low"].copy()
            low_items = low_items.sort_values("surplus_remaining_units", ascending=False)
            to_cover = applied_low_to_headline
            for _, s in low_items.iterrows():
                if to_cover <= 1e-9: break
                give = float(min(to_cover, s["surplus_remaining_units"]))
                if give <= 0: continue
                ng_flow_rows.append({
                    "deficit_habitat": "Net Gain uplift (Headline)",
                    "deficit_broad": "—",
                    "deficit_band": "Net Gain",
                    "surplus_habitat": clean_text(s["habitat"]),
                    "surplus_broad": clean_text(s["broad_group"]),
                    "surplus_band": "Low",
                    "units_transferred": round(give, 7),
                    "flow_type": "low→headline"
                })
                to_cover -= give

        # Combine habitat flows + NG flows into one matrix so the story is traceable
        flows_matrix = pd.concat(
            [alloc["allocation_flows"], pd.DataFrame(ng_flow_rows)],
            ignore_index=True
        ) if ng_flow_rows else alloc["allocation_flows"].copy()

        # Surplus remaining by band (after subtracting what we used for Headline from Low)
        surplus_by_band = alloc["surplus_remaining_by_band"].copy()
        if applied_low_to_headline and applied_low_to_headline > 0:
            mask_low = surplus_by_band["band"] == "Low"
            if mask_low.any():
                surplus_by_band.loc[mask_low, "surplus_remaining_units"] = (
                    surplus_by_band.loc[mask_low, "surplus_remaining_units"] - applied_low_to_headline
                ).clip(lower=0)

        # Habitat residuals after on-site offsets
        residual_table = alloc["residual_off_site"].copy()
        sum_habitat_residuals = float(residual_table["unmet_units_after_on_site_offset"].sum()) if not residual_table.empty else 0.0

        # Net Gain remainder to quote = (Headline after Low) − (habitat residuals)
        remaining_ng_to_quote = None
        if residual_headline_after_low is not None:
            remaining_ng_to_quote = max(residual_headline_after_low - sum_habitat_residuals, 0.0)

        # Combined residual headline table (add NG remainder row only if >0)
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

        # KPIs
        k_units = round(float(combined_residual["unmet_units_after_on_site_offset"].sum()) if not combined_residual.empty else 0.0, 4)
        k_rows  = len(combined_residual) if not combined_residual.empty else 0
        k_ng    = round(float(remaining_ng_to_quote or 0.0), 4)

        # Overall surplus after everything?
        overall_surplus_after_all = float(surplus_by_band["surplus_remaining_units"].sum()) if not surplus_by_band.empty else 0.0
        overflow_happened = (
            (combined_residual.empty or
             (len(combined_residual) == 1 and combined_residual["distinctiveness"].iloc[0] == "Net Gain" and
              combined_residual["unmet_units_after_on_site_offset"].iloc[0] <= 1e-9))
            and (remaining_ng_to_quote is not None and remaining_ng_to_quote <= 1e-9)
            and (overall_surplus_after_all > 1e-9)
        )

        # ---------- EXPLAINER (maths in words) ----------
        explain_md = build_area_explanation(
            alloc=alloc,
            headline_def=headline_def,
            low_available=float(low_remaining),
            applied_low_to_headline=applied_low_to_headline,
            residual_headline_after_low=residual_headline_after_low,
            remaining_ng_to_quote=remaining_ng_to_quote,
            ng_flow_rows=ng_flow_rows
        )
        st.markdown(
            '<div class="explain-card"><h4>What this app just did (in plain English)</h4><p>We read the Metric and applied the trading rules; here’s exactly how units moved:</p>'
            + explain_md.replace("\n","<br/>") +
            f'<p class="explain-kv">Key numbers: <code>headline={0.0 if headline_def is None else float(headline_def):.4f}</code>, '
            f'<code>low_used={float(applied_low_to_headline or 0.0):.4f}</code>, '
            f'<code>headline_after_low={0.0 if residual_headline_after_low is None else float(residual_headline_after_low):.4f}</code>, '
            f'<code>habitat_unmet={sum_habitat_residuals:.4f}</code>, '
            f'<code>ng_remainder={float(remaining_ng_to_quote or 0.0):.4f}</code>'
            + (f', <code>overall_surplus={overall_surplus_after_all:.4f}</code>' if overflow_happened else '') +
            "</p></div>",
            unsafe_allow_html=True
        )
        with st.expander("📊 Sankey — Banded (VH → High → Medium → Low → Net Gain)", expanded=False):
            sankey_fig = build_sankey_banded_with_dual_ng(
                flows_matrix=flows_matrix,
                residual_table=residual_table,
                remaining_ng_to_quote=remaining_ng_to_quote,
                deficit_table=alloc["deficits"]  # original per-habitat needs
            )
            st.plotly_chart(sankey_fig, use_container_width=True, theme="streamlit")
                        


        
        # ---------- HERO CARD ----------
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

        if overflow_happened:
            st.success("🎉 **Overall surplus after meeting all Area + 10% Net Gain**")
            st.write(pd.DataFrame([{"surplus_units_total": round(overall_surplus_after_all, 4)}]))
            st.write(
                surplus_by_band.rename(columns={
                    "band": "distinctiveness_band",
                    "surplus_remaining_units": "surplus_units_after_allocation_and_NG"
                })
            )
            st.session_state["total_overall_surplus_area"] = float(round(overall_surplus_after_all, 4))
        else:
            st.info("No overall surplus remaining after meeting habitat deficits and 10% Net Gain.")
            st.session_state["total_overall_surplus_area"] = 0.0

        st.dataframe(combined_residual, use_container_width=True, height=260)

        cdl1, cdl2, _ = st.columns([1,1,3])
        with cdl1:
            st.download_button("⬇️ Download CSV",
                combined_residual.to_csv(index=False).encode("utf-8"),
                "area_residual_offsite_incl_ng_remainder.csv", "text/csv")
        with cdl2:
            st.download_button("⬇️ Download JSON",
                combined_residual.to_json(orient="records", indent=2).encode("utf-8"),
                "area_residual_offsite_incl_ng_remainder.json", "application/json")
        st.markdown('</div>', unsafe_allow_html=True)

        # Save for Exports
        st.session_state["combined_residual_area"] = combined_residual

        # ---------- Expanders ----------
        with st.expander("🔗 Eligibility matrix (mitigation flows — includes Low→Headline)", expanded=False):
            if flows_matrix.empty:
                st.info("No flows recorded.")
            else:
                show = flows_matrix.rename(columns={
                    "deficit_habitat":"deficit",
                    "deficit_broad":"deficit_broad_group",
                    "deficit_band":"deficit_distinctiveness",
                    "surplus_habitat":"source_surplus",
                    "surplus_broad":"source_broad_group",
                    "surplus_band":"source_distinctiveness",
                    "units_transferred":"units",
                    "flow_type":"flow_type"
                })
                st.dataframe(show, use_container_width=True, height=380)

        with st.expander("🧮 Surplus remaining by band (after all on-site offsets & Low→Headline)", expanded=False):
            st.dataframe(surplus_by_band, use_container_width=True, height=220)

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

        with st.expander("📋 Normalised input table (Area Habitats)", expanded=False):
            st.dataframe(area_norm, use_container_width=True, height=420)

# ---------- HEDGEROWS ----------
with tabs[1]:
    st.subheader("Hedgerows")
    if hedge_norm.empty:
        st.info("No Hedgerows trading summary detected.")
    else:
        with st.expander("📋 Normalised table — Hedgerows", expanded=True):
            st.caption(f"Source sheet: `{hedge_sheet or 'not found'}`")
            st.dataframe(hedge_norm, use_container_width=True, height=480)

# ---------- WATERCOURSES ----------
with tabs[2]:
    st.subheader("Watercourses")
    if water_norm.empty:
        st.info("No Watercourses trading summary detected.")
    else:
        with st.expander("📋 Normalised table — Watercourses", expanded=True):
            st.caption(f"Source sheet: `{water_sheet or 'not found'}`")
            st.dataframe(water_norm, use_container_width=True, height=480)

# ---------- EXPORTS ----------
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

        # Optional: export overall surplus number if any
        surplus_num = st.session_state.get("total_overall_surplus_area", 0.0)
        if surplus_num and surplus_num > 0:
            surplus_df = pd.DataFrame([{
                "category": "Area Habitats",
                "total_overall_surplus_units": float(surplus_num)
            }])
            st.download_button(
                "⬇️ Download overall surplus (Area) — JSON",
                surplus_df.to_json(orient="records", indent=2).encode("utf-8"),
                "overall_surplus_area.json",
                "application/json"
            )

st.caption("The headline card is the number you quote. The flows table now also shows Low→Headline coverage, so Net Gain mitigation is fully traceable.")







