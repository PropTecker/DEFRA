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

# ---------- Banded Sankey: Requirements (left) → Surpluses (right) → Total Net Gain (far right) ----------
# ---------- Banded Sankey: Requirements (left) → Surpluses (right) → Total Net Gain (far right)
import plotly.graph_objects as go

_BAND_RGB = {
    "Very High": (123, 31, 162),
    "High":      (211, 47, 47),
    "Medium":    (25, 118, 210),
    "Low":       (56, 142, 60),
    "Net Gain":  (69, 90, 100),
    "Other":     (120, 120, 120),
}
BAND_ORDER = ["Headline", "Very High", "High", "Medium", "Low", "Total NG"]  # for headers, visual only

def _rgb_triplet(band):
    return _BAND_RGB.get(str(band), _BAND_RGB["Other"])

def _rgb(band):  r,g,b = _rgb_triplet(band); return f"rgb({r},{g},{b})"
def _rgba(band,a=.68): r,g,b=_rgb_triplet(band); return f"rgba({r},{g},{b},{min(max(a,0),1)})"

def _band_xpos_visual():
    # Even spacing across 0.06..0.94 for 6 “visual” columns (Headline, VH, High, Medium, Low, Total NG)
    cols = BAND_ORDER
    return {b: 0.06 + i*(0.88/(len(cols)-1)) for i,b in enumerate(cols)}

def _even_y(n, offset=0.0):
    if n<=0: return []
    ys=[i/(n+1) for i in range(1,n+1)]
    return [min(max(y+offset, 0.03), 0.97) for y in ys]

def build_sankey_requirements_left(
    flows_matrix: pd.DataFrame,
    residual_table: pd.DataFrame | None,
    remaining_ng_to_quote: float | None,
    deficit_table: pd.DataFrame,
    min_link: float = 1e-4,
    height: int = 400,          # was 560
    compact_nodes: bool = True, # default to compact
    show_zebra: bool = True
) -> go.Figure:
    # ----- (all your existing data-prep code stays the same) -----
    f = flows_matrix.copy() if flows_matrix is not None else pd.DataFrame()
    f["units_transferred"] = pd.to_numeric(f.get("units_transferred"), errors="coerce").fillna(0.0)

    cov = (
        f.groupby(["deficit_habitat","deficit_band"], dropna=False)["units_transferred"]
         .sum().reset_index().rename(columns={"units_transferred":"covered_units"})
    )

    dtab = deficit_table.copy() if deficit_table is not None else pd.DataFrame(columns=["habitat","distinctiveness","project_wide_change"])
    dtab["need_units"] = pd.to_numeric(dtab["project_wide_change"], errors="coerce").abs()
    per_def = (
        dtab.groupby(["habitat","distinctiveness"], dropna=False)["need_units"].sum().reset_index()
        .merge(cov, how="left",
               left_on=["habitat","distinctiveness"],
               right_on=["deficit_habitat","deficit_band"])
    )
    per_def["covered_units"]  = pd.to_numeric(per_def["covered_units"], errors="coerce").fillna(0.0)
    per_def["residual_units"] = (per_def["need_units"] - per_def["covered_units"]).clip(lower=0.0)

    residual_map = {}
    if residual_table is not None and not residual_table.empty:
        for _, row in residual_table.iterrows():
            residual_map[f"D: {row['habitat']}"] = float(pd.to_numeric(row["unmet_units_after_on_site_offset"], errors="coerce") or 0.0)

    agg = (
        f.groupby(["deficit_habitat","deficit_band","surplus_habitat","surplus_band"], dropna=False)
         ["units_transferred"].sum().reset_index()
    )
    agg = agg[agg["units_transferred"] > min_link]

    # ----- layout buckets -----
    # “Real” bands for data: we still place nodes by distinctiveness slices VH, High, Medium, Low, Net Gain
    data_band_to_x = {
        "Very High": 0.06 + 1*(0.88/(6-1)),  # place VH in the 2nd visual slot
        "High":      0.06 + 2*(0.88/(6-1)),
        "Medium":    0.06 + 3*(0.88/(6-1)),
        "Low":       0.06 + 4*(0.88/(6-1)),
        "Net Gain":  0.06 + 5*(0.88/(6-1)),
    }
    # Headline gets its own left-most visual slot
    headline_x = 0.06

    req_nodes_by_band = {b: [] for b in ["Very High","High","Medium","Low","Net Gain"]}
    sur_nodes_by_band = {b: [] for b in ["Very High","High","Medium","Low","Net Gain"]}

    for _, r in agg.iterrows():
        d_lab = f"D: {r['deficit_habitat']}"
        s_lab = f"S: {r['surplus_habitat']}"
        d_band = str(r["deficit_band"]) if pd.notna(r["deficit_band"]) else "Other"
        s_band = str(r["surplus_band"]) if pd.notna(r["surplus_band"]) else "Other"
        if d_band not in req_nodes_by_band and d_band != "Net Gain":  # ignore “Other”
            continue
        if s_band not in sur_nodes_by_band and s_band != "Net Gain":
            continue
        if d_band in req_nodes_by_band and d_lab not in req_nodes_by_band[d_band]:
            req_nodes_by_band[d_band].append(d_lab)
        if s_band in sur_nodes_by_band and s_lab not in sur_nodes_by_band[s_band]:
            sur_nodes_by_band[s_band].append(s_lab)

    # add deficits with zero coverage
    for _, r in per_def.iterrows():
        d_lab = f"D: {r['habitat']}"
        d_band = str(r["distinctiveness"])
        if d_band in req_nodes_by_band and d_lab not in sum(req_nodes_by_band.values(), []):
            req_nodes_by_band[d_band].append(d_lab)

    # Always show Headline left node
    headline_left = "D: Headline 10% requirement"

    # ----- assemble nodes -----
    labels, colors, xs, ys, idx = [], [], [], [], {}

    # Headline (left-most)
    labels.append(headline_left); colors.append(_rgb("Net Gain")); xs.append(headline_x - 0.02); ys.append(0.12)
    idx[headline_left] = len(labels) - 1

    # Distinctiveness slices (VH..Low..Net Gain)
    for band, xcenter in data_band_to_x.items():
        # requirements (left side of slice)
        left_x = xcenter - 0.035
        reqs = req_nodes_by_band.get(band, [])
        req_ys = _even_y(len(reqs), offset=0.0 if band != "Net Gain" else -0.06)
        for i, lab in enumerate(reqs):
            labels.append(lab); colors.append(_rgb(band)); xs.append(left_x); ys.append(req_ys[i])
            idx[lab] = len(labels) - 1
        # surpluses (right side of slice)
        right_x = xcenter + 0.035
        surs = sur_nodes_by_band.get(band, [])
        sur_ys = _even_y(len(surs), offset=0.0 if band != "Low" else -0.03)
        for i, lab in enumerate(surs):
            labels.append(lab); colors.append(_rgb(band)); xs.append(right_x); ys.append(sur_ys[i])
            idx[lab] = len(labels) - 1

    # Total NG sink (far right)
    total_ng = "Total Net Gain (to source)"
    labels.append(total_ng); colors.append(_rgb("Net Gain")); xs.append(0.98); ys.append(0.92)
    idx[total_ng] = len(labels) - 1

    # ----- links -----
    sources, targets, values, lcolors = [], [], [], []

    # Deficit → Surplus
    for _, r in agg.iterrows():
        d_lab = f"D: {r['deficit_habitat']}"
        s_lab = f"S: {r['surplus_habitat']}"
        val   = float(r["units_transferred"])
        if val <= min_link: continue
        if d_lab in idx and s_lab in idx:
            sources.append(idx[d_lab]); targets.append(idx[s_lab]); values.append(val)
            lcolors.append(_rgba(str(r["surplus_band"]), 0.72))

    # Each deficit’s unmet residual → Total NG
    for _, r in per_def.iterrows():
        d_lab = f"D: {r['habitat']}"
        residual = residual_map.get(d_lab, float(r["residual_units"]))
        if residual > min_link and d_lab in idx:
            sources.append(idx[d_lab]); targets.append(idx[total_ng]); values.append(residual)
            lcolors.append("rgba(120,120,120,0.6)")

    # Headline → Low surplus (applied)
    low_to_head = f[
        (f["deficit_habitat"].astype(str).str.strip().str.lower() == "net gain uplift (headline)")
        & (f["units_transferred"] > min_link)
    ]
    for _, rr in low_to_head.iterrows():
        s_lab = f"S: {rr['surplus_habitat']}"
        if (headline_left in idx) and (s_lab in idx):
            amt = float(rr["units_transferred"])
            sources.append(idx[headline_left]); targets.append(idx[s_lab]); values.append(amt)
            lcolors.append(_rgba("Low", 0.78))

    # Headline remainder → Total NG
    if (remaining_ng_to_quote or 0.0) > min_link and (headline_left in idx):
        sources.append(idx[headline_left]); targets.append(idx[total_ng])
        values.append(float(remaining_ng_to_quote)); lcolors.append(_rgba("Net Gain", 0.85))

    # If no links, show friendly placeholder
    if not values:
        fig = go.Figure()
        fig.update_layout(margin=dict(l=6, r=6, t=10, b=6), height=max(380, height))
        fig.add_annotation(text="No flows to display", showarrow=False, x=0.5, y=0.5, xref="paper", yref="paper")
        return fig

    node_kwargs = dict(
        pad=8 if compact_nodes else 12,        # was 10/14
        thickness=12 if compact_nodes else 16, # was 14/18
        line=dict(width=0.4, color="rgba(120,120,120,0.25)"),
        label=labels, color=colors, x=xs, y=ys
    )

    fig = go.Figure(data=[go.Sankey(
        arrangement="snap",
        node=node_kwargs,
        link=dict(source=sources, target=targets, value=values, color=lcolors)
    )])

    # ---------------- Zebra stripes that actually line up ----------------
    if show_zebra:
        # Visual columns (left→right): Headline, VH, High, Medium, Low, Total NG
        visual_labels = ["Headline", "Very High", "High", "Medium", "Low", "Total Net Gain"]

        # Compute centres equally spaced across paper coords [0.06..0.94]
        ncols = len(visual_labels)
        x0_all = 0.06
        x1_all = 0.94
        step = (x1_all - x0_all) / (ncols - 1)
        centres = [x0_all + i * step for i in range(ncols)]

        # Convert centres to stripe boundaries halfway to neighbours
        # First and last stripes get a small padding so they don't touch edges
        pad_edge = 0.01
        bounds = []
        for i, c in enumerate(centres):
            if i == 0:
                left = max(0.0, c - step/2 - pad_edge)
            else:
                left = (centres[i-1] + c) / 2.0
            if i == ncols - 1:
                right = min(1.0, c + step/2 + pad_edge)
            else:
                right = (c + centres[i+1]) / 2.0
            bounds.append((left, right))

        # Build alternating “zebra” rectangles and headers
        shapes = []
        annotations = []
        for i, (lab, (x0b, x1b), xc) in enumerate(zip(visual_labels, bounds, centres)):
            fill = "rgba(0,0,0,0.05)" if i % 2 == 0 else "rgba(0,0,0,0.09)"
            shapes.append(dict(
                type="rect", xref="paper", yref="paper",
                x0=x0b, x1=x1b, y0=0.0, y1=1.0,
                layer="below", line=dict(width=0), fillcolor=fill
            ))
            annotations.append(dict(
                text=f"<b>{lab}</b>", x=xc, y=0.995, xref="paper", yref="paper",
                showarrow=False, yanchor="top", font=dict(size=11)   # was 12
            ))

        fig.update_layout(shapes=shapes, annotations=annotations)

    # Tight margins so it fits the expander; no autosizing creep
    fig.update_layout(
        margin=dict(l=6, r=6, t=28 if show_zebra else 6, b=4),  # smaller top/bottom
        height=max(360, height),  # guard but smaller target
        autosize=False
    )

    # Prevent Plotly from expanding beyond container width
    fig.update_layout(
        autosize=False
    )

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
        with st.expander("📊 Sankey — Requirements (left) → Surpluses (right) → Total Net Gain", expanded=False):
            sankey_fig = build_sankey_requirements_left(
                flows_matrix=flows_matrix,
                residual_table=residual_table,
                remaining_ng_to_quote=remaining_ng_to_quote,
                deficit_table=alloc["deficits"],
                height=380,            # <= compact height
                compact_nodes=True,    # force compact
                show_zebra=True
            )
            st.plotly_chart(
                sankey_fig,
                use_container_width=True,
                theme="streamlit",
                config={"displayModeBar": False, "responsive": True}
            )

        
                                


        
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







