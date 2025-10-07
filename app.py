# app.py — DEFRA BNG Metric Reader (Area rules + correct NG remainder row)

import re
from typing import Dict, List, Optional, Tuple
import pandas as pd
import streamlit as st

st.set_page_config(page_title="DEFRA BNG Metric Reader", page_icon="🌿", layout="wide")

# ---------- Utils ----------
def clean_text(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)): return ""
    s = str(x).strip()
    return re.sub(r"\s+", " ", s)

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

def find_header_row(df: pd.DataFrame, within_rows: int = 60) -> Optional[int]:
    for i in range(min(within_rows, len(df))):
        row = " ".join([clean_text(x) for x in df.iloc[i].tolist()]).lower()
        if ("group" in row) and (("on-site" in row and "off-site" in row and "project" in row) or "project wide" in row or "project-wide" in row):
            return i
    return None

def load_trading_df(xls: pd.ExcelFile, sheet: str) -> pd.DataFrame:
    raw = pd.read_excel(xls, sheet_name=sheet, header=None)
    hdr = find_header_row(raw)
    if hdr is None:
        df = pd.read_excel(xls, sheet_name=sheet)
    else:
        headers = raw.iloc[hdr].map(clean_text).tolist()
        df = raw.iloc[hdr+1:].copy()
        df.columns = headers
    df = df.loc[:, ~df.columns.duplicated()].copy()
    df = df.dropna(how="all").reset_index(drop=True)
    return df

def col_like(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cols = {canon(c): c for c in df.columns}
    for c in cands:
        if canon(c) in cols: return cols[canon(c)]
    for k,v in cols.items():
        if any(canon(c) in k for c in cands): return v
    return None

def tag_distinctiveness(df: pd.DataFrame, habitat_col: str) -> pd.DataFrame:
    out = df.copy(); out["__distinctiveness__"] = pd.NA; band = None
    for idx,row in out.iterrows():
        joined = " ".join([clean_text(x) for x in row.tolist() if isinstance(x,str)]).lower()
        if "very high distinctiveness" in joined: band = "Very High"
        elif "high distinctiveness" in joined and "very" not in joined: band = "High"
        elif "medium distinctiveness" in joined: band = "Medium"
        elif "low distinctiveness" in joined: band = "Low"
        if band and isinstance(row.get(habitat_col,""), str) and clean_text(row.get(habitat_col,""))!="":
            out.loc[idx,"__distinctiveness__"] = band
    return out

# ---------- Normalise ----------
def normalise_requirements(xls: pd.ExcelFile, sheet_candidates: List[str], category_label: str):
    sheet = find_sheet(xls, sheet_candidates) or ""
    if not sheet:
        return pd.DataFrame(columns=["category","habitat","broad_group","distinctiveness","project_wide_change","on_site_change"]), {}, sheet
    df = load_trading_df(xls, sheet)
    habitat_col = col_like(df, "Habitat", "Feature")
    broad_col   = col_like(df, "Habitat group", "Broad habitat", "Group")
    proj_col    = col_like(df, "Project-wide unit change", "Project wide unit change")
    ons_col     = col_like(df, "On-site unit change", "On site unit change")
    if not habitat_col or not proj_col:
        return pd.DataFrame(columns=["category","habitat","broad_group","distinctiveness","project_wide_change","on_site_change"]), {}, sheet
    df = tag_distinctiveness(df, habitat_col)
    df = df[~df[habitat_col].isna()]; df = df[df[habitat_col].astype(str).str.strip()!=""].copy()
    for c in [proj_col, ons_col]:
        if c in df.columns: df[c] = coerce_num(df[c])
    out = pd.DataFrame({
        "category": category_label,
        "habitat": df[habitat_col],
        "broad_group": df[broad_col] if broad_col in df.columns else pd.NA,
        "distinctiveness": df["__distinctiveness__"] if "__distinctiveness__" in df.columns else pd.NA,
        "project_wide_change": df[proj_col],
        "on_site_change": df[ons_col] if ons_col in df.columns else pd.NA,
    })
    return out.reset_index(drop=True), {
        "habitat": habitat_col, "broad_group": broad_col or "", "project_wide_change": proj_col,
        "on_site_change": ons_col or "", "distinctiveness_tagged": "__distinctiveness__",
    }, sheet

# ---------- Area rules ----------
def can_offset_area(d_band: str, d_broad: str, d_hab: str, s_band: str, s_broad: str, s_hab: str) -> bool:
    rank = {"Low":1,"Medium":2,"High":3,"Very High":4}
    rd = rank.get(str(d_band),0); rs = rank.get(str(s_band),0)
    d_broad = clean_text(d_broad); s_broad = clean_text(s_broad)
    d_hab = clean_text(d_hab); s_hab = clean_text(s_hab)
    if d_band == "Very High": return d_hab == s_hab
    if d_band == "High":      return d_hab == s_hab
    if d_band == "Medium":    return (d_broad!="" and d_broad==s_broad) and (rs>=rd)  # same group, >=
    if d_band == "Low":       return rs>=rd
    return False

def apply_area_offsets(area_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    data = area_df.copy()
    data["project_wide_change"] = coerce_num(data["project_wide_change"])
    deficits  = data[data["project_wide_change"]<0].copy()
    surpluses = data[data["project_wide_change"]>0].copy()

    elig = []
    for _,d in deficits.iterrows():
        for _,s in surpluses.iterrows():
            if can_offset_area(str(d["distinctiveness"]), d.get("broad_group",""), d.get("habitat",""),
                               str(s["distinctiveness"]), s.get("broad_group",""), s.get("habitat","")):
                elig.append({
                    "deficit_habitat": clean_text(d.get("habitat","")),
                    "deficit_broad":   clean_text(d.get("broad_group","")),
                    "deficit_band":    d["distinctiveness"],
                    "deficit_units":   abs(float(d["project_wide_change"])),
                    "surplus_habitat": clean_text(s.get("habitat","")),
                    "surplus_broad":   clean_text(s.get("broad_group","")),
                    "surplus_band":    s["distinctiveness"],
                    "surplus_units":   float(s["project_wide_change"]),
                })
    elig_df = pd.DataFrame(elig)

    band_rank = {"Low":1,"Medium":2,"High":3,"Very High":4}
    sur = surpluses.copy(); sur["__remain__"] = sur["project_wide_change"].astype(float)

    remaining = []
    for _, d in deficits.iterrows():
        need = abs(float(d["project_wide_change"]))
        d_band = str(d["distinctiveness"]); d_broad = d.get("broad_group",""); d_hab = d.get("habitat","")
        idxs = [si for si,s in sur.iterrows()
                if can_offset_area(d_band, d_broad, d_hab, str(s["distinctiveness"]), s.get("broad_group",""), s.get("habitat",""))
                and sur.loc[si,"__remain__"]>0]
        idxs = sorted(idxs, key=lambda i: (-band_rank.get(str(sur.loc[i,"distinctiveness"]),0), -sur.loc[i,"__remain__"]))
        for i in idxs:
            use = min(need, sur.loc[i,"__remain__"]); 
            if use>0: sur.loc[i,"__remain__"] -= use; need -= use
            if need<=1e-9: break
        if need>1e-9:
            remaining.append({
                "habitat": clean_text(d_hab),
                "broad_group": clean_text(d_broad),
                "distinctiveness": d_band,
                "unmet_units_after_on_site_offset": round(need,4)
            })

    surplus_remaining_by_band = sur.groupby("distinctiveness", dropna=False)["__remain__"].sum().reset_index()
    surplus_remaining_by_band = surplus_remaining_by_band.rename(columns={"distinctiveness":"band","__remain__":"surplus_remaining_units"})

    return {
        "deficits": deficits.sort_values("project_wide_change"),
        "surpluses": surpluses.sort_values("project_wide_change", ascending=False),
        "eligibility": elig_df,
        "surplus_remaining_by_band": surplus_remaining_by_band,
        "residual_off_site": pd.DataFrame(remaining).sort_values(
            ["distinctiveness","unmet_units_after_on_site_offset"], ascending=[False,False]
        ).reset_index(drop=True)
    }

# ---------- Headline ----------
def parse_headline_area_deficit(xls: pd.ExcelFile) -> Optional[float]:
    for name in ["Headline Results","Headline results","Headline","Results"]:
        sheet = find_sheet(xls, [name])
        if not sheet: continue
        hr = pd.read_excel(xls, sheet_name=sheet, header=None)
        header_row = None
        for i in range(min(60,len(hr))):
            row = " ".join([clean_text(x) for x in hr.iloc[i].tolist()]).lower()
            if "unit deficit" in row: header_row=i; break
        if header_row is not None:
            hr2 = hr.iloc[header_row:].copy()
            hr2.columns = [clean_text(x) for x in hr2.iloc[0].tolist()]
            hr2 = hr2.iloc[1:]
            if "Unit Deficit" in hr2.columns:
                mask = hr2.apply(lambda r: "area habitat units" in " ".join([clean_text(v).lower() for v in r.tolist()]), axis=1)
                if mask.any():
                    val = pd.to_numeric(hr2.loc[mask,"Unit Deficit"], errors="coerce")
                    if not val.dropna().empty: return float(val.dropna().iloc[0])
        # fallback scan
        for i in range(len(hr)):
            vals = [clean_text(x) for x in hr.iloc[i].tolist()]
            if any("area habitat units" in v.lower() for v in vals):
                nums = pd.to_numeric(pd.Series(vals), errors="coerce").dropna()
                if not nums.empty: return float(nums.iloc[-1])
    return None

# ---------- UI ----------
st.title("🌿 DEFRA BNG Metric Reader")
st.caption("Reads DEFRA BNG Metric (.xlsx). Normalises Trading Summaries. For Area Habitats, applies your trading rules and nets off remaining Low against the Headline Area Unit Deficit.")

with st.sidebar:
    file = st.file_uploader("Upload DEFRA BNG Metric (.xlsx)", type=["xlsx"])
    st.markdown("---")
    st.markdown("**Area rules:** Very High = same habitat; High = same habitat; Medium = same **broad group**, ≥ Medium; Low = ≥ Low. Remaining Low → Headline deficit.")

if not file:
    st.info("Upload a Metric workbook to begin."); st.stop()

try:
    xls = pd.ExcelFile(file)
except Exception as e:
    st.error(f"Could not open workbook: {e}"); st.stop()

st.success("Workbook loaded.")
st.write("**Sheets detected:**", xls.sheet_names)

AREA_SHEETS = ["Trading Summary Area Habitats","Area Habitats Trading Summary","Area Trading Summary","Trading Summary (Area Habitats)"]
HEDGE_SHEETS = ["Trading Summary Hedgerows","Hedgerows Trading Summary","Hedgerow Trading Summary","Trading Summary (Hedgerows)"]
WATER_SHEETS = ["Trading Summary WaterCs","Trading Summary Watercourses","Watercourses Trading Summary","Trading Summary (Watercourses)"]

area_norm, _, area_sheet = normalise_requirements(xls, AREA_SHEETS, "Area Habitats")
hedge_norm, _, hedge_sheet = normalise_requirements(xls, HEDGE_SHEETS, "Hedgerows")
water_norm, _, water_sheet = normalise_requirements(xls, WATER_SHEETS, "Watercourses")

tabs = st.tabs(["Area Habitats", "Hedgerows", "Watercourses", "Exports"])

# --- Area
with tabs[0]:
    st.subheader("Trading Summary — Area Habitats")
    st.caption(f"Source sheet: `{area_sheet or 'not found'}`")
    if area_norm.empty:
        st.warning("No Area Habitats trading summary detected.")
    else:
        st.dataframe(area_norm, use_container_width=True, height=420)
        alloc = apply_area_offsets(area_norm)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**Deficits**")
            st.dataframe(alloc["deficits"][["habitat","broad_group","distinctiveness","project_wide_change"]],
                         use_container_width=True, height=260)
            st.markdown("**Surpluses**")
            st.dataframe(alloc["surpluses"][["habitat","broad_group","distinctiveness","project_wide_change"]],
                         use_container_width=True, height=260)
        with col2:
            st.markdown("**Eligibility matrix**")
            st.dataframe(alloc["eligibility"], use_container_width=True, height=300)
            st.markdown("**Surplus remaining by band**")
            st.dataframe(alloc["surplus_remaining_by_band"], use_container_width=True, height=160)

        # Headline (10% NG) after applying remaining Low
        headline_def = parse_headline_area_deficit(xls)  # from Headline Results
        low_remaining = float(alloc["surplus_remaining_by_band"].loc[
            alloc["surplus_remaining_by_band"]["band"]=="Low", "surplus_remaining_units"
        ].sum()) if not alloc["surplus_remaining_by_band"].empty else 0.0
        applied_low_to_headline = min(headline_def, low_remaining) if headline_def is not None else None
        residual_headline_after_low = (headline_def - applied_low_to_headline) if headline_def is not None else None

        # SUM of habitat-level residuals (these will also count toward the headline)
        residual_table = alloc["residual_off_site"].copy()
        sum_habitat_residuals = float(residual_table["unmet_units_after_on_site_offset"].sum()) if not residual_table.empty else 0.0

        # Remaining NG to quote = headline residual AFTER Low  − sum of habitat residuals
        remaining_ng_to_quote = None
        if residual_headline_after_low is not None:
            remaining_ng_to_quote = max(residual_headline_after_low - sum_habitat_residuals, 0.0)

        # Show calculation summary
        st.markdown("**Headline Results — Low → Headline & remainder calculation**")
        st.write(pd.DataFrame([{
            "headline_area_unit_deficit": headline_def,
            "low_band_surplus_applied_to_headline": None if applied_low_to_headline is None else round(applied_low_to_headline,4),
            "residual_headline_after_low": None if residual_headline_after_low is None else round(residual_headline_after_low,4),
            "sum_habitat_residuals": round(sum_habitat_residuals,4),
            "remaining_net_gain_to_quote": None if remaining_ng_to_quote is None else round(remaining_ng_to_quote,4),
        }]))

        # Combined residual table = habitat residuals + ONLY the NG remainder (if >0)
        combined_residual = residual_table.copy()
        if remaining_ng_to_quote is not None and remaining_ng_to_quote > 1e-9:
            combined_residual = pd.concat([combined_residual, pd.DataFrame([{
                "habitat": "Net gain uplift (Area, residual after habitat-specific)",
                "broad_group": "—",
                "distinctiveness": "Net Gain",
                "unmet_units_after_on_site_offset": round(remaining_ng_to_quote, 4)
            }])], ignore_index=True)

        st.markdown("**Still needs mitigation OFF-SITE (after offsets + Low→Headline)**")
        if combined_residual.empty:
            st.success("No unmet units after on-site offsets and Low→Headline application.")
        else:
            st.dataframe(combined_residual, use_container_width=True, height=260)

        st.session_state["combined_residual_area"] = combined_residual
        st.download_button("Download residual off-site (Area incl. NG remainder) — CSV",
                           combined_residual.to_csv(index=False).encode("utf-8"),
                           "area_residual_offsite_incl_ng_remainder.csv", "text/csv")

# --- Hedgerows
with tabs[1]:
    st.subheader("Trading Summary — Hedgerows (normalised)")
    st.caption(f"Source sheet: `{hedge_sheet or 'not found'}`")
    if hedge_norm.empty: st.info("No Hedgerows trading summary detected.")
    else: st.dataframe(hedge_norm, use_container_width=True, height=480)

# --- Watercourses
with tabs[2]:
    st.subheader("Trading Summary — Watercourses (normalised)")
    st.caption(f"Source sheet: `{water_sheet or 'not found'}`")
    if water_norm.empty: st.info("No Watercourses trading summary detected.")
    else: st.dataframe(water_norm, use_container_width=True, height=480)

# --- Exports
with tabs[3]:
    st.subheader("Exports")
    norm_concat = pd.concat([df for df in [area_norm, hedge_norm, water_norm] if not df.empty], ignore_index=True) \
        if (not area_norm.empty or not hedge_norm.empty or not water_norm.empty) else \
        pd.DataFrame(columns=["category","habitat","broad_group","distinctiveness","project_wide_change","on_site_change"])
    if norm_concat.empty: st.info("No normalised rows to export.")
    else:
        st.dataframe(norm_concat, use_container_width=True, height=420)
        req_export = norm_concat.copy()
        req_export["required_offsite_units"] = req_export["project_wide_change"].apply(lambda x: abs(x) if pd.notna(x) and x<0 else 0)
        req_export = req_export[req_export["required_offsite_units"]>0].reset_index(drop=True)
        st.download_button("Download normalised requirements — CSV",
                           req_export.to_csv(index=False).encode("utf-8"),
                           "requirements_export.csv", "text/csv")
        st.download_button("Download normalised requirements — JSON",
                           req_export.to_json(orient="records", indent=2).encode("utf-8"),
                           "requirements_export.json", "application/json")
        combined_residual_area = st.session_state.get("combined_residual_area", pd.DataFrame())
        if not combined_residual_area.empty:
            st.download_button("Download residual to mitigate (Area incl. NG remainder) — CSV",
                               combined_residual_area.to_csv(index=False).encode("utf-8"),
                               "area_residual_to_mitigate_incl_ng_remainder.csv", "text/csv")
            st.download_button("Download residual to mitigate (Area incl. NG remainder) — JSON",
                               combined_residual_area.to_json(orient="records", indent=2).encode("utf-8"),
                               "area_residual_to_mitigate_incl_ng_remainder.json", "application/json")

