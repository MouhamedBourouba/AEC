"""
=============================================================================
المرحلة الثانية: تحليل المحفظة التأمينية
Phase 2: Insurance Portfolio Analysis
=============================================================================

This script performs:
A. Data Segmentation:
   - By risk type (residential, commercial, industrial)
   - By RPA seismic zone (0, I, IIa, IIb, III)
   - Sum Insured estimation by commune and wilaya

B. Concentration Analysis:
   - Accumulation calculation and hotspot identification
   - Probable Maximum Loss (PML) simulation

Note: Since VALEUR_ASSURÉE is 100% missing, we use PRIME_NETTE as the
primary metric and estimate insured capital using industry rate assumptions.

Author: Professional Data Analysis - Earthquake Insurance Portfolio
Date: April 2025
"""

import openpyxl
import json
import os
import re
import math
from collections import defaultdict
from datetime import datetime

# ============================================================================
# 1. REUSE MAPPING FROM PHASE 1
# ============================================================================

RPA_ZONES = {
    1: {"zone": "0"}, 8: {"zone": "0"}, 11: {"zone": "0"}, 30: {"zone": "0"},
    33: {"zone": "0"}, 37: {"zone": "0"}, 39: {"zone": "0"}, 45: {"zone": "0"},
    47: {"zone": "0"}, 49: {"zone": "0"}, 50: {"zone": "0"}, 53: {"zone": "0"},
    54: {"zone": "0"}, 56: {"zone": "0"},
    3: {"zone": "I"}, 7: {"zone": "I"}, 12: {"zone": "I"}, 17: {"zone": "I"},
    20: {"zone": "I"}, 40: {"zone": "I"}, 51: {"zone": "I"}, 55: {"zone": "I"},
    57: {"zone": "I"},
    4: {"zone": "IIa"}, 5: {"zone": "IIa"}, 13: {"zone": "IIa"}, 14: {"zone": "IIa"},
    22: {"zone": "IIa"}, 24: {"zone": "IIa"}, 29: {"zone": "IIa"}, 31: {"zone": "IIa"},
    32: {"zone": "IIa"}, 36: {"zone": "IIa"}, 41: {"zone": "IIa"}, 46: {"zone": "IIa"},
    48: {"zone": "IIa"},
    6: {"zone": "IIb"}, 10: {"zone": "IIb"}, 18: {"zone": "IIb"}, 19: {"zone": "IIb"},
    21: {"zone": "IIb"}, 23: {"zone": "IIb"}, 25: {"zone": "IIb"}, 26: {"zone": "IIb"},
    27: {"zone": "IIb"}, 28: {"zone": "IIb"}, 34: {"zone": "IIb"}, 38: {"zone": "IIb"},
    43: {"zone": "IIb"}, 44: {"zone": "IIb"},
    2: {"zone": "III"}, 9: {"zone": "III"}, 15: {"zone": "III"}, 16: {"zone": "III"},
    35: {"zone": "III"}, 42: {"zone": "III"},
}

WILAYA_NAMES = {
    1: "ADRAR", 2: "CHLEF", 3: "LAGHOUAT", 4: "OUM EL BOUAGHI", 5: "BATNA",
    6: "BEJAIA", 7: "BISKRA", 8: "BECHAR", 9: "BLIDA", 10: "BOUIRA",
    11: "TAMANRASSET", 12: "TEBESSA", 13: "TLEMCEN", 14: "TIARET", 15: "TIZI OUZOU",
    16: "ALGER", 17: "DJELFA", 18: "JIJEL", 19: "SETIF", 20: "SAIDA",
    21: "SKIKDA", 22: "SIDI BEL ABBES", 23: "ANNABA", 24: "GUELMA", 25: "CONSTANTINE",
    26: "MEDEA", 27: "MOSTAGANEM", 28: "M'SILA", 29: "MASCARA", 30: "OUARGLA",
    31: "ORAN", 32: "EL BAYADH", 33: "ILLIZI", 34: "B.B. ARRERIDJ", 35: "BOUMERDES",
    36: "EL TAREF", 37: "TINDOUF", 38: "TISSEMSILT", 39: "EL OUED", 40: "KHENCHELA",
    41: "SOUK AHRAS", 42: "TIPAZA", 43: "MILA", 44: "AIN DEFLA", 45: "NAAMA",
    46: "AIN TIMOUCHENT", 47: "GHARDAIA", 48: "RELIZANE", 49: "TIMIMOUN",
    50: "BORDJ BADJI MOKHTAR", 51: "OULED DJELLAL", 52: "BORDJ BAJI MOKHTAR S",
    53: "IN SALAH", 54: "IN GUEZZAM", 55: "TOUGGOURT", 56: "DJANET",
    57: "EL MGHAIR", 58: "EL MENIAA"
}

# ============================================================================
# 2. ESTIMATION PARAMETERS
# ============================================================================
# Since VALEUR_ASSURÉE is 100% missing, we estimate it from PRIME_NETTE
# using standard CATNAT rates in Algeria:
#   Premium = Rate × Insured_Value
#   => Insured_Value = Premium / Rate
# Typical CATNAT rates vary by zone:

CATNAT_RATES = {
    "0":   0.0005,   # 0.05% — very low risk
    "I":   0.00075,  # 0.075%
    "IIa": 0.001,    # 0.10%
    "IIb": 0.0015,   # 0.15%
    "III": 0.002,    # 0.20% — highest risk
    "NC":  0.001,    # default
}

# Damage ratios by zone (Mean Damage Ratio for PML estimation)
# Based on historical earthquake loss data and RPA guidelines
DAMAGE_RATIOS = {
    "0":   {"mdr_50yr": 0.00, "mdr_100yr": 0.01, "mdr_250yr": 0.02, "mdr_475yr": 0.05},
    "I":   {"mdr_50yr": 0.01, "mdr_100yr": 0.03, "mdr_250yr": 0.06, "mdr_475yr": 0.10},
    "IIa": {"mdr_50yr": 0.02, "mdr_100yr": 0.05, "mdr_250yr": 0.10, "mdr_475yr": 0.18},
    "IIb": {"mdr_50yr": 0.04, "mdr_100yr": 0.08, "mdr_250yr": 0.15, "mdr_475yr": 0.25},
    "III": {"mdr_50yr": 0.06, "mdr_100yr": 0.12, "mdr_250yr": 0.22, "mdr_475yr": 0.35},
    "NC":  {"mdr_50yr": 0.03, "mdr_100yr": 0.06, "mdr_250yr": 0.12, "mdr_475yr": 0.20},
}

# Vulnerability amplification by construction type
VULN_AMPLIFIERS = {
    "Installation Industrielle": 1.4,
    "Installation Commerciale": 1.1,
    "NULL": 1.0,
}

# Hypothetical company retention capacity (DZD)
COMPANY_RETENTION = 500_000_000  # 500 million DZD

# ============================================================================
# 3. DATA EXTRACTION
# ============================================================================

def extract_wilaya_code(wilaya_str):
    if wilaya_str is None or wilaya_str == 'NULL':
        return None
    match = re.match(r'(\d+)\s*-?\s*', str(wilaya_str).strip())
    return int(match.group(1)) if match else None

def extract_wilaya_name(wilaya_str):
    if wilaya_str is None or wilaya_str == 'NULL':
        return "INCONNU"
    match = re.match(r'\d+\s*-\s*(.*)', str(wilaya_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "INCONNU"
    return str(wilaya_str).strip()

def extract_type_name(type_str):
    if type_str is None or type_str == 'NULL' or str(type_str).strip() == '':
        return "NULL"
    match = re.match(r'\d+\s*-\s*(.*)', str(type_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "NULL"
    return str(type_str).strip()

def extract_commune_name(commune_str):
    if commune_str is None or commune_str == 'NULL':
        return "INCONNUE"
    match = re.match(r'\d+\s*-\s*(.*)', str(commune_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "INCONNUE"
    return str(commune_str).strip()

def get_zone(code):
    if code and code in RPA_ZONES:
        return RPA_ZONES[code]["zone"]
    return "NC"

def estimate_insured_value(prime_nette, zone):
    rate = CATNAT_RATES.get(zone, 0.001)
    if rate == 0:
        return prime_nette * 1000
    return prime_nette / rate

def read_all_data():
    """Read the latest (2025) Excel data"""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(base_dir, "data", "catnat_2025.xlsx")
    print(f"  Reading: {filepath}...")
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    records = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        if row is None or len(row) < 9:
            continue

        wilaya_code = extract_wilaya_code(row[4])
        wilaya_name = extract_wilaya_name(row[4])
        type_name = extract_type_name(row[3])
        commune = extract_commune_name(row[5])
        zone = get_zone(wilaya_code)

        prime = row[8]
        if prime is None or prime == 'NULL':
            prime = 0.0
        else:
            try:
                prime = float(prime)
            except (ValueError, TypeError):
                prime = 0.0

        estimated_si = estimate_insured_value(prime, zone)

        # Normalize type for grouping
        if "industrielle" in type_name.lower():
            type_group = "Installation Industrielle"
            type_group_ar = "منشأة صناعية"
        elif "commerciale" in type_name.lower():
            type_group = "Installation Commerciale"
            type_group_ar = "منشأة تجارية"
        else:
            type_group = "NULL"
            type_group_ar = "غير محدد (افتراضي: عقارات)"

        records.append({
            "wilaya_code": wilaya_code,
            "wilaya_name": wilaya_name,
            "commune": commune,
            "type_name": type_name,
            "type_group": type_group,
            "type_group_ar": type_group_ar,
            "zone": zone,
            "prime_nette": prime,
            "estimated_si": estimated_si,
        })

    wb.close()
    print(f"  => {len(records)} records loaded.")
    return records


# ============================================================================
# 4. ANALYSIS ENGINE
# ============================================================================

def analyze_phase2(records):
    """Perform Phase 2 comprehensive analysis"""
    
    result = {
        "total_policies": len(records),
        "total_prime": sum(r["prime_nette"] for r in records),
        "total_estimated_si": sum(r["estimated_si"] for r in records),
    }

    # ---- A. DATA SEGMENTATION ----
    
    # A1. By Risk Type
    by_type = defaultdict(lambda: {
        "count": 0, "prime": 0.0, "est_si": 0.0, "type_ar": ""
    })
    for r in records:
        by_type[r["type_group"]]["count"] += 1
        by_type[r["type_group"]]["prime"] += r["prime_nette"]
        by_type[r["type_group"]]["est_si"] += r["estimated_si"]
        by_type[r["type_group"]]["type_ar"] = r["type_group_ar"]
    result["by_type"] = dict(by_type)

    # A2. By RPA Zone
    by_zone = defaultdict(lambda: {
        "count": 0, "prime": 0.0, "est_si": 0.0, "wilayas": set(), "communes": set()
    })
    for r in records:
        by_zone[r["zone"]]["count"] += 1
        by_zone[r["zone"]]["prime"] += r["prime_nette"]
        by_zone[r["zone"]]["est_si"] += r["estimated_si"]
        by_zone[r["zone"]]["wilayas"].add(r["wilaya_name"])
        by_zone[r["zone"]]["communes"].add(r["commune"])
    result["by_zone"] = dict(by_zone)

    # A3. By Wilaya
    by_wilaya = defaultdict(lambda: {
        "count": 0, "prime": 0.0, "est_si": 0.0, "zone": "", "communes": set(),
        "types": defaultdict(int), "code": None
    })
    for r in records:
        key = r["wilaya_name"]
        by_wilaya[key]["count"] += 1
        by_wilaya[key]["prime"] += r["prime_nette"]
        by_wilaya[key]["est_si"] += r["estimated_si"]
        by_wilaya[key]["zone"] = r["zone"]
        by_wilaya[key]["communes"].add(r["commune"])
        by_wilaya[key]["types"][r["type_group"]] += 1
        by_wilaya[key]["code"] = r["wilaya_code"]
    result["by_wilaya"] = dict(by_wilaya)

    # A4. By Commune
    by_commune = defaultdict(lambda: {
        "count": 0, "prime": 0.0, "est_si": 0.0, "wilaya": "", "zone": "",
        "wilaya_code": None
    })
    for r in records:
        key = f"{r['commune']}|{r['wilaya_name']}"
        by_commune[key]["count"] += 1
        by_commune[key]["prime"] += r["prime_nette"]
        by_commune[key]["est_si"] += r["estimated_si"]
        by_commune[key]["wilaya"] = r["wilaya_name"]
        by_commune[key]["zone"] = r["zone"]
        by_commune[key]["wilaya_code"] = r["wilaya_code"]
    result["by_commune"] = dict(by_commune)

    # A5. Zone × Type Cross Matrix
    zone_type = defaultdict(lambda: defaultdict(lambda: {
        "count": 0, "prime": 0.0, "est_si": 0.0
    }))
    for r in records:
        zone_type[r["zone"]][r["type_group"]]["count"] += 1
        zone_type[r["zone"]][r["type_group"]]["prime"] += r["prime_nette"]
        zone_type[r["zone"]][r["type_group"]]["est_si"] += r["estimated_si"]
    result["zone_type"] = {z: dict(t) for z, t in zone_type.items()}

    # ---- B. CONCENTRATION ANALYSIS ----

    # B1. Accumulations by Wilaya — identify hotspots
    hotspots_wilaya = []
    for w_name, w_data in sorted(by_wilaya.items(), key=lambda x: -x[1]["est_si"]):
        exceeds = w_data["est_si"] > COMPANY_RETENTION
        hotspots_wilaya.append({
            "name": w_name,
            "code": w_data["code"],
            "zone": w_data["zone"],
            "count": w_data["count"],
            "prime": w_data["prime"],
            "est_si": w_data["est_si"],
            "communes": len(w_data["communes"]),
            "exceeds_retention": exceeds,
            "ratio_to_retention": w_data["est_si"] / COMPANY_RETENTION,
        })
    result["hotspots_wilaya"] = hotspots_wilaya

    # B2. Accumulations by Commune — detailed hotspots
    hotspots_commune = []
    for c_key, c_data in sorted(by_commune.items(), key=lambda x: -x[1]["est_si"]):
        parts = c_key.split("|")
        commune_name = parts[0]
        hotspots_commune.append({
            "commune": commune_name,
            "wilaya": c_data["wilaya"],
            "zone": c_data["zone"],
            "count": c_data["count"],
            "prime": c_data["prime"],
            "est_si": c_data["est_si"],
        })
    result["hotspots_commune"] = hotspots_commune[:50]

    # B3. Probable Maximum Loss (PML) Simulation
    pml_scenarios = []
    return_periods = [50, 100, 250, 475]

    for rp in return_periods:
        mdr_key = f"mdr_{rp}yr"
        total_loss = 0
        zone_losses = {}

        for zone_name, z_data in by_zone.items():
            dr = DAMAGE_RATIOS.get(zone_name, DAMAGE_RATIOS["NC"])
            mdr = dr.get(mdr_key, 0.1)
            zone_si = z_data["est_si"]
            zone_loss = zone_si * mdr
            zone_losses[zone_name] = {
                "est_si": zone_si,
                "mdr": mdr,
                "loss": zone_loss,
            }
            total_loss += zone_loss

        pml_scenarios.append({
            "return_period": rp,
            "total_loss": total_loss,
            "loss_ratio": total_loss / result["total_estimated_si"] if result["total_estimated_si"] > 0 else 0,
            "exceeds_retention": total_loss > COMPANY_RETENTION,
            "ratio_to_retention": total_loss / COMPANY_RETENTION,
            "zone_losses": zone_losses,
        })
    result["pml_scenarios"] = pml_scenarios

    # B4. Targeted PML: Earthquake in Zone III only (worst case)
    zone3_si = by_zone.get("III", {}).get("est_si", 0)
    targeted_pml = []
    for rp in return_periods:
        mdr = DAMAGE_RATIOS["III"][f"mdr_{rp}yr"]
        loss = zone3_si * mdr
        targeted_pml.append({
            "return_period": rp,
            "zone": "III",
            "exposed_si": zone3_si,
            "mdr": mdr,
            "estimated_loss": loss,
            "ratio_to_retention": loss / COMPANY_RETENTION,
        })
    result["targeted_pml_zone3"] = targeted_pml

    # B5. Top 5 Wilaya PML (individual worst-case)
    top5_wilaya_pml = []
    for hw in hotspots_wilaya[:5]:
        zone = hw["zone"]
        si = hw["est_si"]
        mdr_250 = DAMAGE_RATIOS.get(zone, DAMAGE_RATIOS["NC"]).get("mdr_250yr", 0.12)
        mdr_475 = DAMAGE_RATIOS.get(zone, DAMAGE_RATIOS["NC"]).get("mdr_475yr", 0.20)
        top5_wilaya_pml.append({
            "wilaya": hw["name"],
            "zone": zone,
            "est_si": si,
            "loss_250yr": si * mdr_250,
            "loss_475yr": si * mdr_475,
            "ratio_250yr": (si * mdr_250) / COMPANY_RETENTION,
            "ratio_475yr": (si * mdr_475) / COMPANY_RETENTION,
        })
    result["top5_wilaya_pml"] = top5_wilaya_pml

    return result


# ============================================================================
# 5. HTML DASHBOARD GENERATION
# ============================================================================

def fmt(n, decimals=0):
    """Format number with thousands separator"""
    if decimals == 0:
        return f"{n:,.0f}"
    return f"{n:,.{decimals}f}"

def fmt_pct(n):
    return f"{n:.1f}%"

def fmt_billions(n):
    """Format large numbers in billions"""
    if abs(n) >= 1e9:
        return f"{n/1e9:.2f} مليار"
    if abs(n) >= 1e6:
        return f"{n/1e6:.1f} مليون"
    return fmt(n)


def generate_dashboard(analysis, output_path):
    """Generate Phase 2 interactive HTML dashboard"""

    zones_order = ["0", "I", "IIa", "IIb", "III", "NC"]
    zone_colors = {
        "0": "#2ecc71", "I": "#f1c40f", "IIa": "#e67e22",
        "IIb": "#e74c3c", "III": "#8e44ad", "NC": "#95a5a6"
    }
    zone_labels_ar = {
        "0": "Zone 0 - ضعيفة جدًا", "I": "Zone I - ضعيفة", "IIa": "Zone IIa - متوسطة",
        "IIb": "Zone IIb - مرتفعة", "III": "Zone III - مرتفعة جدًا", "NC": "غير مصنف"
    }
    type_labels = {
        "Installation Industrielle": "منشأة صناعية",
        "Installation Commerciale": "منشأة تجارية",
        "NULL": "غير محدد"
    }

    # Prepare chart data
    zone_si_data = []
    zone_prime_data = []
    zone_count_data = []
    zone_labels_list = []
    zone_colors_list = []
    for z in zones_order:
        zd = analysis["by_zone"].get(z, {"count": 0, "prime": 0, "est_si": 0})
        zone_si_data.append(round(zd.get("est_si", 0), 0))
        zone_prime_data.append(round(zd.get("prime", 0), 0))
        zone_count_data.append(zd.get("count", 0))
        zone_labels_list.append(zone_labels_ar.get(z, z))
        zone_colors_list.append(zone_colors.get(z, "#999"))

    # Type data
    type_names_chart = []
    type_si_chart = []
    type_prime_chart = []
    type_count_chart = []
    type_colors_chart = ["#3b82f6", "#f59e0b", "#6b7280"]
    for tg in ["Installation Industrielle", "Installation Commerciale", "NULL"]:
        td = analysis["by_type"].get(tg, {"count": 0, "prime": 0, "est_si": 0})
        type_names_chart.append(type_labels.get(tg, tg))
        type_si_chart.append(round(td.get("est_si", 0), 0))
        type_prime_chart.append(round(td.get("prime", 0), 0))
        type_count_chart.append(td.get("count", 0))

    # Zone × Type matrix data
    matrix_zones = []
    matrix_types = list(type_labels.keys())
    matrix_data = {}
    for z in zones_order:
        zt = analysis["zone_type"].get(z, {})
        for tg in matrix_types:
            td = zt.get(tg, {"count": 0, "est_si": 0, "prime": 0})
            if z not in matrix_data:
                matrix_data[z] = {}
            matrix_data[z][tg] = td

    # Top wilayas for chart
    top_wilayas = analysis["hotspots_wilaya"][:15]
    wilaya_names_chart = [w["name"] for w in top_wilayas]
    wilaya_si_chart = [round(w["est_si"], 0) for w in top_wilayas]
    wilaya_colors_chart = [zone_colors.get(w["zone"], "#999") for w in top_wilayas]

    # Top communes for chart
    top_communes = analysis["hotspots_commune"][:15]
    commune_names_chart = [f"{c['commune']} ({c['wilaya']})" for c in top_communes]
    commune_si_chart = [round(c["est_si"], 0) for c in top_communes]
    commune_colors_chart = [zone_colors.get(c["zone"], "#999") for c in top_communes]

    # PML data
    pml_rp = [s["return_period"] for s in analysis["pml_scenarios"]]
    pml_total_loss = [round(s["total_loss"], 0) for s in analysis["pml_scenarios"]]
    pml_ratio = [round(s["ratio_to_retention"] * 100, 1) for s in analysis["pml_scenarios"]]

    # Pre-compute KPI values
    total_policies = analysis["total_policies"]
    total_prime = analysis["total_prime"]
    total_est_si = analysis["total_estimated_si"]
    high_risk_si = analysis["by_zone"].get("III", {}).get("est_si", 0) + analysis["by_zone"].get("IIb", {}).get("est_si", 0)
    high_risk_pct = round(high_risk_si / total_est_si * 100, 1) if total_est_si > 0 else 0
    num_hotspot_wilayas = sum(1 for w in analysis["hotspots_wilaya"] if w["exceeds_retention"])
    pml_475 = analysis["pml_scenarios"][-1]["total_loss"]
    pml_475_ratio = analysis["pml_scenarios"][-1]["ratio_to_retention"]
    retention_str = fmt_billions(COMPANY_RETENTION)

    # Count hotspot wilayas exceeding retention
    exceeding_wilayas = [w for w in analysis["hotspots_wilaya"] if w["exceeds_retention"]]

    html = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>المرحلة الثانية: تحليل المحفظة التأمينية</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@300;400;600;700;800;900&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #0b0f1e;
            --bg-secondary: #111827;
            --bg-card: #1a2235;
            --bg-card-alt: #1e2a42;
            --text-primary: #e8edf5;
            --text-secondary: #8b95a8;
            --text-muted: #5a6478;
            --accent-blue: #3b82f6;
            --accent-purple: #8b5cf6;
            --accent-green: #10b981;
            --accent-amber: #f59e0b;
            --accent-red: #ef4444;
            --accent-orange: #f97316;
            --accent-cyan: #06b6d4;
            --border-color: rgba(255,255,255,0.06);
            --radius: 16px;
            --radius-sm: 10px;
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Cairo', 'Inter', sans-serif;
            background: var(--bg-primary);
            color: var(--text-primary);
            min-height: 100vh;
            direction: rtl;
        }}
        body::before {{
            content: '';
            position: fixed;
            inset: 0;
            background:
                radial-gradient(ellipse 600px 400px at 15% 15%, rgba(139,92,246,0.08), transparent),
                radial-gradient(ellipse 500px 400px at 85% 75%, rgba(59,130,246,0.06), transparent),
                radial-gradient(ellipse 400px 300px at 50% 40%, rgba(239,68,68,0.04), transparent);
            z-index: -1;
        }}
        .container {{ max-width: 1500px; margin: 0 auto; padding: 25px 35px; }}

        /* Header */
        .header {{
            text-align: center;
            margin-bottom: 35px;
            padding: 35px 40px;
            background: linear-gradient(135deg, rgba(139,92,246,0.12), rgba(59,130,246,0.08));
            border-radius: var(--radius);
            border: 1px solid var(--border-color);
            position: relative;
            overflow: hidden;
        }}
        .header::after {{
            content: '';
            position: absolute;
            inset: -50%;
            background: conic-gradient(from 0deg, transparent, rgba(139,92,246,0.03), transparent, rgba(59,130,246,0.02), transparent);
            animation: spin 25s linear infinite;
        }}
        @keyframes spin {{ 100% {{ transform: rotate(360deg); }} }}
        .header-content {{ position: relative; z-index: 1; }}
        .header h1 {{
            font-size: 2.1rem; font-weight: 800;
            background: linear-gradient(135deg, #8b5cf6, #3b82f6, #06b6d4);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
            margin-bottom: 10px; line-height: 1.5;
        }}
        .header .subtitle {{ font-size: 1rem; color: var(--text-secondary); }}
        .header .badge {{
            display: inline-block; background: rgba(139,92,246,0.15); color: var(--accent-purple);
            padding: 6px 18px; border-radius: 30px; font-size: 0.82rem; font-weight: 600;
            margin-top: 12px; border: 1px solid rgba(139,92,246,0.25);
        }}

        /* Tabs */
        .nav-tabs {{
            display: flex; gap: 6px; margin-bottom: 28px; padding: 5px;
            background: var(--bg-secondary); border-radius: var(--radius);
            border: 1px solid var(--border-color); overflow-x: auto;
        }}
        .nav-tab {{
            padding: 11px 22px; border-radius: var(--radius-sm); cursor: pointer;
            font-weight: 600; font-size: 0.88rem; color: var(--text-secondary);
            transition: all 0.3s; white-space: nowrap; border: none; background: none;
            font-family: inherit;
        }}
        .nav-tab:hover {{ color: var(--text-primary); background: rgba(255,255,255,0.04); }}
        .nav-tab.active {{
            background: linear-gradient(135deg, var(--accent-purple), var(--accent-blue));
            color: white; box-shadow: 0 4px 15px rgba(139,92,246,0.3);
        }}
        .tab-content {{ display: none; animation: fadeIn 0.4s ease; }}
        .tab-content.active {{ display: block; }}
        @keyframes fadeIn {{ from {{ opacity: 0; transform: translateY(8px); }} to {{ opacity: 1; transform: translateY(0); }} }}

        /* KPI Cards */
        .kpi-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
            gap: 18px; margin-bottom: 28px;
        }}
        .kpi-card {{
            background: var(--bg-card); border-radius: var(--radius); padding: 22px;
            border: 1px solid var(--border-color); transition: all 0.3s;
        }}
        .kpi-card:hover {{ transform: translateY(-2px); box-shadow: 0 6px 25px rgba(0,0,0,0.3); }}
        .kpi-icon {{ width: 44px; height: 44px; border-radius: 11px; display: flex; align-items: center; justify-content: center; font-size: 1.3rem; margin-bottom: 13px; }}
        .kpi-value {{ font-size: 1.6rem; font-weight: 800; margin-bottom: 4px; line-height: 1.2; }}
        .kpi-label {{ color: var(--text-secondary); font-size: 0.82rem; font-weight: 500; }}
        .kpi-sub {{ font-size: 0.75rem; margin-top: 6px; padding: 3px 10px; border-radius: 20px; display: inline-block; }}
        .kpi-sub.danger {{ background: rgba(239,68,68,0.15); color: var(--accent-red); }}
        .kpi-sub.warning {{ background: rgba(245,158,11,0.15); color: var(--accent-amber); }}
        .kpi-sub.good {{ background: rgba(16,185,129,0.15); color: var(--accent-green); }}

        /* Charts */
        .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(480px, 1fr)); gap: 22px; margin-bottom: 28px; }}
        .chart-card {{ background: var(--bg-card); border-radius: var(--radius); padding: 25px; border: 1px solid var(--border-color); }}
        .chart-card h3 {{ font-size: 1.05rem; font-weight: 700; margin-bottom: 18px; display: flex; align-items: center; gap: 10px; }}
        .chart-card h3 .icon {{ width: 30px; height: 30px; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 0.95rem; }}
        .chart-wrapper {{ position: relative; height: 340px; }}
        .chart-wrapper.tall {{ height: 440px; }}
        .chart-wrapper.short {{ height: 280px; }}

        /* Tables */
        .table-card {{ background: var(--bg-card); border-radius: var(--radius); padding: 25px; border: 1px solid var(--border-color); margin-bottom: 22px; overflow-x: auto; }}
        .table-card h3 {{ font-size: 1.05rem; font-weight: 700; margin-bottom: 18px; display: flex; align-items: center; gap: 10px; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
        th {{ background: rgba(139,92,246,0.1); color: var(--accent-purple); font-weight: 700; padding: 12px 14px; text-align: right; white-space: nowrap; border-bottom: 2px solid rgba(139,92,246,0.2); }}
        td {{ padding: 11px 14px; border-bottom: 1px solid var(--border-color); }}
        tr:hover td {{ background: rgba(255,255,255,0.02); }}

        /* Zone badges */
        .zb {{ display: inline-block; padding: 3px 12px; border-radius: 18px; font-size: 0.78rem; font-weight: 700; }}
        .zb-0 {{ background: rgba(46,204,113,0.15); color: #2ecc71; }}
        .zb-I {{ background: rgba(241,196,15,0.15); color: #f1c40f; }}
        .zb-IIa {{ background: rgba(230,126,34,0.15); color: #e67e22; }}
        .zb-IIb {{ background: rgba(231,76,60,0.15); color: #e74c3c; }}
        .zb-III {{ background: rgba(142,68,173,0.15); color: #8e44ad; }}
        .zb-NC {{ background: rgba(149,165,166,0.15); color: #95a5a6; }}

        /* Hotspot flag */
        .hot {{ color: var(--accent-red); font-weight: 700; }}
        .safe {{ color: var(--accent-green); }}

        /* Alert box */
        .alert-box {{
            padding: 18px 22px; border-radius: var(--radius-sm); margin-bottom: 22px;
            border: 1px solid; font-size: 0.9rem; line-height: 1.8;
        }}
        .alert-danger {{ background: rgba(239,68,68,0.08); border-color: rgba(239,68,68,0.2); color: #fca5a5; }}
        .alert-warning {{ background: rgba(245,158,11,0.08); border-color: rgba(245,158,11,0.2); color: #fcd34d; }}
        .alert-info {{ background: rgba(59,130,246,0.08); border-color: rgba(59,130,246,0.2); color: #93c5fd; }}

        /* PML scenario cards */
        .pml-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 18px; margin-bottom: 22px; }}
        .pml-card {{
            background: var(--bg-card); border-radius: var(--radius); padding: 22px;
            border: 1px solid var(--border-color); text-align: center;
        }}
        .pml-card .pml-rp {{ font-size: 0.82rem; color: var(--text-secondary); margin-bottom: 8px; }}
        .pml-card .pml-loss {{ font-size: 1.4rem; font-weight: 800; margin-bottom: 4px; }}
        .pml-card .pml-ratio {{ font-size: 0.85rem; font-weight: 600; }}

        .note {{ font-size: 0.82rem; color: var(--text-muted); line-height: 1.8; padding: 15px; background: rgba(255,255,255,0.02); border-radius: var(--radius-sm); margin-top: 15px; }}

        @media (max-width: 768px) {{
            .container {{ padding: 15px; }}
            .charts-grid {{ grid-template-columns: 1fr; }}
            .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .header h1 {{ font-size: 1.4rem; }}
        }}
    </style>
</head>
<body>
<div class="container">
    <div class="header">
        <div class="header-content">
            <h1>المرحلة الثانية: تحليل المحفظة التأمينية</h1>
            <p class="subtitle">Phase 2: Insurance Portfolio Analysis — Segmentation & Concentration</p>
            <span class="badge">📊 بيانات 2025 | رأس المال المقدر: {fmt_billions(total_est_si)} د.ج</span>
        </div>
    </div>

    <div class="nav-tabs">
        <button class="nav-tab active" onclick="showTab('overview')">📊 نظرة عامة</button>
        <button class="nav-tab" onclick="showTab('segmentation')">📋 تقسيم البيانات</button>
        <button class="nav-tab" onclick="showTab('zone-si')">🗺️ التوزيع الجغرافي</button>
        <button class="nav-tab" onclick="showTab('accumulation')">🔥 تحليل التراكمات</button>
        <button class="nav-tab" onclick="showTab('pml')">⚡ الخسارة القصوى (PML)</button>
        <button class="nav-tab" onclick="showTab('hotspots')">📍 النقاط الساخنة</button>
    </div>

    <!-- ====================== OVERVIEW ====================== -->
    <div id="tab-overview" class="tab-content active">
        <div class="alert-info alert-box">
            ⚠️ <strong>ملاحظة منهجية:</strong> بسبب غياب حقل القيمة المؤمنة (VALEUR_ASSURÉE) بنسبة 100%، تم تقدير رأس المال المعرض للخطر
            من الأقساط الصافية باستخدام معدلات CATNAT المعيارية حسب المنطقة الزلزالية.
            <br>التقديرات: Zone 0 (0.05%) | Zone I (0.075%) | Zone IIa (0.10%) | Zone IIb (0.15%) | Zone III (0.20%)
        </div>

        <div class="kpi-grid">
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(59,130,246,0.15);">📋</div>
                <div class="kpi-value" style="color:var(--accent-blue);">{total_policies:,}</div>
                <div class="kpi-label">إجمالي الوثائق</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(16,185,129,0.15);">💰</div>
                <div class="kpi-value" style="color:var(--accent-green);">{fmt_billions(total_prime)}</div>
                <div class="kpi-label">إجمالي الأقساط الصافية (د.ج)</div>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(139,92,246,0.15);">🏦</div>
                <div class="kpi-value" style="color:var(--accent-purple);">{fmt_billions(total_est_si)}</div>
                <div class="kpi-label">رأس المال المقدر (Sum Insured)</div>
                <span class="kpi-sub warning">تقدير من الأقساط</span>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(239,68,68,0.15);">⚠️</div>
                <div class="kpi-value" style="color:var(--accent-red);">{high_risk_pct}%</div>
                <div class="kpi-label">تركيز في مناطق عالية الخطورة</div>
                <span class="kpi-sub danger">IIb + III</span>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(249,115,22,0.15);">🔥</div>
                <div class="kpi-value" style="color:var(--accent-orange);">{num_hotspot_wilayas}</div>
                <div class="kpi-label">ولايات تتجاوز قدرة الاحتفاظ</div>
                <span class="kpi-sub danger">الحد: {retention_str}</span>
            </div>
            <div class="kpi-card">
                <div class="kpi-icon" style="background:rgba(6,182,212,0.15);">⚡</div>
                <div class="kpi-value" style="color:var(--accent-cyan);">{fmt_billions(pml_475)}</div>
                <div class="kpi-label">PML سيناريو 475 سنة</div>
                <span class="kpi-sub {'danger' if pml_475_ratio > 1 else 'warning'}">{round(pml_475_ratio*100,0):.0f}% من الاحتفاظ</span>
            </div>
        </div>

        <div class="charts-grid">
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(139,92,246,0.15);">🏦</span> توزيع رأس المال المقدر حسب المنطقة</h3>
                <div class="chart-wrapper"><canvas id="overviewZoneSI"></canvas></div>
            </div>
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(59,130,246,0.15);">📊</span> توزيع رأس المال حسب نوع الخطر</h3>
                <div class="chart-wrapper"><canvas id="overviewTypeSI"></canvas></div>
            </div>
        </div>
    </div>

    <!-- ====================== SEGMENTATION ====================== -->
    <div id="tab-segmentation" class="tab-content">
        <div class="table-card">
            <h3><span class="icon" style="background:rgba(59,130,246,0.15);">📋</span> أ. تقسيم حسب طبيعة الخطر</h3>
            <table>
                <thead><tr>
                    <th>نوع الخطر</th><th>عدد الوثائق</th><th>النسبة</th>
                    <th>الأقساط (د.ج)</th><th>رأس المال المقدر (د.ج)</th><th>نسبة رأس المال</th>
                </tr></thead>
                <tbody>
"""

    for tg in ["Installation Industrielle", "Installation Commerciale", "NULL"]:
        td = analysis["by_type"].get(tg, {"count": 0, "prime": 0, "est_si": 0})
        pct = round(td["count"] / total_policies * 100, 1) if total_policies else 0
        si_pct = round(td["est_si"] / total_est_si * 100, 1) if total_est_si else 0
        html += f"""                    <tr>
                        <td><strong>{type_labels.get(tg, tg)}</strong></td>
                        <td>{td['count']:,}</td><td>{pct}%</td>
                        <td>{fmt(td['prime'])}</td>
                        <td>{fmt(td['est_si'])}</td><td>{si_pct}%</td>
                    </tr>
"""
    html += f"""                    <tr style="font-weight:700;background:rgba(139,92,246,0.05);">
                        <td>الإجمالي</td><td>{total_policies:,}</td><td>100%</td>
                        <td>{fmt(total_prime)}</td><td>{fmt(total_est_si)}</td><td>100%</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(139,92,246,0.15);">🗺️</span> ب. تقسيم حسب المنطقة الزلزالية (RPA)</h3>
            <table>
                <thead><tr>
                    <th>المنطقة</th><th>عدد الوثائق</th><th>النسبة</th>
                    <th>الأقساط (د.ج)</th><th>رأس المال المقدر (د.ج)</th>
                    <th>نسبة رأس المال</th><th>عدد الولايات</th>
                </tr></thead>
                <tbody>
"""

    for z in zones_order:
        zd = analysis["by_zone"].get(z, {"count": 0, "prime": 0, "est_si": 0, "wilayas": set()})
        pct = round(zd["count"] / total_policies * 100, 1) if total_policies else 0
        si_pct = round(zd["est_si"] / total_est_si * 100, 1) if total_est_si else 0
        n_wil = len(zd.get("wilayas", set())) if isinstance(zd.get("wilayas"), set) else 0
        html += f"""                    <tr>
                        <td><span class="zb zb-{z}">{zone_labels_ar.get(z, z)}</span></td>
                        <td>{zd['count']:,}</td><td>{pct}%</td>
                        <td>{fmt(zd['prime'])}</td><td>{fmt(zd['est_si'])}</td>
                        <td>{si_pct}%</td><td>{n_wil}</td>
                    </tr>
"""

    html += f"""                    <tr style="font-weight:700;background:rgba(139,92,246,0.05);">
                        <td>الإجمالي</td><td>{total_policies:,}</td><td>100%</td>
                        <td>{fmt(total_prime)}</td><td>{fmt(total_est_si)}</td><td>100%</td><td>—</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(249,115,22,0.15);">📊</span> ج. المصفوفة: المنطقة الزلزالية × نوع الخطر (عدد الوثائق)</h3>
            <table>
                <thead><tr>
                    <th>المنطقة</th><th>صناعية</th><th>تجارية</th><th>غير محدد</th><th>الإجمالي</th>
                </tr></thead>
                <tbody>
"""

    for z in zones_order:
        zt = analysis["zone_type"].get(z, {})
        ind = zt.get("Installation Industrielle", {"count": 0})["count"]
        com = zt.get("Installation Commerciale", {"count": 0})["count"]
        nul = zt.get("NULL", {"count": 0})["count"]
        total_z = ind + com + nul
        html += f"""                    <tr>
                        <td><span class="zb zb-{z}">{z}</span></td>
                        <td>{ind:,}</td><td>{com:,}</td><td>{nul:,}</td><td><strong>{total_z:,}</strong></td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="charts-grid">
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(59,130,246,0.15);">📊</span> مقارنة: الأقساط vs رأس المال المقدر</h3>
                <div class="chart-wrapper"><canvas id="segCompareChart"></canvas></div>
            </div>
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(249,115,22,0.15);">📊</span> المصفوفة البصرية: منطقة × نوع (رأس المال)</h3>
                <div class="chart-wrapper"><canvas id="segMatrixChart"></canvas></div>
            </div>
        </div>
    </div>

    <!-- ====================== GEOGRAPHIC DISTRIBUTION ====================== -->
    <div id="tab-zone-si" class="tab-content">
        <div class="table-card">
            <h3><span class="icon" style="background:rgba(139,92,246,0.15);">🏛️</span> رأس المال المعرض للخطر حسب الولاية (أعلى 25)</h3>
            <table>
                <thead><tr>
                    <th>#</th><th>الولاية</th><th>المنطقة</th><th>عدد الوثائق</th>
                    <th>الأقساط (د.ج)</th><th>رأس المال المقدر (د.ج)</th>
                    <th>% من الإجمالي</th><th>بلديات</th><th>حالة التراكم</th>
                </tr></thead>
                <tbody>
"""

    for i, hw in enumerate(analysis["hotspots_wilaya"][:25], 1):
        si_pct = round(hw["est_si"] / total_est_si * 100, 1) if total_est_si else 0
        status = '<span class="hot">🔴 يتجاوز الاحتفاظ</span>' if hw["exceeds_retention"] else '<span class="safe">🟢 ضمن الحدود</span>'
        html += f"""                    <tr>
                        <td>{i}</td><td><strong>{hw['name']}</strong></td>
                        <td><span class="zb zb-{hw['zone']}">{hw['zone']}</span></td>
                        <td>{hw['count']:,}</td><td>{fmt(hw['prime'])}</td>
                        <td>{fmt(hw['est_si'])}</td><td>{si_pct}%</td>
                        <td>{hw['communes']}</td><td>{status}</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="chart-card" style="margin-bottom:22px;">
            <h3><span class="icon" style="background:rgba(139,92,246,0.15);">📊</span> رأس المال المقدر — أكبر 15 ولاية</h3>
            <div class="chart-wrapper tall"><canvas id="wilayaSIChart"></canvas></div>
        </div>
    </div>

    <!-- ====================== ACCUMULATION ====================== -->
    <div id="tab-accumulation" class="tab-content">
"""

    if exceeding_wilayas:
        html += f"""        <div class="alert-danger alert-box">
            🚨 <strong>تنبيه — تجاوز قدرة الاحتفاظ:</strong> تم تحديد <strong>{len(exceeding_wilayas)} ولاية</strong>
            يتجاوز فيها رأس المال المقدر قدرة احتفاظ الشركة ({retention_str} د.ج).
            هذه الولايات تمثل نقاط تركيز مفرط تستوجب إعادة توزيع أو ترتيبات إعادة التأمين.
        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(239,68,68,0.15);">🔴</span> الولايات المتجاوزة لقدرة الاحتفاظ ({retention_str} د.ج)</h3>
            <table>
                <thead><tr>
                    <th>#</th><th>الولاية</th><th>المنطقة</th><th>رأس المال المقدر</th>
                    <th>نسبة التجاوز</th><th>المبلغ الفائض</th><th>التوصية</th>
                </tr></thead>
                <tbody>
"""
        for i, ew in enumerate(exceeding_wilayas, 1):
            excess = ew["est_si"] - COMPANY_RETENTION
            ratio_str = f"{ew['ratio_to_retention']:.1f}x"
            if ew["ratio_to_retention"] > 5:
                rec = "إعادة تأمين عاجلة + تحديد سقف"
            elif ew["ratio_to_retention"] > 2:
                rec = "إعادة تأمين مع مراجعة التسعير"
            else:
                rec = "مراقبة + إعادة تأمين اختيارية"
            html += f"""                    <tr>
                        <td>{i}</td><td><strong>{ew['name']}</strong></td>
                        <td><span class="zb zb-{ew['zone']}">{ew['zone']}</span></td>
                        <td>{fmt(ew['est_si'])}</td>
                        <td class="hot">{ratio_str}</td>
                        <td class="hot">{fmt(excess)}</td>
                        <td>{rec}</td>
                    </tr>
"""
        html += """                </tbody>
            </table>
        </div>
"""
    else:
        html += """        <div class="alert-info alert-box">
            ✅ لا توجد ولايات تتجاوز قدرة الاحتفاظ الحالية. المحفظة ضمن الحدود المقبولة.
        </div>
"""

    html += """
        <div class="chart-card">
            <h3><span class="icon" style="background:rgba(239,68,68,0.15);">📊</span> خريطة التراكمات — رأس المال vs حد الاحتفاظ</h3>
            <div class="chart-wrapper tall"><canvas id="accumChart"></canvas></div>
        </div>
    </div>

    <!-- ====================== PML ====================== -->
    <div id="tab-pml" class="tab-content">
        <div class="alert-warning alert-box">
            📐 <strong>منهجية PML:</strong> تم تقدير الخسارة القصوى المحتملة باستخدام نسب الأضرار المتوسطة (MDR)
            لكل منطقة زلزالية، مطبقة على رأس المال المقدر. تم اعتماد أربع فترات عودة: 50، 100، 250، و475 سنة.
        </div>

        <div class="pml-grid">
"""

    pml_colors = ["var(--accent-green)", "var(--accent-amber)", "var(--accent-orange)", "var(--accent-red)"]
    for i, sc in enumerate(analysis["pml_scenarios"]):
        loss_str = fmt_billions(sc["total_loss"])
        ratio_str = f"{sc['ratio_to_retention']*100:.0f}%"
        exceed = "🔴 يتجاوز" if sc["exceeds_retention"] else "🟢 ضمن الحدود"
        html += f"""            <div class="pml-card" style="border-top: 3px solid {pml_colors[i]};">
                <div class="pml-rp">فترة العودة: <strong>{sc['return_period']} سنة</strong></div>
                <div class="pml-loss" style="color:{pml_colors[i]};">{loss_str} د.ج</div>
                <div class="pml-ratio">{ratio_str} من قدرة الاحتفاظ</div>
                <div style="margin-top:8px;font-size:0.82rem;">{exceed}</div>
            </div>
"""

    html += """        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(239,68,68,0.15);">📊</span> تفصيل PML حسب المنطقة الزلزالية (سيناريو 475 سنة)</h3>
            <table>
                <thead><tr>
                    <th>المنطقة</th><th>رأس المال المعرض</th><th>نسبة الأضرار (MDR)</th>
                    <th>الخسارة المقدرة</th><th>% من إجمالي الخسارة</th>
                </tr></thead>
                <tbody>
"""

    pml_475_data = analysis["pml_scenarios"][-1]
    total_loss_475 = pml_475_data["total_loss"]
    for z in zones_order:
        zl = pml_475_data["zone_losses"].get(z, {"est_si": 0, "mdr": 0, "loss": 0})
        loss_pct = round(zl["loss"] / total_loss_475 * 100, 1) if total_loss_475 > 0 else 0
        html += f"""                    <tr>
                        <td><span class="zb zb-{z}">{zone_labels_ar.get(z, z)}</span></td>
                        <td>{fmt(zl['est_si'])}</td>
                        <td>{zl['mdr']*100:.0f}%</td>
                        <td><strong>{fmt(zl['loss'])}</strong></td>
                        <td>{loss_pct}%</td>
                    </tr>
"""

    html += f"""                    <tr style="font-weight:700;background:rgba(239,68,68,0.05);">
                        <td>الإجمالي</td><td>{fmt(total_est_si)}</td><td>—</td>
                        <td><strong>{fmt(total_loss_475)}</strong></td><td>100%</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(142,68,173,0.15);">🎯</span> PML مستهدف — زلزال كبير في Zone III فقط</h3>
            <table>
                <thead><tr>
                    <th>فترة العودة</th><th>رأس المال المعرض (Zone III)</th>
                    <th>MDR</th><th>الخسارة المقدرة</th><th>نسبة من الاحتفاظ</th>
                </tr></thead>
                <tbody>
"""

    for tp in analysis["targeted_pml_zone3"]:
        html += f"""                    <tr>
                        <td><strong>{tp['return_period']} سنة</strong></td>
                        <td>{fmt(tp['exposed_si'])}</td>
                        <td>{tp['mdr']*100:.0f}%</td>
                        <td><strong>{fmt(tp['estimated_loss'])}</strong></td>
                        <td>{tp['ratio_to_retention']*100:.0f}%</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="table-card">
            <h3><span class="icon" style="background:rgba(249,115,22,0.15);">🏛️</span> PML حسب أكبر 5 ولايات (سيناريو 250 و 475 سنة)</h3>
            <table>
                <thead><tr>
                    <th>الولاية</th><th>المنطقة</th><th>رأس المال المقدر</th>
                    <th>خسارة 250 سنة</th><th>% احتفاظ</th>
                    <th>خسارة 475 سنة</th><th>% احتفاظ</th>
                </tr></thead>
                <tbody>
"""

    for wp in analysis["top5_wilaya_pml"]:
        html += f"""                    <tr>
                        <td><strong>{wp['wilaya']}</strong></td>
                        <td><span class="zb zb-{wp['zone']}">{wp['zone']}</span></td>
                        <td>{fmt(wp['est_si'])}</td>
                        <td>{fmt(wp['loss_250yr'])}</td>
                        <td>{wp['ratio_250yr']*100:.0f}%</td>
                        <td class="hot">{fmt(wp['loss_475yr'])}</td>
                        <td class="hot">{wp['ratio_475yr']*100:.0f}%</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="charts-grid">
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(239,68,68,0.15);">📈</span> تطور PML حسب فترة العودة</h3>
                <div class="chart-wrapper"><canvas id="pmlTrendChart"></canvas></div>
            </div>
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(142,68,173,0.15);">📊</span> توزيع خسائر PML-475 حسب المنطقة</h3>
                <div class="chart-wrapper"><canvas id="pmlZoneChart"></canvas></div>
            </div>
        </div>
    </div>

    <!-- ====================== HOTSPOTS ====================== -->
    <div id="tab-hotspots" class="tab-content">
        <div class="table-card">
            <h3><span class="icon" style="background:rgba(239,68,68,0.15);">📍</span> النقاط الساخنة — أكبر 30 بلدية من حيث رأس المال المقدر</h3>
            <table>
                <thead><tr>
                    <th>#</th><th>البلدية</th><th>الولاية</th><th>المنطقة</th>
                    <th>عدد الوثائق</th><th>الأقساط (د.ج)</th><th>رأس المال المقدر (د.ج)</th>
                </tr></thead>
                <tbody>
"""

    for i, hc in enumerate(analysis["hotspots_commune"][:30], 1):
        html += f"""                    <tr>
                        <td>{i}</td>
                        <td><strong>{hc['commune']}</strong></td>
                        <td>{hc['wilaya']}</td>
                        <td><span class="zb zb-{hc['zone']}">{hc['zone']}</span></td>
                        <td>{hc['count']:,}</td>
                        <td>{fmt(hc['prime'])}</td>
                        <td>{fmt(hc['est_si'])}</td>
                    </tr>
"""

    html += """                </tbody>
            </table>
        </div>

        <div class="chart-card">
            <h3><span class="icon" style="background:rgba(249,115,22,0.15);">📊</span> أكبر 15 بلدية — رأس المال المقدر</h3>
            <div class="chart-wrapper tall"><canvas id="communeSIChart"></canvas></div>
        </div>
    </div>
</div>
"""

    # ====================== JAVASCRIPT ======================
    html += f"""
<script>
    function showTab(id) {{
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
        document.getElementById('tab-' + id).classList.add('active');
        event.target.classList.add('active');
    }}

    Chart.defaults.color = '#8b95a8';
    Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
    Chart.defaults.font.family = "'Cairo','Inter',sans-serif";

    const zLabels = {json.dumps(zone_labels_list)};
    const zColors = {json.dumps(zone_colors_list)};
    const zSI = {json.dumps(zone_si_data)};
    const zPrime = {json.dumps(zone_prime_data)};
    const zCount = {json.dumps(zone_count_data)};

    // Overview - Zone SI doughnut
    new Chart(document.getElementById('overviewZoneSI'), {{
        type: 'doughnut',
        data: {{
            labels: zLabels,
            datasets: [{{ data: zSI, backgroundColor: zColors, borderWidth: 2, borderColor: '#1a2235', hoverOffset: 12 }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false, cutout: '50%',
            plugins: {{
                legend: {{ position: 'bottom', labels: {{ padding: 12, usePointStyle: true }} }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + (ctx.raw/1e6).toFixed(1) + ' M DA' }} }}
            }}
        }}
    }});

    // Overview - Type SI doughnut
    new Chart(document.getElementById('overviewTypeSI'), {{
        type: 'doughnut',
        data: {{
            labels: {json.dumps(type_names_chart)},
            datasets: [{{ data: {json.dumps(type_si_chart)}, backgroundColor: {json.dumps(type_colors_chart)}, borderWidth: 2, borderColor: '#1a2235', hoverOffset: 12 }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false, cutout: '50%',
            plugins: {{
                legend: {{ position: 'bottom', labels: {{ padding: 12, usePointStyle: true }} }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + (ctx.raw/1e6).toFixed(1) + ' M DA' }} }}
            }}
        }}
    }});

    // Segmentation - Compare bar
    new Chart(document.getElementById('segCompareChart'), {{
        type: 'bar',
        data: {{
            labels: zLabels,
            datasets: [
                {{ label: 'الأقساط (M DA)', data: zPrime.map(v => v/1e6), backgroundColor: 'rgba(59,130,246,0.7)', borderRadius: 6, barPercentage: 0.4 }},
                {{ label: 'رأس المال المقدر (M DA)', data: zSI.map(v => v/1e6), backgroundColor: 'rgba(139,92,246,0.7)', borderRadius: 6, barPercentage: 0.4 }}
            ]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
            scales: {{ y: {{ beginAtZero: true, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}, x: {{ grid: {{ display: false }} }} }}
        }}
    }});

    // Segmentation - Matrix stacked bar (SI by zone × type)
    const matrixData = {json.dumps({z: {type_labels.get(t,t): d.get("est_si",0) for t,d in analysis["zone_type"].get(z, {}).items()} for z in zones_order})};
    const allTypes = {json.dumps(list(type_labels.values()))};
    const tColors = ['#3b82f6', '#f59e0b', '#6b7280'];
    new Chart(document.getElementById('segMatrixChart'), {{
        type: 'bar',
        data: {{
            labels: zLabels,
            datasets: allTypes.map((t, i) => ({{
                label: t,
                data: {json.dumps(zones_order)}.map(z => (matrixData[z] && matrixData[z][t]) ? matrixData[z][t]/1e6 : 0),
                backgroundColor: tColors[i] + '99',
                borderColor: tColors[i],
                borderWidth: 1,
                borderRadius: 4
            }}))
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
            scales: {{ x: {{ stacked: true, grid: {{ display: false }} }}, y: {{ stacked: true, beginAtZero: true, title: {{ display: true, text: 'M DA' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }} }}
        }}
    }});

    // Wilaya SI horizontal bar
    new Chart(document.getElementById('wilayaSIChart'), {{
        type: 'bar',
        data: {{
            labels: {json.dumps(wilaya_names_chart)},
            datasets: [{{
                label: 'رأس المال المقدر (M DA)',
                data: {json.dumps(wilaya_si_chart)}.map(v => v/1e6),
                backgroundColor: {json.dumps([c + 'cc' for c in wilaya_colors_chart])},
                borderColor: {json.dumps(wilaya_colors_chart)},
                borderWidth: 2, borderRadius: 6
            }}]
        }},
        options: {{
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.raw.toFixed(1) + ' M DA' }} }}
            }},
            scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'M DA' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}, y: {{ grid: {{ display: false }} }} }}
        }}
    }});

    // Accumulation chart with retention line
    const accWilayas = {json.dumps([w["name"] for w in analysis["hotspots_wilaya"][:20]])};
    const accSI = {json.dumps([round(w["est_si"]/1e6, 1) for w in analysis["hotspots_wilaya"][:20]])};
    const accColors = {json.dumps([zone_colors.get(w["zone"], "#999") for w in analysis["hotspots_wilaya"][:20]])};
    const retentionLine = {COMPANY_RETENTION / 1e6};

    new Chart(document.getElementById('accumChart'), {{
        type: 'bar',
        data: {{
            labels: accWilayas,
            datasets: [
                {{
                    label: 'رأس المال المقدر (M DA)',
                    data: accSI,
                    backgroundColor: accColors.map(c => c + 'cc'),
                    borderColor: accColors,
                    borderWidth: 2, borderRadius: 6
                }},
                {{
                    label: 'حد الاحتفاظ',
                    data: Array(accWilayas.length).fill(retentionLine),
                    type: 'line',
                    borderColor: '#ef4444',
                    borderWidth: 2,
                    borderDash: [8, 4],
                    pointRadius: 0,
                    fill: false
                }}
            ]
        }},
        options: {{
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
            scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'M DA' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}, y: {{ grid: {{ display: false }} }} }}
        }}
    }});

    // PML Trend
    new Chart(document.getElementById('pmlTrendChart'), {{
        type: 'line',
        data: {{
            labels: {json.dumps([f"{rp} سنة" for rp in pml_rp])},
            datasets: [
                {{
                    label: 'الخسارة الإجمالية (M DA)',
                    data: {json.dumps(pml_total_loss)}.map(v => v/1e6),
                    borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.1)',
                    fill: true, tension: 0.4, pointRadius: 6, pointHoverRadius: 8
                }},
                {{
                    label: 'حد الاحتفاظ (M DA)',
                    data: Array(4).fill(retentionLine),
                    borderColor: '#f59e0b', borderDash: [8, 4], pointRadius: 0, fill: false
                }}
            ]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
            scales: {{ y: {{ beginAtZero: true, title: {{ display: true, text: 'M DA' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }} }}
        }}
    }});

    // PML Zone Distribution pie
    const pml475Zones = {json.dumps({zone_labels_ar.get(z, z): round(pml_475_data["zone_losses"].get(z, {"loss":0})["loss"],0) for z in zones_order})};
    new Chart(document.getElementById('pmlZoneChart'), {{
        type: 'doughnut',
        data: {{
            labels: Object.keys(pml475Zones),
            datasets: [{{ data: Object.values(pml475Zones), backgroundColor: zColors, borderWidth: 2, borderColor: '#1a2235' }}]
        }},
        options: {{
            responsive: true, maintainAspectRatio: false, cutout: '50%',
            plugins: {{
                legend: {{ position: 'bottom', labels: {{ padding: 12, usePointStyle: true }} }},
                tooltip: {{ callbacks: {{ label: ctx => ctx.label + ': ' + (ctx.raw/1e6).toFixed(1) + ' M DA' }} }}
            }}
        }}
    }});

    // Commune SI chart
    new Chart(document.getElementById('communeSIChart'), {{
        type: 'bar',
        data: {{
            labels: {json.dumps(commune_names_chart)},
            datasets: [{{
                label: 'رأس المال المقدر (M DA)',
                data: {json.dumps(commune_si_chart)}.map(v => v/1e6),
                backgroundColor: {json.dumps([c + 'cc' for c in commune_colors_chart])},
                borderColor: {json.dumps(commune_colors_chart)},
                borderWidth: 2, borderRadius: 6
            }}]
        }},
        options: {{
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: {{ legend: {{ display: false }} }},
            scales: {{ x: {{ beginAtZero: true, title: {{ display: true, text: 'M DA' }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}, y: {{ grid: {{ display: false }} }} }}
        }}
    }});
</script>
</body>
</html>"""

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"\n  => Dashboard saved: {output_path}")


# ============================================================================
# 6. MAIN
# ============================================================================

def main():
    print("=" * 70)
    print("Phase 2: Insurance Portfolio Analysis")
    print("=" * 70)

    records = read_all_data()
    print("\n  Analyzing portfolio...")
    analysis = analyze_phase2(records)

    # Print summary
    print(f"\n{'='*70}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*70}")
    print(f"  Total Policies:     {analysis['total_policies']:,}")
    print(f"  Total Premium:      {analysis['total_prime']:,.0f} DA")
    print(f"  Est. Sum Insured:   {analysis['total_estimated_si']:,.0f} DA")

    print(f"\n  --- BY ZONE ---")
    for z in ["0", "I", "IIa", "IIb", "III", "NC"]:
        zd = analysis["by_zone"].get(z, {"count": 0, "est_si": 0})
        si_pct = round(zd["est_si"] / analysis["total_estimated_si"] * 100, 1) if analysis["total_estimated_si"] else 0
        print(f"  Zone {z:4s}: {zd['count']:>6,} policies | Est. SI: {zd['est_si']:>15,.0f} DA ({si_pct}%)")

    print(f"\n  --- BY TYPE ---")
    for tg in ["Installation Industrielle", "Installation Commerciale", "NULL"]:
        td = analysis["by_type"].get(tg, {"count": 0, "est_si": 0})
        print(f"  {tg:30s}: {td['count']:>6,} policies | Est. SI: {td['est_si']:>15,.0f} DA")

    print(f"\n  --- HOTSPOT WILAYAS (Exceeding Retention: {COMPANY_RETENTION:,.0f} DA) ---")
    exceeding = [w for w in analysis["hotspots_wilaya"] if w["exceeds_retention"]]
    for ew in exceeding:
        print(f"  {ew['name']:20s} Zone {ew['zone']:4s} | SI: {ew['est_si']:>15,.0f} DA ({ew['ratio_to_retention']:.1f}x retention)")

    print(f"\n  --- PML SCENARIOS ---")
    for sc in analysis["pml_scenarios"]:
        exceed = "EXCEEDS" if sc["exceeds_retention"] else "within"
        print(f"  RP {sc['return_period']:>3d}yr: Loss = {sc['total_loss']:>15,.0f} DA ({sc['ratio_to_retention']*100:.0f}% of retention) [{exceed}]")

    # Generate dashboard
    base_dir = os.path.dirname(os.path.abspath(__file__))
    output = os.path.join(base_dir, "phase2_portfolio_dashboard.html")
    generate_dashboard(analysis, output)

    print(f"\n  Open in browser: {output}")


if __name__ == "__main__":
    main()
