"""
=============================================================================
المرحلة الأولى: تحديد وتصنيف المخاطر (مرجعية RPA 99/2003)
Phase 1: Risk Identification and Classification (RPA Reference)
=============================================================================

This script performs:
1. RPA 99/2003 Seismic Zone Mapping for all 58 Algerian Wilayas
2. Portfolio classification by seismic zone
3. Vulnerability analysis by construction/risk type
4. Generation of comprehensive output tables and HTML dashboard

Author: Professional Data Analysis - Earthquake Insurance Portfolio
Date: April 2025
"""

import openpyxl
import json
import os
import re
from collections import defaultdict, Counter
from datetime import datetime

# ============================================================================
# 1. RPA 99/2003 OFFICIAL SEISMIC ZONE CLASSIFICATION
# ============================================================================
# Based on DTR BC 2-48 (RPA 99 / version 2003)
# Classification: Zone 0 (negligible) → Zone III (high)
# Note: Some wilayas are split between zones; we assign conservatively
# (higher risk zone) for insurance purposes, and note the split.

# Format: wilaya_code: {"zone": "X", "zone_label": "...", "split": bool, "zones": [...]}

RPA_ZONES = {
    # === ZONE 0 — Sismicité négligeable ===
    1:  {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    8:  {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    11: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    30: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    33: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    37: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    39: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    45: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    47: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    49: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    50: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    53: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    54: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},
    56: {"zone": "0",   "zone_label": "Zone 0 - Sismicité négligeable",    "split": False},

    # === ZONE I — Sismicité faible ===
    3:  {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    7:  {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    12: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    17: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    20: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": True, "zones": ["I", "IIa"]},
    40: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    51: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    55: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},
    57: {"zone": "I",   "zone_label": "Zone I - Sismicité faible",         "split": False},

    # === ZONE IIa — Sismicité moyenne ===
    4:  {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    5:  {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    13: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    14: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": True, "zones": ["I", "IIa"]},
    22: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    24: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    29: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    31: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    32: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": True, "zones": ["0", "I", "IIa"]},
    36: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    41: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": True, "zones": ["I", "IIa"]},
    46: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},
    48: {"zone": "IIa", "zone_label": "Zone IIa - Sismicité moyenne",      "split": False},

    # === ZONE IIb — Sismicité moyenne à élevée ===
    6:  {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    10: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    18: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    19: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb"]},
    21: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    23: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    25: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb"]},
    26: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": False},
    27: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb"]},
    28: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["I", "IIa", "IIb"]},
    34: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb"]},
    38: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["I", "IIa", "IIb"]},
    43: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb"]},
    44: {"zone": "IIb", "zone_label": "Zone IIb - Sismicité élevée",       "split": True, "zones": ["IIa", "IIb", "III"]},

    # === ZONE III — Sismicité très élevée ===
    2:  {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": True, "zones": ["IIb", "III"]},
    9:  {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": False},
    15: {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": True, "zones": ["IIb", "III"]},
    16: {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": False},
    35: {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": False},
    42: {"zone": "III", "zone_label": "Zone III - Sismicité très élevée",   "split": False},
}

# Wilaya names mapping
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

# Risk level coefficients (RPA acceleration coefficients)
ZONE_COEFFICIENTS = {
    "0":   {"A": 0.00, "risk_level": 0, "risk_label_ar": "ضعيفة جدًا", "risk_label_fr": "Négligeable"},
    "I":   {"A": 0.10, "risk_level": 1, "risk_label_ar": "ضعيفة",      "risk_label_fr": "Faible"},
    "IIa": {"A": 0.15, "risk_level": 2, "risk_label_ar": "متوسطة",     "risk_label_fr": "Moyenne"},
    "IIb": {"A": 0.20, "risk_level": 3, "risk_label_ar": "مرتفعة",     "risk_label_fr": "Élevée"},
    "III": {"A": 0.25, "risk_level": 4, "risk_label_ar": "مرتفعة جدًا", "risk_label_fr": "Très élevée"},
}

# ============================================================================
# 2. VULNERABILITY CLASSIFICATION BY CONSTRUCTION TYPE
# ============================================================================
# RPA defines vulnerability based on structural system type.
# We map the insurance "TYPE" field to vulnerability categories.

VULNERABILITY_MATRIX = {
    "Installation Industrielle": {
        "type_code": 1,
        "vulnerability_class": "B",
        "vulnerability_level": "Élevée",
        "vulnerability_level_ar": "مرتفعة",
        "vulnerability_score": 0.8,
        "description_fr": "Structures industrielles - grandes portées, charges lourdes, risque de dommages structurels majeurs",
        "description_ar": "منشآت صناعية - فتحات واسعة، أحمال ثقيلة، خطر أضرار هيكلية كبيرة",
        "factors": [
            "Grandes portées sans contreventement adéquat",
            "Équipements lourds pouvant amplifier les forces sismiques",
            "Matières dangereuses potentielles",
            "Coût de remplacement élevé"
        ]
    },
    "Installation Commerciale": {
        "type_code": 2,
        "vulnerability_class": "B",
        "vulnerability_level": "Moyenne à Élevée",
        "vulnerability_level_ar": "متوسطة إلى مرتفعة",
        "vulnerability_score": 0.6,
        "description_fr": "Installations commerciales - occupation dense, risque pour les personnes",
        "description_ar": "منشآت تجارية - كثافة إشغال عالية، خطر على الأشخاص",
        "factors": [
            "Occupation dense du public",
            "Vitrines et éléments non-structuraux fragiles",
            "Stocks et marchandises vulnérables",
            "Perte d'exploitation significative"
        ]
    },
    "Habitation": {
        "type_code": 3,
        "vulnerability_class": "C",
        "vulnerability_level": "Moyenne",
        "vulnerability_level_ar": "متوسطة",
        "vulnerability_score": 0.5,
        "description_fr": "Habitations résidentielles - qualité de construction variable",
        "description_ar": "مباني سكنية - جودة بناء متفاوتة",
        "factors": [
            "Qualité de construction variable",
            "Conformité parasismique incertaine (bâtiments anciens)",
            "Densité d'occupation résidentielle",
            "Valeur patrimoniale des occupants"
        ]
    },
    "NULL": {
        "type_code": 0,
        "vulnerability_class": "NC",
        "vulnerability_level": "Non classé",
        "vulnerability_level_ar": "غير مصنف",
        "vulnerability_score": 0.5,
        "description_fr": "Type non renseigné - classification par défaut",
        "description_ar": "نوع غير محدد - تصنيف افتراضي",
        "factors": [
            "Type de risque non identifié dans le système",
            "Nécessite une vérification manuelle",
            "Classé par défaut en vulnérabilité moyenne"
        ]
    }
}


# ============================================================================
# 3. DATA EXTRACTION AND PROCESSING
# ============================================================================

def extract_wilaya_code(wilaya_str):
    """Extract numeric wilaya code from string like '2   - CHLEF'"""
    if wilaya_str is None or wilaya_str == 'NULL':
        return None
    match = re.match(r'(\d+)\s*-?\s*', str(wilaya_str).strip())
    if match:
        return int(match.group(1))
    return None

def extract_wilaya_name(wilaya_str):
    """Extract wilaya name from string like '2   - CHLEF'"""
    if wilaya_str is None or wilaya_str == 'NULL':
        return "INCONNU"
    match = re.match(r'\d+\s*-\s*(.*)', str(wilaya_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "INCONNU"
    return str(wilaya_str).strip()

def extract_type_name(type_str):
    """Extract type name from string like '2   - Installation Commerciale'"""
    if type_str is None or type_str == 'NULL' or str(type_str).strip() == '':
        return "NULL"
    match = re.match(r'\d+\s*-\s*(.*)', str(type_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "NULL"
    return str(type_str).strip()

def extract_commune_name(commune_str):
    """Extract commune name from string like '497 - OULED BEN ABDELKADER'"""
    if commune_str is None or commune_str == 'NULL':
        return "INCONNUE"
    match = re.match(r'\d+\s*-\s*(.*)', str(commune_str).strip())
    if match:
        name = match.group(1).strip()
        return name if name else "INCONNUE"
    return str(commune_str).strip()

def get_zone_for_wilaya(code):
    """Get RPA seismic zone for a wilaya code"""
    if code in RPA_ZONES:
        return RPA_ZONES[code]
    # Default for unknown wilayas
    return {"zone": "NC", "zone_label": "Non classé", "split": False}

def get_vulnerability(type_name):
    """Get vulnerability info for a risk type"""
    for key, val in VULNERABILITY_MATRIX.items():
        if key.lower() in type_name.lower():
            return val
    return VULNERABILITY_MATRIX["NULL"]


def read_excel_data(filepath):
    """Read an Excel file and return structured records"""
    print(f"  Reading: {filepath}...")
    wb = openpyxl.load_workbook(filepath, read_only=True)
    ws = wb.active
    records = []
    
    for i, row in enumerate(ws.iter_rows(min_row=2, values_only=True)):
        if row is None:
            continue
        if len(row) < 9:
            continue
        
        wilaya_code = extract_wilaya_code(row[4])
        wilaya_name = extract_wilaya_name(row[4])
        type_name = extract_type_name(row[3])
        commune_name = extract_commune_name(row[5])
        zone_info = get_zone_for_wilaya(wilaya_code) if wilaya_code else {"zone": "NC", "zone_label": "Non classé", "split": False}
        vuln_info = get_vulnerability(type_name)
        
        # Parse VALEUR_ASSUREE
        valeur = row[6]
        if valeur is None or valeur == 'NULL' or str(valeur).strip() == '':
            valeur = 0.0
        else:
            try:
                valeur = float(valeur)
            except (ValueError, TypeError):
                valeur = 0.0
        
        # Parse PRIME_NETTE
        prime = row[8] if len(row) > 8 else 0
        if prime is None or prime == 'NULL':
            prime = 0.0
        else:
            try:
                prime = float(prime)
            except (ValueError, TypeError):
                prime = 0.0
        
        records.append({
            "numero_police": row[0],
            "date_effet": str(row[1]) if row[1] else "",
            "date_expiration": str(row[2]) if row[2] else "",
            "type_raw": str(row[3]) if row[3] else "NULL",
            "type_name": type_name,
            "wilaya_code": wilaya_code,
            "wilaya_name": wilaya_name,
            "commune": commune_name,
            "valeur_assuree": valeur,
            "prime_nette": prime,
            "zone": zone_info["zone"],
            "zone_label": zone_info["zone_label"],
            "zone_split": zone_info.get("split", False),
            "vulnerability_class": vuln_info["vulnerability_class"],
            "vulnerability_level": vuln_info["vulnerability_level"],
            "vulnerability_score": vuln_info["vulnerability_score"],
        })
    
    wb.close()
    print(f"  → {len(records)} records loaded.")
    return records


# ============================================================================
# 4. ANALYSIS ENGINE
# ============================================================================

def analyze_portfolio(records, year_label):
    """Perform comprehensive analysis on the portfolio"""
    
    analysis = {
        "year": year_label,
        "total_policies": len(records),
        "total_prime": sum(r["prime_nette"] for r in records),
        "total_valeur": sum(r["valeur_assuree"] for r in records),
        
        # By Zone
        "by_zone": defaultdict(lambda: {"count": 0, "prime": 0.0, "valeur": 0.0, "wilayas": set()}),
        
        # By Wilaya
        "by_wilaya": defaultdict(lambda: {"count": 0, "prime": 0.0, "valeur": 0.0, "zone": "", "zone_label": "", "communes": set()}),
        
        # By Type
        "by_type": defaultdict(lambda: {"count": 0, "prime": 0.0, "valeur": 0.0, "vuln_class": "", "vuln_level": ""}),
        
        # By Zone × Type (cross analysis)
        "zone_type_matrix": defaultdict(lambda: defaultdict(lambda: {"count": 0, "prime": 0.0})),
        
        # By Commune (top hotspots)
        "by_commune": defaultdict(lambda: {"count": 0, "prime": 0.0, "valeur": 0.0, "wilaya": "", "zone": ""}),
        
        # Data quality
        "null_types": 0,
        "null_wilayas": 0,
        "null_valeurs": 0,
    }
    
    for r in records:
        zone = r["zone"]
        wilaya_key = f"{r['wilaya_code']}_{r['wilaya_name']}"
        type_key = r["type_name"]
        commune_key = f"{r['commune']}_{r['wilaya_name']}"
        
        # By zone
        analysis["by_zone"][zone]["count"] += 1
        analysis["by_zone"][zone]["prime"] += r["prime_nette"]
        analysis["by_zone"][zone]["valeur"] += r["valeur_assuree"]
        analysis["by_zone"][zone]["wilayas"].add(r["wilaya_name"])
        
        # By wilaya
        analysis["by_wilaya"][wilaya_key]["count"] += 1
        analysis["by_wilaya"][wilaya_key]["prime"] += r["prime_nette"]
        analysis["by_wilaya"][wilaya_key]["valeur"] += r["valeur_assuree"]
        analysis["by_wilaya"][wilaya_key]["zone"] = zone
        analysis["by_wilaya"][wilaya_key]["zone_label"] = r["zone_label"]
        analysis["by_wilaya"][wilaya_key]["communes"].add(r["commune"])
        
        # By type
        analysis["by_type"][type_key]["count"] += 1
        analysis["by_type"][type_key]["prime"] += r["prime_nette"]
        analysis["by_type"][type_key]["valeur"] += r["valeur_assuree"]
        analysis["by_type"][type_key]["vuln_class"] = r["vulnerability_class"]
        analysis["by_type"][type_key]["vuln_level"] = r["vulnerability_level"]
        
        # Zone × Type matrix
        analysis["zone_type_matrix"][zone][type_key]["count"] += 1
        analysis["zone_type_matrix"][zone][type_key]["prime"] += r["prime_nette"]
        
        # By commune
        analysis["by_commune"][commune_key]["count"] += 1
        analysis["by_commune"][commune_key]["prime"] += r["prime_nette"]
        analysis["by_commune"][commune_key]["valeur"] += r["valeur_assuree"]
        analysis["by_commune"][commune_key]["wilaya"] = r["wilaya_name"]
        analysis["by_commune"][commune_key]["zone"] = zone
        
        # Data quality
        if r["type_name"] == "NULL":
            analysis["null_types"] += 1
        if r["wilaya_code"] is None:
            analysis["null_wilayas"] += 1
        if r["valeur_assuree"] == 0:
            analysis["null_valeurs"] += 1
    
    return analysis


# ============================================================================
# 5. HTML DASHBOARD GENERATION
# ============================================================================

def generate_dashboard(analyses, output_path):
    """Generate a comprehensive interactive HTML dashboard"""
    
    # Use latest year analysis as primary
    latest = analyses[-1]
    all_zones_order = ["0", "I", "IIa", "IIb", "III", "NC"]
    zone_colors = {
        "0": "#2ecc71", "I": "#f1c40f", "IIa": "#e67e22",
        "IIb": "#e74c3c", "III": "#8e44ad", "NC": "#95a5a6"
    }
    zone_labels = {
        "0": "Zone 0 - ضعيفة جدًا", "I": "Zone I - ضعيفة", "IIa": "Zone IIa - متوسطة",
        "IIb": "Zone IIb - مرتفعة", "III": "Zone III - مرتفعة جدًا", "NC": "غير مصنف"
    }
    
    # Prepare zone data for charts
    zone_counts = []
    zone_primes = []
    zone_labels_list = []
    zone_colors_list = []
    for z in all_zones_order:
        if z in latest["by_zone"]:
            data = latest["by_zone"][z]
            zone_counts.append(data["count"])
            zone_primes.append(round(data["prime"], 2))
        else:
            zone_counts.append(0)
            zone_primes.append(0)
        zone_labels_list.append(zone_labels.get(z, z))
        zone_colors_list.append(zone_colors.get(z, "#999"))
    
    # Prepare type data
    type_names = []
    type_counts = []
    type_primes = []
    vuln_levels = []
    for t_name, t_data in sorted(latest["by_type"].items(), key=lambda x: -x[1]["count"]):
        type_names.append(t_name if t_name != "NULL" else "غير محدد")
        type_counts.append(t_data["count"])
        type_primes.append(round(t_data["prime"], 2))
        vuln_levels.append(t_data["vuln_level"])
    
    # Prepare wilaya table (top 25 by policy count)
    wilaya_rows = []
    for w_key, w_data in sorted(latest["by_wilaya"].items(), key=lambda x: -x[1]["count"])[:25]:
        parts = w_key.split("_", 1)
        code = parts[0]
        name = parts[1] if len(parts) > 1 else "INCONNU"
        wilaya_rows.append({
            "code": code,
            "name": name,
            "count": w_data["count"],
            "prime": round(w_data["prime"], 2),
            "zone": w_data["zone"],
            "zone_label": w_data["zone_label"],
            "communes": len(w_data["communes"]),
            "pct": round(w_data["count"] / latest["total_policies"] * 100, 1)
        })
    
    # Prepare commune hotspots (top 20 by count in high-risk zones)
    hotspot_rows = []
    for c_key, c_data in sorted(latest["by_commune"].items(), key=lambda x: -x[1]["count"])[:20]:
        parts = c_key.rsplit("_", 1)
        hotspot_rows.append({
            "commune": parts[0],
            "wilaya": c_data["wilaya"],
            "count": c_data["count"],
            "prime": round(c_data["prime"], 2),
            "zone": c_data["zone"],
        })
    
    # Zone × Type matrix
    matrix_data = {}
    for z in all_zones_order:
        if z in latest["zone_type_matrix"]:
            matrix_data[z] = {}
            for t_name, t_data in latest["zone_type_matrix"][z].items():
                matrix_data[z][t_name if t_name != "NULL" else "غير محدد"] = t_data["count"]
    
    # Yearly trend data
    trend_years = []
    trend_totals = []
    trend_zone3 = []
    trend_zone2b = []
    trend_primes = []
    for a in analyses:
        trend_years.append(a["year"])
        trend_totals.append(a["total_policies"])
        trend_zone3.append(a["by_zone"].get("III", {}).get("count", 0) if isinstance(a["by_zone"].get("III"), dict) else 0)
        trend_zone2b.append(a["by_zone"].get("IIb", {}).get("count", 0) if isinstance(a["by_zone"].get("IIb"), dict) else 0)
        trend_primes.append(round(a["total_prime"], 2))
    
    # Build RPA reference table
    rpa_ref_rows = []
    for code in sorted(RPA_ZONES.keys()):
        info = RPA_ZONES[code]
        name = WILAYA_NAMES.get(code, f"Wilaya {code}")
        coeff = ZONE_COEFFICIENTS.get(info["zone"], {})
        rpa_ref_rows.append({
            "code": str(code).zfill(2),
            "name": name,
            "zone": info["zone"],
            "A": coeff.get("A", 0),
            "risk_ar": coeff.get("risk_label_ar", "—"),
            "risk_fr": coeff.get("risk_label_fr", "—"),
            "split": info["split"],
            "zones": ", ".join(info.get("zones", [info["zone"]])),
        })

    # Pre-compute values that would cause issues inside f-string expressions
    high_risk_count = latest['by_zone'].get('III', {}).get('count', 0) + latest['by_zone'].get('IIb', {}).get('count', 0)
    high_risk_pct = round(high_risk_count / latest['total_policies'] * 100, 1)
    growth_pct = round((analyses[-1]['total_policies'] - analyses[0]['total_policies']) / analyses[0]['total_policies'] * 100, 1)

    html = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>المرحلة الأولى: تحديد وتصنيف المخاطر - مرجعية RPA 99/2003</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Cairo:wght@300;400;600;700;800;900&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <style>
        :root {{
            --bg-primary: #0a0e1a;
            --bg-secondary: #111827;
            --bg-card: #1a2235;
            --bg-card-hover: #1f2a40;
            --text-primary: #e8edf5;
            --text-secondary: #8b95a8;
            --text-muted: #5a6478;
            --accent-blue: #3b82f6;
            --accent-purple: #8b5cf6;
            --accent-green: #10b981;
            --accent-amber: #f59e0b;
            --accent-red: #ef4444;
            --accent-orange: #f97316;
            --border-color: rgba(255,255,255,0.06);
            --glow-blue: rgba(59, 130, 246, 0.15);
            --glow-purple: rgba(139, 92, 246, 0.15);
            --glow-red: rgba(239, 68, 68, 0.15);
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
        
        /* Animated background */
        body::before {{
            content: '';
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: 
                radial-gradient(ellipse 600px 400px at 20% 20%, rgba(59,130,246,0.08), transparent),
                radial-gradient(ellipse 500px 300px at 80% 70%, rgba(139,92,246,0.06), transparent),
                radial-gradient(ellipse 400px 400px at 50% 50%, rgba(239,68,68,0.04), transparent);
            z-index: -1;
            animation: bgPulse 12s ease-in-out infinite alternate;
        }}
        @keyframes bgPulse {{
            0% {{ opacity: 0.7; }}
            100% {{ opacity: 1; }}
        }}
        
        .container {{
            max-width: 1440px;
            margin: 0 auto;
            padding: 30px 40px;
        }}
        
        /* Header */
        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding: 40px;
            background: linear-gradient(135deg, rgba(59,130,246,0.1), rgba(139,92,246,0.1));
            border-radius: var(--radius);
            border: 1px solid var(--border-color);
            position: relative;
            overflow: hidden;
        }}
        .header::after {{
            content: '';
            position: absolute;
            top: -50%; left: -50%; width: 200%; height: 200%;
            background: conic-gradient(from 0deg, transparent, rgba(59,130,246,0.03), transparent, rgba(139,92,246,0.03), transparent);
            animation: headerRotate 20s linear infinite;
        }}
        @keyframes headerRotate {{
            100% {{ transform: rotate(360deg); }}
        }}
        .header-content {{ position: relative; z-index: 1; }}
        .header h1 {{
            font-size: 2.2rem;
            font-weight: 800;
            background: linear-gradient(135deg, #3b82f6, #8b5cf6, #ec4899);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 12px;
            line-height: 1.5;
        }}
        .header .subtitle {{
            font-size: 1.1rem;
            color: var(--text-secondary);
            font-weight: 400;
        }}
        .header .badge {{
            display: inline-block;
            background: rgba(59,130,246,0.15);
            color: var(--accent-blue);
            padding: 6px 18px;
            border-radius: 30px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-top: 15px;
            border: 1px solid rgba(59,130,246,0.2);
        }}
        
        /* Navigation Tabs */
        .nav-tabs {{
            display: flex;
            gap: 8px;
            margin-bottom: 30px;
            padding: 6px;
            background: var(--bg-secondary);
            border-radius: var(--radius);
            border: 1px solid var(--border-color);
            overflow-x: auto;
        }}
        .nav-tab {{
            padding: 12px 24px;
            border-radius: var(--radius-sm);
            cursor: pointer;
            font-weight: 600;
            font-size: 0.9rem;
            color: var(--text-secondary);
            transition: all 0.3s;
            white-space: nowrap;
            border: none;
            background: none;
            font-family: inherit;
        }}
        .nav-tab:hover {{ color: var(--text-primary); background: rgba(255,255,255,0.05); }}
        .nav-tab.active {{
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            color: white;
            box-shadow: 0 4px 15px rgba(59,130,246,0.3);
        }}
        
        /* Tab Content */
        .tab-content {{ display: none; animation: fadeIn 0.4s ease; }}
        .tab-content.active {{ display: block; }}
        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        
        /* KPI Cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .kpi-card {{
            background: var(--bg-card);
            border-radius: var(--radius);
            padding: 24px;
            border: 1px solid var(--border-color);
            transition: all 0.3s;
            position: relative;
            overflow: hidden;
        }}
        .kpi-card:hover {{
            transform: translateY(-3px);
            border-color: rgba(59,130,246,0.3);
            box-shadow: 0 8px 30px rgba(0,0,0,0.3);
        }}
        .kpi-card .kpi-icon {{
            width: 48px; height: 48px;
            border-radius: 12px;
            display: flex; align-items: center; justify-content: center;
            font-size: 1.4rem;
            margin-bottom: 15px;
        }}
        .kpi-card .kpi-value {{
            font-size: 2rem;
            font-weight: 800;
            margin-bottom: 5px;
            line-height: 1.2;
        }}
        .kpi-card .kpi-label {{
            color: var(--text-secondary);
            font-size: 0.85rem;
            font-weight: 500;
        }}
        .kpi-card .kpi-trend {{
            font-size: 0.8rem;
            margin-top: 8px;
            padding: 3px 10px;
            border-radius: 20px;
            display: inline-block;
        }}
        .kpi-card .kpi-trend.up {{ background: rgba(16,185,129,0.15); color: var(--accent-green); }}
        .kpi-card .kpi-trend.warning {{ background: rgba(245,158,11,0.15); color: var(--accent-amber); }}
        .kpi-card .kpi-trend.danger {{ background: rgba(239,68,68,0.15); color: var(--accent-red); }}
        
        /* Charts Grid */
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
            margin-bottom: 30px;
        }}
        .chart-card {{
            background: var(--bg-card);
            border-radius: var(--radius);
            padding: 28px;
            border: 1px solid var(--border-color);
        }}
        .chart-card h3 {{
            font-size: 1.1rem;
            font-weight: 700;
            margin-bottom: 20px;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        .chart-card h3 .icon {{
            width: 32px; height: 32px;
            border-radius: 8px;
            display: flex; align-items: center; justify-content: center;
            font-size: 1rem;
        }}
        .chart-wrapper {{ position: relative; height: 350px; }}
        .chart-wrapper.tall {{ height: 450px; }}
        
        /* Tables */
        .table-card {{
            background: var(--bg-card);
            border-radius: var(--radius);
            padding: 28px;
            border: 1px solid var(--border-color);
            margin-bottom: 24px;
            overflow-x: auto;
        }}
        .table-card h3 {{
            font-size: 1.1rem;
            font-weight: 700;
            margin-bottom: 20px;
            display: flex; align-items: center; gap: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.88rem;
        }}
        th {{
            background: rgba(59,130,246,0.1);
            color: var(--accent-blue);
            font-weight: 700;
            padding: 14px 16px;
            text-align: right;
            white-space: nowrap;
            border-bottom: 2px solid rgba(59,130,246,0.2);
        }}
        td {{
            padding: 12px 16px;
            border-bottom: 1px solid var(--border-color);
            color: var(--text-primary);
        }}
        tr:hover td {{ background: rgba(255,255,255,0.02); }}
        
        /* Zone badges */
        .zone-badge {{
            display: inline-block;
            padding: 4px 14px;
            border-radius: 20px;
            font-size: 0.8rem;
            font-weight: 700;
        }}
        .zone-0   {{ background: rgba(46,204,113,0.15); color: #2ecc71; }}
        .zone-I   {{ background: rgba(241,196,15,0.15); color: #f1c40f; }}
        .zone-IIa {{ background: rgba(230,126,34,0.15); color: #e67e22; }}
        .zone-IIb {{ background: rgba(231,76,60,0.15);  color: #e74c3c; }}
        .zone-III {{ background: rgba(142,68,173,0.15); color: #8e44ad; }}
        .zone-NC  {{ background: rgba(149,165,166,0.15); color: #95a5a6; }}
        
        /* Vulnerability cards */
        .vuln-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .vuln-card {{
            background: var(--bg-card);
            border-radius: var(--radius);
            padding: 28px;
            border: 1px solid var(--border-color);
            transition: all 0.3s;
        }}
        .vuln-card:hover {{
            border-color: rgba(139,92,246,0.3);
            box-shadow: 0 8px 30px rgba(0,0,0,0.2);
        }}
        .vuln-card h4 {{
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 8px;
        }}
        .vuln-card .vuln-level {{
            font-size: 0.85rem;
            color: var(--accent-amber);
            font-weight: 600;
            margin-bottom: 15px;
        }}
        .vuln-card p {{
            font-size: 0.85rem;
            color: var(--text-secondary);
            line-height: 1.8;
            margin-bottom: 15px;
        }}
        .vuln-card ul {{
            list-style: none;
            padding: 0;
        }}
        .vuln-card ul li {{
            font-size: 0.82rem;
            color: var(--text-secondary);
            padding: 5px 0;
            padding-right: 18px;
            position: relative;
        }}
        .vuln-card ul li::before {{
            content: '⚠';
            position: absolute;
            right: 0;
            font-size: 0.7rem;
        }}
        .vuln-score {{
            display: flex;
            align-items: center;
            gap: 10px;
            margin-top: 12px;
        }}
        .vuln-bar {{
            flex: 1;
            height: 8px;
            background: rgba(255,255,255,0.1);
            border-radius: 4px;
            overflow: hidden;
        }}
        .vuln-bar-fill {{
            height: 100%;
            border-radius: 4px;
            transition: width 1s ease;
        }}
        
        /* RPA Reference Section */
        .rpa-zone-legend {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .legend-item {{
            display: flex;
            align-items: center;
            gap: 12px;
            padding: 16px;
            background: var(--bg-card);
            border-radius: var(--radius-sm);
            border: 1px solid var(--border-color);
        }}
        .legend-dot {{
            width: 16px; height: 16px;
            border-radius: 50%;
            flex-shrink: 0;
        }}
        .legend-info .legend-title {{ font-weight: 700; font-size: 0.9rem; }}
        .legend-info .legend-desc {{ font-size: 0.78rem; color: var(--text-secondary); margin-top: 3px; }}
        
        /* Data quality */
        .dq-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
        }}
        .dq-item {{
            background: var(--bg-card);
            border-radius: var(--radius-sm);
            padding: 20px;
            border: 1px solid var(--border-color);
        }}
        .dq-item .dq-label {{ font-size: 0.85rem; color: var(--text-secondary); margin-bottom: 8px; }}
        .dq-item .dq-value {{ font-size: 1.5rem; font-weight: 800; }}
        .dq-item .dq-pct {{ font-size: 0.85rem; color: var(--accent-amber); }}
        
        /* Print styles */
        @media print {{
            body {{ background: white; color: black; }}
            .nav-tabs {{ display: none; }}
            .tab-content {{ display: block !important; page-break-inside: avoid; }}
        }}
        
        @media (max-width: 768px) {{
            .container {{ padding: 15px; }}
            .charts-grid {{ grid-template-columns: 1fr; }}
            .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
            .header h1 {{ font-size: 1.5rem; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <div class="header">
            <div class="header-content">
                <h1>المرحلة الأولى: تحديد وتصنيف المخاطر الزلزالية</h1>
                <p class="subtitle">Phase 1: Identification et Classification des Risques Sismiques — Référentiel RPA 99/2003</p>
                <span class="badge">📊 تحليل محفظة التأمين ضد الكوارث الطبيعية {latest['year']}</span>
            </div>
        </div>
        
        <!-- Navigation Tabs -->
        <div class="nav-tabs">
            <button class="nav-tab active" onclick="showTab('overview')">📊 نظرة عامة</button>
            <button class="nav-tab" onclick="showTab('rpa-ref')">🗺️ مرجعية RPA</button>
            <button class="nav-tab" onclick="showTab('zone-analysis')">📍 تحليل المناطق</button>
            <button class="nav-tab" onclick="showTab('vulnerability')">🏗️ الهشاشة الجوهرية</button>
            <button class="nav-tab" onclick="showTab('wilaya-detail')">🏛️ تفصيل الولايات</button>
            <button class="nav-tab" onclick="showTab('hotspots')">🔥 النقاط الساخنة</button>
            <button class="nav-tab" onclick="showTab('data-quality')">📋 جودة البيانات</button>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 1: OVERVIEW -->
        <!-- ============================================================ -->
        <div id="tab-overview" class="tab-content active">
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="kpi-icon" style="background:rgba(59,130,246,0.15);">📋</div>
                    <div class="kpi-value" style="color:var(--accent-blue);">{latest['total_policies']:,}</div>
                    <div class="kpi-label">إجمالي الوثائق ({latest['year']})</div>
                    <span class="kpi-trend up">+{growth_pct}% منذ {analyses[0]['year']}</span>
                </div>
                <div class="kpi-card">
                    <div class="kpi-icon" style="background:rgba(16,185,129,0.15);">💰</div>
                    <div class="kpi-value" style="color:var(--accent-green);">{latest['total_prime']:,.0f}</div>
                    <div class="kpi-label">إجمالي الأقساط الصافية (د.ج)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-icon" style="background:rgba(239,68,68,0.15);">⚠️</div>
                    <div class="kpi-value" style="color:var(--accent-red);">{high_risk_count:,}</div>
                    <div class="kpi-label">وثائق في مناطق عالية الخطورة (IIb + III)</div>
                    <span class="kpi-trend danger">{high_risk_pct}% من المحفظة</span>
                </div>
                <div class="kpi-card">
                    <div class="kpi-icon" style="background:rgba(139,92,246,0.15);">🏙️</div>
                    <div class="kpi-value" style="color:var(--accent-purple);">{len(latest['by_wilaya'])}</div>
                    <div class="kpi-label">ولاية مغطاة</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-icon" style="background:rgba(249,115,22,0.15);">🏘️</div>
                    <div class="kpi-value" style="color:var(--accent-orange);">{len(latest['by_commune'])}</div>
                    <div class="kpi-label">بلدية مغطاة</div>
                </div>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(59,130,246,0.15);">📊</span> توزيع الوثائق حسب المنطقة الزلزالية</h3>
                    <div class="chart-wrapper"><canvas id="zoneDistChart"></canvas></div>
                </div>
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(139,92,246,0.15);">💰</span> توزيع الأقساط حسب المنطقة الزلزالية</h3>
                    <div class="chart-wrapper"><canvas id="zonePrimeChart"></canvas></div>
                </div>
            </div>
            
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(16,185,129,0.15);">📈</span> تطور المحفظة عبر السنوات</h3>
                <div class="chart-wrapper"><canvas id="trendChart"></canvas></div>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 2: RPA REFERENCE -->
        <!-- ============================================================ -->
        <div id="tab-rpa-ref" class="tab-content">
            <div class="chart-card" style="margin-bottom:24px;">
                <h3><span class="icon" style="background:rgba(59,130,246,0.15);">📖</span> التقسيم الزلزالي حسب RPA 99/2003</h3>
                <p style="color:var(--text-secondary);font-size:0.9rem;margin-bottom:20px;line-height:1.8;">
                    يعتمد التقسيم الزلزالي للجزائر على القواعد الجزائرية لمقاومة الزلازل (RPA 99 / النسخة 2003) — المرجع التقني DTR BC 2-48.
                    يُقسم الإقليم الوطني إلى خمس مناطق زلزالية حسب مستوى الخطورة، ولكل منطقة معامل تسارع زلزالي (A) يُستخدم في حسابات مقاومة الزلازل.
                </p>
            </div>
            
            <div class="rpa-zone-legend">
                <div class="legend-item">
                    <div class="legend-dot" style="background:#2ecc71;"></div>
                    <div class="legend-info">
                        <div class="legend-title">Zone 0 — ضعيفة جدًا</div>
                        <div class="legend-desc">A = 0.00 | Sismicité négligeable</div>
                    </div>
                </div>
                <div class="legend-item">
                    <div class="legend-dot" style="background:#f1c40f;"></div>
                    <div class="legend-info">
                        <div class="legend-title">Zone I — ضعيفة</div>
                        <div class="legend-desc">A = 0.10 | Sismicité faible</div>
                    </div>
                </div>
                <div class="legend-item">
                    <div class="legend-dot" style="background:#e67e22;"></div>
                    <div class="legend-info">
                        <div class="legend-title">Zone IIa — متوسطة</div>
                        <div class="legend-desc">A = 0.15 | Sismicité moyenne</div>
                    </div>
                </div>
                <div class="legend-item">
                    <div class="legend-dot" style="background:#e74c3c;"></div>
                    <div class="legend-info">
                        <div class="legend-title">Zone IIb — مرتفعة</div>
                        <div class="legend-desc">A = 0.20 | Sismicité élevée</div>
                    </div>
                </div>
                <div class="legend-item">
                    <div class="legend-dot" style="background:#8e44ad;"></div>
                    <div class="legend-info">
                        <div class="legend-title">Zone III — مرتفعة جدًا</div>
                        <div class="legend-desc">A = 0.25 | Sismicité très élevée</div>
                    </div>
                </div>
            </div>
            
            <div class="table-card">
                <h3><span class="icon" style="background:rgba(59,130,246,0.15);">🗺️</span> الخريطة المرجعية: تصنيف الولايات حسب المنطقة الزلزالية</h3>
                <table>
                    <thead>
                        <tr>
                            <th>الرمز</th>
                            <th>الولاية</th>
                            <th>المنطقة الزلزالية</th>
                            <th>معامل التسارع (A)</th>
                            <th>مستوى الخطورة</th>
                            <th>تقسيم داخلي</th>
                            <th>المناطق الفرعية</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    for row in rpa_ref_rows:
        zone_class = f"zone-{row['zone']}"
        split_icon = "⚠️ نعم" if row["split"] else "—"
        html += f"""                        <tr>
                            <td><strong>{row['code']}</strong></td>
                            <td>{row['name']}</td>
                            <td><span class="zone-badge {zone_class}">{row['zone']}</span></td>
                            <td>{row['A']}</td>
                            <td>{row['risk_ar']}</td>
                            <td>{split_icon}</td>
                            <td>{row['zones']}</td>
                        </tr>
"""
    
    html += """                    </tbody>
                </table>
            </div>
            
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(139,92,246,0.15);">📊</span> توزيع الولايات حسب المنطقة الزلزالية</h3>
                <div class="chart-wrapper"><canvas id="rpaDistChart"></canvas></div>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 3: ZONE ANALYSIS -->
        <!-- ============================================================ -->
        <div id="tab-zone-analysis" class="tab-content">
            <div class="table-card">
                <h3><span class="icon" style="background:rgba(239,68,68,0.15);">📍</span> تحليل المحفظة حسب المنطقة الزلزالية</h3>
                <table>
                    <thead>
                        <tr>
                            <th>المنطقة</th>
                            <th>عدد الوثائق</th>
                            <th>النسبة %</th>
                            <th>الأقساط الصافية (د.ج)</th>
                            <th>نسبة الأقساط %</th>
                            <th>عدد الولايات</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    for z in all_zones_order:
        if z in latest["by_zone"]:
            data = latest["by_zone"][z]
            zone_class = f"zone-{z}"
            pct = round(data["count"] / latest["total_policies"] * 100, 1)
            prime_pct = round(data["prime"] / latest["total_prime"] * 100, 1) if latest["total_prime"] > 0 else 0
            html += f"""                        <tr>
                            <td><span class="zone-badge {zone_class}">{zone_labels.get(z, z)}</span></td>
                            <td><strong>{data['count']:,}</strong></td>
                            <td>{pct}%</td>
                            <td>{data['prime']:,.0f}</td>
                            <td>{prime_pct}%</td>
                            <td>{len(data['wilayas'])}</td>
                        </tr>
"""
    
    html += """                    </tbody>
                </table>
            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(239,68,68,0.15);">🎯</span> مقارنة التركيز: الوثائق vs الأقساط</h3>
                    <div class="chart-wrapper"><canvas id="zoneCompareChart"></canvas></div>
                </div>
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(249,115,22,0.15);">📊</span> المصفوفة: المنطقة × نوع الخطر</h3>
                    <div class="chart-wrapper"><canvas id="zoneTypeChart"></canvas></div>
                </div>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 4: VULNERABILITY -->
        <!-- ============================================================ -->
        <div id="tab-vulnerability" class="tab-content">
            <div class="chart-card" style="margin-bottom:24px;">
                <h3><span class="icon" style="background:rgba(245,158,11,0.15);">🏗️</span> الهشاشة الجوهرية حسب نوع البناء</h3>
                <p style="color:var(--text-secondary);font-size:0.9rem;line-height:1.8;">
                    تحدد الهشاشة الجوهرية مستوى تعرض المنشأة للأضرار الزلزالية حسب طبيعة البناء ونظامه الإنشائي.
                    يعتمد التصنيف على معايير RPA المتعلقة بنوع الهيكل، المواد المستخدمة، والحمولات.
                </p>
            </div>
            
            <div class="vuln-grid">
"""
    
    vuln_colors = {
        "Installation Industrielle": "#ef4444",
        "Installation Commerciale": "#f59e0b",
        "Habitation": "#3b82f6",
        "NULL": "#6b7280"
    }
    
    for key, info in VULNERABILITY_MATRIX.items():
        color = vuln_colors.get(key, "#6b7280")
        title = key if key != "NULL" else "غير محدد (NULL)"
        count_in_data = latest["by_type"].get(key, {}).get("count", 0)
        
        factors_html = "".join(f"<li>{f}</li>" for f in info["factors"])
        
        html += f"""                <div class="vuln-card">
                    <h4 style="color:{color};">🏢 {title}</h4>
                    <div class="vuln-level">الهشاشة: {info['vulnerability_level']} — {info['vulnerability_level_ar']}</div>
                    <p>{info['description_ar']}<br/><em style="color:var(--text-muted);">{info['description_fr']}</em></p>
                    <ul>{factors_html}</ul>
                    <div class="vuln-score">
                        <span style="font-size:0.8rem;color:var(--text-secondary);">درجة الهشاشة:</span>
                        <div class="vuln-bar">
                            <div class="vuln-bar-fill" style="width:{info['vulnerability_score']*100}%;background:{color};"></div>
                        </div>
                        <span style="font-size:0.85rem;font-weight:700;color:{color};">{info['vulnerability_score']}</span>
                    </div>
                    <div style="margin-top:12px;padding:10px;background:rgba(255,255,255,0.03);border-radius:8px;">
                        <span style="font-size:0.8rem;color:var(--text-secondary);">عدد الوثائق في المحفظة:</span>
                        <strong style="color:var(--text-primary);margin-right:8px;">{count_in_data:,}</strong>
                    </div>
                </div>
"""
    
    html += """            </div>
            
            <div class="charts-grid">
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(245,158,11,0.15);">📊</span> توزيع الوثائق حسب نوع الخطر</h3>
                    <div class="chart-wrapper"><canvas id="typeDistChart"></canvas></div>
                </div>
                <div class="chart-card">
                    <h3><span class="icon" style="background:rgba(239,68,68,0.15);">⚡</span> مؤشر الخطر المركب (منطقة × هشاشة)</h3>
                    <div class="chart-wrapper"><canvas id="compositeRiskChart"></canvas></div>
                </div>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 5: WILAYA DETAIL -->
        <!-- ============================================================ -->
        <div id="tab-wilaya-detail" class="tab-content">
            <div class="table-card">
                <h3><span class="icon" style="background:rgba(59,130,246,0.15);">🏛️</span> تفصيل الولايات (أعلى 25 من حيث عدد الوثائق)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>الرمز</th>
                            <th>الولاية</th>
                            <th>المنطقة</th>
                            <th>عدد الوثائق</th>
                            <th>النسبة %</th>
                            <th>الأقساط (د.ج)</th>
                            <th>عدد البلديات</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    for i, row in enumerate(wilaya_rows, 1):
        zone_class = f"zone-{row['zone']}"
        html += f"""                        <tr>
                            <td>{i}</td>
                            <td><strong>{row['code']}</strong></td>
                            <td>{row['name']}</td>
                            <td><span class="zone-badge {zone_class}">{row['zone']}</span></td>
                            <td><strong>{row['count']:,}</strong></td>
                            <td>{row['pct']}%</td>
                            <td>{row['prime']:,.0f}</td>
                            <td>{row['communes']}</td>
                        </tr>
"""
    
    html += """                    </tbody>
                </table>
            </div>
            
            <div class="chart-card">
                <h3><span class="icon" style="background:rgba(139,92,246,0.15);">📊</span> أكبر 15 ولاية حسب عدد الوثائق</h3>
                <div class="chart-wrapper tall"><canvas id="wilayaBarChart"></canvas></div>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 6: HOTSPOTS -->
        <!-- ============================================================ -->
        <div id="tab-hotspots" class="tab-content">
            <div class="table-card">
                <h3><span class="icon" style="background:rgba(239,68,68,0.15);">🔥</span> النقاط الساخنة — أكبر 20 بلدية من حيث التركيز</h3>
                <table>
                    <thead>
                        <tr>
                            <th>#</th>
                            <th>البلدية</th>
                            <th>الولاية</th>
                            <th>المنطقة</th>
                            <th>عدد الوثائق</th>
                            <th>الأقساط (د.ج)</th>
                        </tr>
                    </thead>
                    <tbody>
"""
    
    for i, row in enumerate(hotspot_rows, 1):
        zone_class = f"zone-{row['zone']}"
        html += f"""                        <tr>
                            <td>{i}</td>
                            <td><strong>{row['commune']}</strong></td>
                            <td>{row['wilaya']}</td>
                            <td><span class="zone-badge {zone_class}">{row['zone']}</span></td>
                            <td><strong>{row['count']:,}</strong></td>
                            <td>{row['prime']:,.0f}</td>
                        </tr>
"""
    
    html += """                    </tbody>
                </table>
            </div>
        </div>
        
        <!-- ============================================================ -->
        <!-- TAB 7: DATA QUALITY -->
        <!-- ============================================================ -->
        <div id="tab-data-quality" class="tab-content">
            <div class="chart-card" style="margin-bottom:24px;">
                <h3><span class="icon" style="background:rgba(245,158,11,0.15);">📋</span> تقييم جودة البيانات</h3>
                <p style="color:var(--text-secondary);font-size:0.9rem;line-height:1.8;">
                    التقييم التالي يكشف عن نقاط الضعف في البيانات المتاحة والتي قد تؤثر على دقة التحليل.
                    يُوصى بمعالجة هذه النقاط قبل الانتقال إلى المراحل التالية.
                </p>
            </div>
            
            <div class="dq-grid">
                <div class="dq-item">
                    <div class="dq-label">📄 إجمالي السجلات</div>
"""
    
    html += f"""                    <div class="dq-value" style="color:var(--accent-blue);">{latest['total_policies']:,}</div>
                </div>
                <div class="dq-item">
                    <div class="dq-label">⚠️ نوع الخطر غير محدد (NULL)</div>
                    <div class="dq-value" style="color:var(--accent-amber);">{latest['null_types']:,}</div>
                    <div class="dq-pct">{round(latest['null_types']/latest['total_policies']*100, 1)}% من الإجمالي</div>
                </div>
                <div class="dq-item">
                    <div class="dq-label">⚠️ الولاية غير محددة</div>
                    <div class="dq-value" style="color:var(--accent-amber);">{latest['null_wilayas']:,}</div>
                    <div class="dq-pct">{round(latest['null_wilayas']/latest['total_policies']*100, 1)}% من الإجمالي</div>
                </div>
                <div class="dq-item">
                    <div class="dq-label">⚠️ القيمة المؤمنة = 0 أو NULL</div>
                    <div class="dq-value" style="color:var(--accent-red);">{latest['null_valeurs']:,}</div>
                    <div class="dq-pct">{round(latest['null_valeurs']/latest['total_policies']*100, 1)}% من الإجمالي</div>
                </div>
            </div>
        </div>
    </div>
"""
    
    # JavaScript
    html += f"""
    <script>
        // Tab switching
        function showTab(tabId) {{
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
            document.getElementById('tab-' + tabId).classList.add('active');
            event.target.classList.add('active');
        }}
        
        // Chart.js defaults
        Chart.defaults.color = '#8b95a8';
        Chart.defaults.borderColor = 'rgba(255,255,255,0.06)';
        Chart.defaults.font.family = "'Cairo', 'Inter', sans-serif";
        
        const zoneLabels = {json.dumps(zone_labels_list)};
        const zoneColors = {json.dumps(zone_colors_list)};
        const zoneCounts = {json.dumps(zone_counts)};
        const zonePrimes = {json.dumps(zone_primes)};
        
        // 1. Zone Distribution (Doughnut)
        new Chart(document.getElementById('zoneDistChart'), {{
            type: 'doughnut',
            data: {{
                labels: zoneLabels,
                datasets: [{{
                    data: zoneCounts,
                    backgroundColor: zoneColors,
                    borderWidth: 2,
                    borderColor: '#1a2235',
                    hoverOffset: 15
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ position: 'bottom', labels: {{ padding: 15, usePointStyle: true }} }},
                    tooltip: {{
                        callbacks: {{
                            label: (ctx) => {{
                                const total = ctx.dataset.data.reduce((a,b) => a+b, 0);
                                const pct = ((ctx.raw / total) * 100).toFixed(1);
                                return ctx.label + ': ' + ctx.raw.toLocaleString() + ' (' + pct + '%)';
                            }}
                        }}
                    }}
                }},
                cutout: '55%'
            }}
        }});
        
        // 2. Zone Primes (Doughnut)
        new Chart(document.getElementById('zonePrimeChart'), {{
            type: 'doughnut',
            data: {{
                labels: zoneLabels,
                datasets: [{{
                    data: zonePrimes,
                    backgroundColor: zoneColors,
                    borderWidth: 2,
                    borderColor: '#1a2235',
                    hoverOffset: 15
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{
                    legend: {{ position: 'bottom', labels: {{ padding: 15, usePointStyle: true }} }},
                    tooltip: {{
                        callbacks: {{
                            label: (ctx) => {{
                                return ctx.label + ': ' + ctx.raw.toLocaleString() + ' د.ج';
                            }}
                        }}
                    }}
                }},
                cutout: '55%'
            }}
        }});
        
        // 3. Trend Chart
        new Chart(document.getElementById('trendChart'), {{
            type: 'line',
            data: {{
                labels: {json.dumps(trend_years)},
                datasets: [
                    {{
                        label: 'إجمالي الوثائق',
                        data: {json.dumps(trend_totals)},
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59,130,246,0.1)',
                        fill: true,
                        tension: 0.4,
                        pointRadius: 6,
                        pointHoverRadius: 8,
                        yAxisID: 'y'
                    }},
                    {{
                        label: 'وثائق Zone III',
                        data: {json.dumps(trend_zone3)},
                        borderColor: '#8e44ad',
                        backgroundColor: 'rgba(142,68,173,0.1)',
                        fill: true,
                        tension: 0.4,
                        pointRadius: 6,
                        yAxisID: 'y'
                    }},
                    {{
                        label: 'وثائق Zone IIb',
                        data: {json.dumps(trend_zone2b)},
                        borderColor: '#e74c3c',
                        backgroundColor: 'rgba(231,76,60,0.1)',
                        fill: true,
                        tension: 0.4,
                        pointRadius: 6,
                        yAxisID: 'y'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
                scales: {{
                    y: {{ beginAtZero: true, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}
                }}
            }}
        }});
        
        // 4. RPA Distribution
        const rpaZoneCounts = {{}};
        const rpaData = {json.dumps(rpa_ref_rows)};
        rpaData.forEach(r => {{
            if (!rpaZoneCounts[r.zone]) rpaZoneCounts[r.zone] = 0;
            rpaZoneCounts[r.zone]++;
        }});
        const rpaLabels = ['0', 'I', 'IIa', 'IIb', 'III'];
        const rpaColors = ['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c', '#8e44ad'];
        new Chart(document.getElementById('rpaDistChart'), {{
            type: 'bar',
            data: {{
                labels: rpaLabels.map(z => 'Zone ' + z),
                datasets: [{{
                    label: 'عدد الولايات',
                    data: rpaLabels.map(z => rpaZoneCounts[z] || 0),
                    backgroundColor: rpaColors.map(c => c + '99'),
                    borderColor: rpaColors,
                    borderWidth: 2,
                    borderRadius: 8,
                    barPercentage: 0.6
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    y: {{ beginAtZero: true, ticks: {{ stepSize: 2 }}, grid: {{ color: 'rgba(255,255,255,0.05)' }} }},
                    x: {{ grid: {{ display: false }} }}
                }}
            }}
        }});
        
        // 5. Zone Compare (Grouped Bar)
        new Chart(document.getElementById('zoneCompareChart'), {{
            type: 'bar',
            data: {{
                labels: zoneLabels,
                datasets: [
                    {{
                        label: 'نسبة الوثائق %',
                        data: zoneCounts.map(c => +((c / {latest['total_policies']}) * 100).toFixed(1)),
                        backgroundColor: 'rgba(59,130,246,0.7)',
                        borderRadius: 6,
                        barPercentage: 0.4
                    }},
                    {{
                        label: 'نسبة الأقساط %',
                        data: zonePrimes.map(p => +((p / {round(latest['total_prime'], 2)}) * 100).toFixed(1)),
                        backgroundColor: 'rgba(139,92,246,0.7)',
                        borderRadius: 6,
                        barPercentage: 0.4
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
                scales: {{
                    y: {{ beginAtZero: true, grid: {{ color: 'rgba(255,255,255,0.05)' }} }},
                    x: {{ grid: {{ display: false }} }}
                }}
            }}
        }});
        
        // 6. Zone × Type (Stacked Bar)
        const matrixData = {json.dumps(matrix_data)};
        const allTypes = [...new Set(Object.values(matrixData).flatMap(z => Object.keys(z)))];
        const typeColors = ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6'];
        new Chart(document.getElementById('zoneTypeChart'), {{
            type: 'bar',
            data: {{
                labels: {json.dumps(list(zone_labels.values()))},
                datasets: allTypes.map((t, i) => ({{
                    label: t,
                    data: {json.dumps(all_zones_order)}.map(z => matrixData[z] ? (matrixData[z][t] || 0) : 0),
                    backgroundColor: typeColors[i % typeColors.length] + '99',
                    borderColor: typeColors[i % typeColors.length],
                    borderWidth: 1,
                    borderRadius: 4,
                }}))
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
                scales: {{
                    x: {{ stacked: true, grid: {{ display: false }} }},
                    y: {{ stacked: true, beginAtZero: true, grid: {{ color: 'rgba(255,255,255,0.05)' }} }}
                }}
            }}
        }});
        
        // 7. Type Distribution (Pie)
        new Chart(document.getElementById('typeDistChart'), {{
            type: 'pie',
            data: {{
                labels: {json.dumps(type_names)},
                datasets: [{{
                    data: {json.dumps(type_counts)},
                    backgroundColor: ['#3b82f6', '#ef4444', '#10b981', '#f59e0b', '#8b5cf6'],
                    borderWidth: 2,
                    borderColor: '#1a2235'
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true, padding: 15 }} }} }}
            }}
        }});
        
        // 8. Composite Risk Chart (Radar)
        const zones = ['0', 'I', 'IIa', 'IIb', 'III'];
        const zoneRisk = [0, 1, 2, 3, 4];
        const zonePolicyPct = zoneCounts.slice(0,5).map(c => +((c / {latest['total_policies']}) * 100).toFixed(1));
        const zonePrimePct = zonePrimes.slice(0,5).map(p => +((p / {round(latest['total_prime'], 2)}) * 100).toFixed(1));
        new Chart(document.getElementById('compositeRiskChart'), {{
            type: 'radar',
            data: {{
                labels: zones.map(z => 'Zone ' + z),
                datasets: [
                    {{
                        label: 'نسبة الوثائق %',
                        data: zonePolicyPct,
                        borderColor: '#3b82f6',
                        backgroundColor: 'rgba(59,130,246,0.15)',
                        pointBackgroundColor: '#3b82f6'
                    }},
                    {{
                        label: 'نسبة الأقساط %',
                        data: zonePrimePct,
                        borderColor: '#ef4444',
                        backgroundColor: 'rgba(239,68,68,0.15)',
                        pointBackgroundColor: '#ef4444'
                    }},
                    {{
                        label: 'معامل الخطر (x10)',
                        data: zoneRisk.map(r => r * 10),
                        borderColor: '#f59e0b',
                        backgroundColor: 'rgba(245,158,11,0.1)',
                        pointBackgroundColor: '#f59e0b'
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ position: 'bottom', labels: {{ usePointStyle: true }} }} }},
                scales: {{
                    r: {{
                        beginAtZero: true,
                        grid: {{ color: 'rgba(255,255,255,0.05)' }},
                        angleLines: {{ color: 'rgba(255,255,255,0.05)' }},
                        pointLabels: {{ font: {{ size: 12 }} }}
                    }}
                }}
            }}
        }});
        
        // 9. Wilaya Bar Chart
        const wilayaData = {json.dumps(wilaya_rows[:15])};
        new Chart(document.getElementById('wilayaBarChart'), {{
            type: 'bar',
            data: {{
                labels: wilayaData.map(w => w.name),
                datasets: [{{
                    label: 'عدد الوثائق',
                    data: wilayaData.map(w => w.count),
                    backgroundColor: wilayaData.map(w => {{
                        const zc = {json.dumps(zone_colors)};
                        return (zc[w.zone] || '#666') + 'cc';
                    }}),
                    borderColor: wilayaData.map(w => {{
                        const zc = {json.dumps(zone_colors)};
                        return zc[w.zone] || '#666';
                    }}),
                    borderWidth: 2,
                    borderRadius: 6
                }}]
            }},
            options: {{
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
                scales: {{
                    x: {{ beginAtZero: true, grid: {{ color: 'rgba(255,255,255,0.05)' }} }},
                    y: {{ grid: {{ display: false }} }}
                }}
            }}
        }});
    </script>
</body>
</html>"""
    
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"\n✅ Dashboard generated: {output_path}")


# ============================================================================
# 6. MAIN EXECUTION
# ============================================================================

def main():
    print("=" * 70)
    print("المرحلة الأولى: تحديد وتصنيف المخاطر الزلزالية")
    print("Phase 1: Risk Identification & Classification (RPA 99/2003)")
    print("=" * 70)
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(base_dir, "data")
    
    files = [
        ("2023", os.path.join(data_dir, "catnat_2023.xlsx")),
        ("2024", os.path.join(data_dir, "Catnat_2024.xlsx")),
        ("2025", os.path.join(data_dir, "catnat_2025.xlsx")),
    ]
    
    analyses = []
    
    for year, filepath in files:
        print(f"\n📂 Processing {year}...")
        records = read_excel_data(filepath)
        analysis = analyze_portfolio(records, year)
        analyses.append(analysis)
        
        print(f"  ✅ {analysis['total_policies']:,} policies | Prime: {analysis['total_prime']:,.0f} DA")
        print(f"  📊 Zone distribution:")
        for z in ["0", "I", "IIa", "IIb", "III", "NC"]:
            if z in analysis["by_zone"]:
                count = analysis["by_zone"][z]["count"]
                pct = round(count / analysis["total_policies"] * 100, 1)
                print(f"     Zone {z}: {count:,} ({pct}%)")
    
    # Generate dashboard
    output_html = os.path.join(base_dir, "phase1_rpa_dashboard.html")
    generate_dashboard(analyses, output_html)
    
    # Print summary
    latest = analyses[-1]
    print("\n" + "=" * 70)
    print("📊 SUMMARY — PHASE 1 RESULTS")
    print("=" * 70)
    
    high_risk = latest["by_zone"].get("III", {}).get("count", 0) + latest["by_zone"].get("IIb", {}).get("count", 0)
    high_risk_pct = round(high_risk / latest["total_policies"] * 100, 1)
    
    print(f"  📋 Total Policies: {latest['total_policies']:,}")
    print(f"  💰 Total Premium: {latest['total_prime']:,.0f} DA")
    print(f"  ⚠️  High Risk (IIb+III): {high_risk:,} ({high_risk_pct}%)")
    print(f"  🏙️  Wilayas Covered: {len(latest['by_wilaya'])}")
    print(f"  🏘️  Communes Covered: {len(latest['by_commune'])}")
    print(f"  📋 NULL Types: {latest['null_types']:,} ({round(latest['null_types']/latest['total_policies']*100,1)}%)")
    print(f"  📋 NULL Values: {latest['null_valeurs']:,} ({round(latest['null_valeurs']/latest['total_policies']*100,1)}%)")
    
    print(f"\n✅ Dashboard saved to: {output_html}")
    print("   Open in a browser to view the interactive analysis.")


if __name__ == "__main__":
    main()
