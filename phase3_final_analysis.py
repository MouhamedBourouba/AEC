"""
=============================================================================
المرحلة الثالثة: تشخيص التوازن والتوصيات + المخرجات النهائية
Phase 3: Balance Diagnosis & Recommendations + Final Deliverables
=============================================================================

Outputs:
  1. GIS-style interactive map of portfolio on RPA zones
  2. Accumulation dashboard by wilaya and risk type
  3. Strategic recommendations memo
  4. Vulnerability indicators and balance diagnosis
"""

import openpyxl
import json
import os
import re
import math
from collections import defaultdict

# ============================================================================
# 1. DATA MAPPINGS (from Phase 1 & 2)
# ============================================================================

RPA_ZONES = {
    1:{"zone":"0"},8:{"zone":"0"},11:{"zone":"0"},30:{"zone":"0"},33:{"zone":"0"},
    37:{"zone":"0"},39:{"zone":"0"},45:{"zone":"0"},47:{"zone":"0"},49:{"zone":"0"},
    50:{"zone":"0"},53:{"zone":"0"},54:{"zone":"0"},56:{"zone":"0"},
    3:{"zone":"I"},7:{"zone":"I"},12:{"zone":"I"},17:{"zone":"I"},20:{"zone":"I"},
    40:{"zone":"I"},51:{"zone":"I"},55:{"zone":"I"},57:{"zone":"I"},
    4:{"zone":"IIa"},5:{"zone":"IIa"},13:{"zone":"IIa"},14:{"zone":"IIa"},
    22:{"zone":"IIa"},24:{"zone":"IIa"},29:{"zone":"IIa"},31:{"zone":"IIa"},
    32:{"zone":"IIa"},36:{"zone":"IIa"},41:{"zone":"IIa"},46:{"zone":"IIa"},48:{"zone":"IIa"},
    6:{"zone":"IIb"},10:{"zone":"IIb"},18:{"zone":"IIb"},19:{"zone":"IIb"},
    21:{"zone":"IIb"},23:{"zone":"IIb"},25:{"zone":"IIb"},26:{"zone":"IIb"},
    27:{"zone":"IIb"},28:{"zone":"IIb"},34:{"zone":"IIb"},38:{"zone":"IIb"},
    43:{"zone":"IIb"},44:{"zone":"IIb"},
    2:{"zone":"III"},9:{"zone":"III"},15:{"zone":"III"},16:{"zone":"III"},
    35:{"zone":"III"},42:{"zone":"III"},
}

WILAYA_NAMES = {
    1:"ADRAR",2:"CHLEF",3:"LAGHOUAT",4:"OUM EL BOUAGHI",5:"BATNA",
    6:"BEJAIA",7:"BISKRA",8:"BECHAR",9:"BLIDA",10:"BOUIRA",
    11:"TAMANRASSET",12:"TEBESSA",13:"TLEMCEN",14:"TIARET",15:"TIZI OUZOU",
    16:"ALGER",17:"DJELFA",18:"JIJEL",19:"SETIF",20:"SAIDA",
    21:"SKIKDA",22:"SIDI BEL ABBES",23:"ANNABA",24:"GUELMA",25:"CONSTANTINE",
    26:"MEDEA",27:"MOSTAGANEM",28:"M'SILA",29:"MASCARA",30:"OUARGLA",
    31:"ORAN",32:"EL BAYADH",33:"ILLIZI",34:"B.B. ARRERIDJ",35:"BOUMERDES",
    36:"EL TAREF",37:"TINDOUF",38:"TISSEMSILT",39:"EL OUED",40:"KHENCHELA",
    41:"SOUK AHRAS",42:"TIPAZA",43:"MILA",44:"AIN DEFLA",45:"NAAMA",
    46:"AIN TIMOUCHENT",47:"GHARDAIA",48:"RELIZANE",
}

# Approximate wilaya center coordinates for map (lat, lon)
WILAYA_COORDS = {
    1:(27.87,-0.29),2:(36.17,1.33),3:(33.80,2.88),4:(35.88,7.11),5:(35.56,6.17),
    6:(36.75,5.08),7:(34.85,5.73),8:(31.62,-2.22),9:(36.47,2.83),10:(36.38,3.90),
    11:(22.79,5.53),12:(35.40,8.12),13:(34.88,-1.32),14:(35.37,1.32),15:(36.71,4.05),
    16:(36.75,3.06),17:(34.67,3.25),18:(36.82,5.77),19:(36.19,5.41),20:(34.83,0.15),
    21:(36.88,6.91),22:(35.19,-0.63),23:(36.90,7.77),24:(36.46,7.43),25:(36.36,6.61),
    26:(36.26,2.75),27:(35.93,0.09),28:(35.71,4.54),29:(35.40,0.14),30:(31.95,5.33),
    31:(35.70,-0.63),32:(33.69,1.02),33:(26.51,8.47),34:(35.98,4.76),35:(36.77,3.47),
    36:(36.77,8.31),37:(27.67,-8.15),38:(35.61,1.81),39:(33.37,6.85),40:(35.43,7.14),
    41:(36.29,7.95),42:(36.59,2.45),43:(36.45,6.26),44:(36.26,1.97),45:(33.27,-0.31),
    46:(35.29,-1.14),47:(32.49,3.67),48:(35.73,0.56),
}

CATNAT_RATES = {"0":0.0005,"I":0.00075,"IIa":0.001,"IIb":0.0015,"III":0.002,"NC":0.001}
ZONE_RISK_COEFF = {"0":0.00,"I":0.10,"IIa":0.15,"IIb":0.20,"III":0.25,"NC":0.05}
DAMAGE_RATIOS = {
    "0":{"50":0.00,"100":0.01,"250":0.02,"475":0.05},
    "I":{"50":0.01,"100":0.03,"250":0.06,"475":0.10},
    "IIa":{"50":0.02,"100":0.05,"250":0.10,"475":0.18},
    "IIb":{"50":0.04,"100":0.08,"250":0.15,"475":0.25},
    "III":{"50":0.06,"100":0.12,"250":0.22,"475":0.35},
    "NC":{"50":0.03,"100":0.06,"250":0.12,"475":0.20},
}
COMPANY_RETENTION = 500_000_000

# ============================================================================
# 2. DATA LOADING
# ============================================================================

def extract_code(s):
    if not s or s=='NULL': return None
    m=re.match(r'(\d+)',str(s).strip())
    return int(m.group(1)) if m else None

def extract_name(s):
    if not s or s=='NULL': return "INCONNU"
    m=re.match(r'\d+\s*-\s*(.*)',str(s).strip())
    return m.group(1).strip() if m and m.group(1).strip() else str(s).strip()

def extract_type(s):
    if not s or s=='NULL' or not str(s).strip(): return "NULL"
    m=re.match(r'\d+\s*-\s*(.*)',str(s).strip())
    return m.group(1).strip() if m and m.group(1).strip() else str(s).strip()

def load_data():
    base = os.path.dirname(os.path.abspath(__file__))
    fp = os.path.join(base,"data","catnat_2025.xlsx")
    print(f"  Loading {fp}...")
    wb = openpyxl.load_workbook(fp, read_only=True)
    ws = wb.active
    recs = []
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or len(row)<9: continue
        wc = extract_code(row[4])
        zone = RPA_ZONES.get(wc,{}).get("zone","NC") if wc else "NC"
        prime = 0.0
        try: prime = float(row[8]) if row[8] and row[8]!='NULL' else 0.0
        except: prime = 0.0
        rate = CATNAT_RATES.get(zone,0.001)
        est_si = prime/rate if rate>0 else 0

        tn = extract_type(row[3])
        if "industrielle" in tn.lower(): tg,tga = "Industrielle","صناعية"
        elif "commerciale" in tn.lower(): tg,tga = "Commerciale","تجارية"
        else: tg,tga = "Non classé","غير محدد"

        recs.append({
            "wc":wc,"wn":extract_name(row[4]),"commune":extract_name(row[5]),
            "zone":zone,"prime":prime,"si":est_si,"tg":tg,"tga":tga
        })
    wb.close()
    print(f"  => {len(recs)} records")
    return recs

# ============================================================================
# 3. ANALYSIS
# ============================================================================

def analyze(recs):
    R = {"n":len(recs),"prime":sum(r["prime"] for r in recs),"si":sum(r["si"] for r in recs)}

    # By wilaya
    bw = defaultdict(lambda:{"n":0,"prime":0,"si":0,"zone":"","communes":set(),"types":defaultdict(lambda:{"n":0,"si":0})})
    for r in recs:
        w=r["wn"]
        bw[w]["n"]+=1; bw[w]["prime"]+=r["prime"]; bw[w]["si"]+=r["si"]
        bw[w]["zone"]=r["zone"]; bw[w]["communes"].add(r["commune"])
        bw[w]["types"][r["tg"]]["n"]+=1; bw[w]["types"][r["tg"]]["si"]+=r["si"]
        bw[w]["code"]=r["wc"]
    R["bw"]=dict(bw)

    # By zone
    bz = defaultdict(lambda:{"n":0,"prime":0,"si":0,"wilayas":set()})
    for r in recs:
        bz[r["zone"]]["n"]+=1; bz[r["zone"]]["prime"]+=r["prime"]
        bz[r["zone"]]["si"]+=r["si"]; bz[r["zone"]]["wilayas"].add(r["wn"])
    R["bz"]=dict(bz)

    # By type
    bt = defaultdict(lambda:{"n":0,"prime":0,"si":0})
    for r in recs:
        bt[r["tg"]]["n"]+=1; bt[r["tg"]]["prime"]+=r["prime"]; bt[r["tg"]]["si"]+=r["si"]
    R["bt"]=dict(bt)

    # By commune
    bc = defaultdict(lambda:{"n":0,"prime":0,"si":0,"wn":"","zone":""})
    for r in recs:
        k=f"{r['commune']}|{r['wn']}"
        bc[k]["n"]+=1; bc[k]["prime"]+=r["prime"]; bc[k]["si"]+=r["si"]
        bc[k]["wn"]=r["wn"]; bc[k]["zone"]=r["zone"]
    R["bc"]=dict(bc)

    # Vulnerability indicators
    vi = {}
    for w,d in bw.items():
        z = d["zone"]
        coeff = ZONE_RISK_COEFF.get(z, 0.05)
        # Indicator: SI × risk_coefficient (weighted exposure)
        weighted_exposure = d["si"] * coeff
        # Concentration ratio: wilaya SI / total SI
        concentration = d["si"] / R["si"] if R["si"]>0 else 0
        # Risk-adjusted concentration: concentration * risk_level
        risk_adj = concentration * ({"0":1,"I":2,"IIa":3,"IIb":4,"III":5,"NC":1}.get(z,1))
        # Balance score: ideal = proportional to population, actual = concentration
        # Higher = more overexposed
        vi[w] = {
            "zone": z, "si": d["si"], "prime": d["prime"], "n": d["n"],
            "coeff": coeff, "weighted_exp": weighted_exposure,
            "concentration": concentration, "risk_adj": risk_adj,
            "exceeds": d["si"] > COMPANY_RETENTION,
            "ratio_ret": d["si"] / COMPANY_RETENTION,
            "pml_250": d["si"] * DAMAGE_RATIOS.get(z,DAMAGE_RATIOS["NC"])["250"],
            "pml_475": d["si"] * DAMAGE_RATIOS.get(z,DAMAGE_RATIOS["NC"])["475"],
        }
    R["vi"] = vi

    # Target portfolio distribution (ideal balance)
    # Zone 0+I should hold ~30-40% of SI, IIa ~25-30%, IIb ~20-25%, III ~10-15%
    target = {"0":15,"I":15,"IIa":25,"IIb":25,"III":15,"NC":5}
    actual = {}
    for z in ["0","I","IIa","IIb","III","NC"]:
        zd = bz.get(z,{"si":0})
        actual[z] = round(zd["si"]/R["si"]*100,1) if R["si"]>0 else 0
    R["target"] = target
    R["actual"] = actual
    R["gap"] = {z: round(actual.get(z,0) - target.get(z,0), 1) for z in target}

    return R

# ============================================================================
# 4. DASHBOARD GENERATION
# ============================================================================

def fmt(n): return f"{n:,.0f}"
def fmt_b(n):
    if abs(n)>=1e9: return f"{n/1e9:.2f} Mrd"
    if abs(n)>=1e6: return f"{n/1e6:.1f} M"
    return fmt(n)

def gen_dashboard(A, path):
    zones = ["0","I","IIa","IIb","III","NC"]
    zc = {"0":"#22c55e","I":"#eab308","IIa":"#f97316","IIb":"#ef4444","III":"#a855f7","NC":"#6b7280"}
    zl = {"0":"Zone 0","I":"Zone I","IIa":"Zone IIa","IIb":"Zone IIb","III":"Zone III","NC":"N/C"}
    zla = {"0":"ضعيفة جدًا","I":"ضعيفة","IIa":"متوسطة","IIb":"مرتفعة","III":"مرتفعة جدًا","NC":"—"}

    # Prepare wilaya map data
    map_data = []
    for w,d in sorted(A["bw"].items(), key=lambda x:-x[1]["si"]):
        code = d.get("code")
        if code and code in WILAYA_COORDS:
            lat,lon = WILAYA_COORDS[code]
            map_data.append({
                "name":w,"code":code,"lat":lat,"lon":lon,
                "zone":d["zone"],"n":d["n"],"si":round(d["si"]),"prime":round(d["prime"]),
                "communes":len(d["communes"]),"color":zc.get(d["zone"],"#666"),
                "exceeds": d["si"]>COMPANY_RETENTION,
                "radius": max(6, min(45, math.sqrt(d["si"]/1e8)*3)),
            })

    # Wilaya accumulation table sorted by risk-adjusted concentration
    vi_sorted = sorted(A["vi"].items(), key=lambda x:-x[1]["risk_adj"])

    # Actual vs target data
    actual_data = [A["actual"].get(z,0) for z in zones]
    target_data = [A["target"].get(z,0) for z in zones]
    gap_data = [A["gap"].get(z,0) for z in zones]

    # Top 20 wilayas by SI for accum chart
    top20 = sorted(A["bw"].items(), key=lambda x:-x[1]["si"])[:20]

    # Zone × Type heatmap data
    zt_data = {}
    for w,d in A["bw"].items():
        z = d["zone"]
        if z not in zt_data: zt_data[z] = {"Industrielle":0,"Commerciale":0,"Non classé":0}
        for t,td in d["types"].items():
            if t in zt_data[z]: zt_data[z][t] += td["si"]

    # Compute summary metrics
    total_si = A["si"]
    high_risk_si = A["bz"].get("III",{"si":0})["si"] + A["bz"].get("IIb",{"si":0})["si"]
    high_risk_pct = round(high_risk_si/total_si*100,1) if total_si else 0
    n_exceed = sum(1 for v in A["vi"].values() if v["exceeds"])
    total_pml475 = sum(v["pml_475"] for v in A["vi"].values())
    max_wilaya = max(A["vi"].items(), key=lambda x:x[1]["si"])

    # Zone imbalance severity
    max_over = max(A["gap"].values())
    max_under = min(A["gap"].values())
    overexposed_zones = [z for z in zones if A["gap"].get(z,0) > 5]
    underexposed_zones = [z for z in zones if A["gap"].get(z,0) < -5]

    html = f"""<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>المرحلة الثالثة: تشخيص التوازن والتوصيات</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Cairo:wght@300;400;600;700;800;900&family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {{
    --bg: #060a14; --bg2: #0f1729; --card: #141d30; --card2: #1a2540;
    --text: #e2e8f0; --text2: #7c8db0; --muted: #4a5a78;
    --blue: #3b82f6; --purple: #8b5cf6; --green: #10b981; --amber: #f59e0b;
    --red: #ef4444; --orange: #f97316; --cyan: #06b6d4; --pink: #ec4899;
    --border: rgba(255,255,255,0.05);
    --r: 14px; --rs: 8px;
}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Cairo','Inter',sans-serif;background:var(--bg);color:var(--text);direction:rtl;min-height:100vh}}
body::before{{content:'';position:fixed;inset:0;background:radial-gradient(ellipse 700px 500px at 20% 10%,rgba(139,92,246,0.07),transparent),radial-gradient(ellipse 600px 400px at 80% 80%,rgba(59,130,246,0.05),transparent);z-index:-1}}
.c{{max-width:1500px;margin:0 auto;padding:22px 32px}}
.hdr{{text-align:center;margin-bottom:32px;padding:32px;background:linear-gradient(135deg,rgba(168,85,247,0.12),rgba(6,182,212,0.08));border-radius:var(--r);border:1px solid var(--border);position:relative;overflow:hidden}}
.hdr::after{{content:'';position:absolute;inset:-50%;background:conic-gradient(from 0deg,transparent,rgba(168,85,247,0.03),transparent,rgba(6,182,212,0.02),transparent);animation:sp 30s linear infinite}}
@keyframes sp{{100%{{transform:rotate(360deg)}}}}
.hdr-c{{position:relative;z-index:1}}
.hdr h1{{font-size:2rem;font-weight:900;background:linear-gradient(135deg,#a855f7,#06b6d4,#3b82f6);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px;line-height:1.5}}
.hdr .sub{{font-size:0.95rem;color:var(--text2)}}
.hdr .bdg{{display:inline-block;background:rgba(168,85,247,0.15);color:var(--purple);padding:5px 16px;border-radius:25px;font-size:0.8rem;font-weight:600;margin-top:10px;border:1px solid rgba(168,85,247,0.25)}}
.tabs{{display:flex;gap:5px;margin-bottom:25px;padding:4px;background:var(--bg2);border-radius:var(--r);border:1px solid var(--border);overflow-x:auto;flex-wrap:nowrap}}
.tab{{padding:10px 20px;border-radius:var(--rs);cursor:pointer;font-weight:600;font-size:0.85rem;color:var(--text2);transition:all .3s;white-space:nowrap;border:none;background:none;font-family:inherit}}
.tab:hover{{color:var(--text);background:rgba(255,255,255,0.04)}}
.tab.on{{background:linear-gradient(135deg,var(--purple),var(--cyan));color:#fff;box-shadow:0 4px 12px rgba(139,92,246,.3)}}
.tc{{display:none;animation:fi .4s ease}}.tc.on{{display:block}}
@keyframes fi{{from{{opacity:0;transform:translateY(6px)}}to{{opacity:1;transform:translateY(0)}}}}
.kg{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:24px}}
.kc{{background:var(--card);border-radius:var(--r);padding:20px;border:1px solid var(--border);transition:all .3s}}
.kc:hover{{transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.3)}}
.ki{{width:40px;height:40px;border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.2rem;margin-bottom:10px}}
.kv{{font-size:1.5rem;font-weight:800;margin-bottom:3px;line-height:1.2}}
.kl{{color:var(--text2);font-size:0.8rem;font-weight:500}}
.ks{{font-size:0.72rem;margin-top:5px;padding:2px 8px;border-radius:15px;display:inline-block}}
.ks-d{{background:rgba(239,68,68,.15);color:var(--red)}}.ks-w{{background:rgba(245,158,11,.15);color:var(--amber)}}.ks-g{{background:rgba(16,185,129,.15);color:var(--green)}}
.cg{{display:grid;grid-template-columns:repeat(auto-fit,minmax(460px,1fr));gap:20px;margin-bottom:24px}}
.cc{{background:var(--card);border-radius:var(--r);padding:22px;border:1px solid var(--border)}}
.cc h3{{font-size:1rem;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
.cc h3 .ic{{width:28px;height:28px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:.9rem}}
.cw{{position:relative;height:320px}}.cw.t{{height:420px}}.cw.s{{height:260px}}
.tc-box{{background:var(--card);border-radius:var(--r);padding:22px;border:1px solid var(--border);margin-bottom:20px;overflow-x:auto}}
.tc-box h3{{font-size:1rem;font-weight:700;margin-bottom:16px;display:flex;align-items:center;gap:8px}}
table{{width:100%;border-collapse:collapse;font-size:.82rem}}
th{{background:rgba(168,85,247,.1);color:var(--purple);font-weight:700;padding:10px 12px;text-align:right;white-space:nowrap;border-bottom:2px solid rgba(168,85,247,.2)}}
td{{padding:9px 12px;border-bottom:1px solid var(--border)}}tr:hover td{{background:rgba(255,255,255,.02)}}
.zb{{display:inline-block;padding:2px 10px;border-radius:15px;font-size:.75rem;font-weight:700}}
.z0{{background:rgba(34,197,94,.15);color:#22c55e}}.zI{{background:rgba(234,179,8,.15);color:#eab308}}
.z2a{{background:rgba(249,115,22,.15);color:#f97316}}.z2b{{background:rgba(239,68,68,.15);color:#ef4444}}
.z3{{background:rgba(168,85,247,.15);color:#a855f7}}.zN{{background:rgba(107,114,128,.15);color:#6b7280}}
.hot{{color:var(--red);font-weight:700}}.safe{{color:var(--green)}}
.ab{{padding:16px 20px;border-radius:var(--rs);margin-bottom:20px;border:1px solid;font-size:.88rem;line-height:1.8}}
.ab-d{{background:rgba(239,68,68,.06);border-color:rgba(239,68,68,.15);color:#fca5a5}}
.ab-w{{background:rgba(245,158,11,.06);border-color:rgba(245,158,11,.15);color:#fcd34d}}
.ab-i{{background:rgba(59,130,246,.06);border-color:rgba(59,130,246,.15);color:#93c5fd}}
.ab-s{{background:rgba(16,185,129,.06);border-color:rgba(16,185,129,.15);color:#86efac}}
/* Map */
.map-container{{position:relative;width:100%;height:600px;background:var(--card);border-radius:var(--r);border:1px solid var(--border);overflow:hidden;margin-bottom:20px}}
.map-svg{{width:100%;height:100%}}
.map-legend{{position:absolute;bottom:15px;right:15px;background:rgba(10,14,26,.9);padding:14px 18px;border-radius:var(--rs);border:1px solid var(--border);font-size:.78rem}}
.map-legend .leg-item{{display:flex;align-items:center;gap:8px;margin-bottom:5px}}
.map-legend .leg-dot{{width:12px;height:12px;border-radius:50%;flex-shrink:0}}
.map-title{{position:absolute;top:15px;right:15px;background:rgba(10,14,26,.9);padding:10px 16px;border-radius:var(--rs);border:1px solid var(--border)}}
.map-title h4{{font-size:.95rem;font-weight:700;color:var(--text)}}
.map-title p{{font-size:.75rem;color:var(--text2)}}
/* Rec cards */
.rec-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:18px;margin-bottom:24px}}
.rec-card{{background:var(--card);border-radius:var(--r);padding:22px;border-left:4px solid;transition:all .3s}}
.rec-card:hover{{transform:translateY(-2px);box-shadow:0 6px 20px rgba(0,0,0,.3)}}
.rec-card h4{{font-size:.95rem;font-weight:700;margin-bottom:8px}}
.rec-card p{{font-size:.82rem;color:var(--text2);line-height:1.8}}
.rec-card .priority{{font-size:.72rem;padding:2px 8px;border-radius:12px;display:inline-block;margin-bottom:8px;font-weight:600}}
.gauge-row{{display:flex;gap:15px;margin-bottom:24px;flex-wrap:wrap}}
.gauge{{flex:1;min-width:200px;background:var(--card);border-radius:var(--r);padding:18px;border:1px solid var(--border);text-align:center}}
.gauge .g-label{{font-size:.8rem;color:var(--text2);margin-bottom:8px}}
.gauge .g-value{{font-size:1.8rem;font-weight:800;margin-bottom:4px}}
.gauge .g-bar{{height:8px;background:rgba(255,255,255,.08);border-radius:4px;overflow:hidden;margin-top:8px}}
.gauge .g-fill{{height:100%;border-radius:4px;transition:width 1.5s ease}}
.gauge .g-desc{{font-size:.72rem;color:var(--muted);margin-top:6px}}
@media(max-width:768px){{.c{{padding:12px}}.cg{{grid-template-columns:1fr}}.kg{{grid-template-columns:repeat(2,1fr)}}.hdr h1{{font-size:1.3rem}}}}
</style>
</head>
<body>
<div class="c">
<div class="hdr"><div class="hdr-c">
<h1>التقرير النهائي: تشخيص التوازن والتوصيات الاستراتيجية</h1>
<p class="sub">Phase 3: Balance Diagnosis, GIS Mapping & Strategic Recommendations</p>
<span class="bdg">🎯 دراسة شاملة — محفظة CATNAT 2025</span>
</div></div>

<div class="tabs">
<button class="tab on" onclick="st('diag')">🎯 تشخيص التوازن</button>
<button class="tab" onclick="st('map')">🗺️ خريطة GIS</button>
<button class="tab" onclick="st('accum')">📊 لوحة التراكمات</button>
<button class="tab" onclick="st('vuln')">⚡ مؤشرات الهشاشة</button>
<button class="tab" onclick="st('recs')">💡 التوصيات الاستراتيجية</button>
</div>

<!-- ============ DIAGNOSIS ============ -->
<div id="t-diag" class="tc on">
<div class="ab ab-d">
🚨 <strong>تشخيص عام:</strong> المحفظة تعاني من <strong>اختلال كبير</strong> في التوزيع الجغرافي. {high_risk_pct}% من رأس المال يتركز في المناطق الأعلى خطورة (IIb+III)، بينما المناطق الأقل خطورة (0+I) لا تمثل سوى {round(A["actual"].get("0",0)+A["actual"].get("I",0),1)}%.
</div>

<div class="kg">
<div class="kc"><div class="ki" style="background:rgba(168,85,247,.15)">🏦</div><div class="kv" style="color:var(--purple)">{fmt_b(total_si)} د.ج</div><div class="kl">رأس المال المقدر</div></div>
<div class="kc"><div class="ki" style="background:rgba(239,68,68,.15)">⚠️</div><div class="kv" style="color:var(--red)">{high_risk_pct}%</div><div class="kl">تركيز عالي الخطورة</div><span class="ks ks-d">IIb + III</span></div>
<div class="kc"><div class="ki" style="background:rgba(249,115,22,.15)">🔥</div><div class="kv" style="color:var(--orange)">{n_exceed}</div><div class="kl">ولايات متجاوزة للاحتفاظ</div></div>
<div class="kc"><div class="ki" style="background:rgba(6,182,212,.15)">📊</div><div class="kv" style="color:var(--cyan)">{fmt_b(total_pml475)}</div><div class="kl">PML-475 الإجمالي</div><span class="ks ks-d">{round(total_pml475/COMPANY_RETENTION*100)}% احتفاظ</span></div>
<div class="kc"><div class="ki" style="background:rgba(236,72,153,.15)">🎯</div><div class="kv" style="color:var(--pink)">{max_wilaya[0]}</div><div class="kl">أخطر ولاية</div><span class="ks ks-d">{round(max_wilaya[1]["ratio_ret"],1)}x احتفاظ</span></div>
</div>

<div class="cc" style="margin-bottom:20px">
<h3><span class="ic" style="background:rgba(168,85,247,.15)">📊</span> فجوة التوازن: التوزيع الفعلي vs المستهدف</h3>
<p style="color:var(--text2);font-size:.85rem;margin-bottom:15px;line-height:1.7">يقارن هذا المخطط بين التوزيع الفعلي لرأس المال حسب المنطقة الزلزالية والتوزيع المستهدف (المثالي) لتحقيق توازن المحفظة.</p>
<div class="cw"><canvas id="gapChart"></canvas></div>
</div>

<div class="gauge-row">
"""

    for z in zones:
        actual_pct = A["actual"].get(z,0)
        target_pct = A["target"].get(z,0)
        gap = A["gap"].get(z,0)
        color = zc[z]
        if gap > 10: status,scolor = "تركيز مفرط 🔴",var_red
        elif gap > 5: status,scolor = "فوق المستهدف 🟠","var(--orange)"
        elif gap < -10: status,scolor = "ضعف تركيز 🔵","var(--blue)"
        elif gap < -5: status,scolor = "تحت المستهدف 🟡","var(--amber)"
        else: status,scolor = "متوازن ✅","var(--green)"
        bar_w = min(actual_pct * 2.5, 100)
        html += f"""<div class="gauge">
<div class="g-label">{zl[z]} — {zla[z]}</div>
<div class="g-value" style="color:{color}">{actual_pct}%</div>
<div class="g-bar"><div class="g-fill" style="width:{bar_w}%;background:{color}"></div></div>
<div class="g-desc">المستهدف: {target_pct}% | الفجوة: {'+' if gap>0 else ''}{gap}pp</div>
<div style="margin-top:6px;font-size:.72rem;color:{scolor if 'var' in str(scolor) else scolor}">{status}</div>
</div>
"""

    html += """</div>
</div>

<!-- ============ GIS MAP ============ -->
<div id="t-map" class="tc">
<div class="ab ab-i">
🗺️ <strong>خريطة المحفظة على التقسيم الزلزالي RPA:</strong> خريطة تفاعلية — يمكنك التكبير والتصغير والسحب. انقر على أي دائرة لعرض التفاصيل. حجم الدائرة = رأس المال | اللون = المنطقة الزلزالية. استخدم عناصر التحكم لتصفية المناطق.
</div>
<div id="leaflet-map" style="width:100%;height:650px;border-radius:var(--r);border:1px solid var(--border);overflow:hidden;"></div>
<div style="display:flex;gap:12px;margin-top:14px;flex-wrap:wrap;">
<button class="map-filter-btn on" data-zone="all" onclick="filterZone('all',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(139,92,246,.2);color:var(--purple);font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(139,92,246,.3);transition:all .3s;">🌍 الكل</button>
<button class="map-filter-btn" data-zone="III" onclick="filterZone('III',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(168,85,247,.1);color:#a855f7;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(168,85,247,.2);transition:all .3s;">🟣 Zone III</button>
<button class="map-filter-btn" data-zone="IIb" onclick="filterZone('IIb',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(239,68,68,.1);color:#ef4444;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(239,68,68,.2);transition:all .3s;">🔴 Zone IIb</button>
<button class="map-filter-btn" data-zone="IIa" onclick="filterZone('IIa',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(249,115,22,.1);color:#f97316;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(249,115,22,.2);transition:all .3s;">🟠 Zone IIa</button>
<button class="map-filter-btn" data-zone="I" onclick="filterZone('I',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(234,179,8,.1);color:#eab308;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(234,179,8,.2);transition:all .3s;">🟡 Zone I</button>
<button class="map-filter-btn" data-zone="0" onclick="filterZone('0',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(34,197,94,.1);color:#22c55e;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(34,197,94,.2);transition:all .3s;">🟢 Zone 0</button>
<button class="map-filter-btn" data-zone="exceed" onclick="filterZone('exceed',this)" style="all:unset;cursor:pointer;padding:8px 18px;border-radius:var(--rs);background:rgba(239,68,68,.1);color:#ef4444;font-weight:600;font-size:.8rem;font-family:inherit;border:1px solid rgba(239,68,68,.3);transition:all .3s;">🔥 متجاوزة فقط</button>
</div>
</div>"""

    # Prepare Leaflet data as JSON
    leaflet_data = json.dumps(map_data, ensure_ascii=False)

    html += f"""

<!-- ============ ACCUMULATION ============ -->
<div id="t-accum" class="tc">
<div class="cg">
<div class="cc"><h3><span class="ic" style="background:rgba(239,68,68,.15)">🔥</span> تراكمات رأس المال vs حد الاحتفاظ</h3><div class="cw t"><canvas id="accumChart2"></canvas></div></div>
<div class="cc"><h3><span class="ic" style="background:rgba(168,85,247,.15)">📊</span> التوزيع الجغرافي لرأس المال</h3><div class="cw t"><canvas id="geoDistChart"></canvas></div></div>
</div>

<div class="tc-box">
<h3><span class="ic" style="background:rgba(249,115,22,.15)">📋</span> لوحة التراكمات: الولاية × نوع الخطر (أعلى 20)</h3>
<table>
<thead><tr><th>#</th><th>الولاية</th><th>المنطقة</th><th>صناعية</th><th>تجارية</th><th>غير محدد</th><th>الإجمالي (M DA)</th><th>% إجمالي</th><th>الحالة</th></tr></thead>
<tbody>
"""

    for i,(w,d) in enumerate(top20, 1):
        z = d["zone"]
        zclass = {"0":"z0","I":"zI","IIa":"z2a","IIb":"z2b","III":"z3","NC":"zN"}.get(z,"zN")
        ind_si = d["types"].get("Industrielle",{"si":0})["si"]
        com_si = d["types"].get("Commerciale",{"si":0})["si"]
        nul_si = d["types"].get("Non classé",{"si":0})["si"]
        total_w = d["si"]
        pct = round(total_w/total_si*100,1) if total_si else 0
        status = '<span class="hot">🔴 متجاوز</span>' if d["si"]>COMPANY_RETENTION else '<span class="safe">🟢 ضمن الحدود</span>'
        html += f'<tr><td>{i}</td><td><strong>{w}</strong></td><td><span class="zb {zclass}">{z}</span></td>'
        html += f'<td>{fmt_b(ind_si)}</td><td>{fmt_b(com_si)}</td><td>{fmt_b(nul_si)}</td>'
        html += f'<td><strong>{fmt_b(total_w)}</strong></td><td>{pct}%</td><td>{status}</td></tr>\n'

    html += """</tbody></table></div>

<div class="cc">
<h3><span class="ic" style="background:rgba(6,182,212,.15)">📊</span> هيكل المحفظة: المنطقة × نوع الخطر (رأس المال)</h3>
<div class="cw"><canvas id="ztHeatChart"></canvas></div>
</div>
</div>

<!-- ============ VULNERABILITY ============ -->
<div id="t-vuln" class="tc">
<div class="ab ab-w">
⚡ <strong>مؤشرات الهشاشة:</strong> تقيس هذه المؤشرات نسبة رأس المال المعرض للخطر مرجحة بمستوى الخطورة الزلزالية. كلما ارتفع المؤشر، زادت هشاشة الولاية.
</div>

<div class="tc-box">
<h3><span class="ic" style="background:rgba(239,68,68,.15)">⚡</span> مؤشرات الهشاشة حسب الولاية (أعلى 25)</h3>
<table>
<thead><tr><th>#</th><th>الولاية</th><th>المنطقة</th><th>رأس المال (M DA)</th><th>التركيز %</th><th>مؤشر الخطر المركب</th><th>PML-250 (M DA)</th><th>PML-475 (M DA)</th><th>التقييم</th></tr></thead>
<tbody>
"""

    for i,(w,v) in enumerate(vi_sorted[:25], 1):
        z = v["zone"]
        zclass = {"0":"z0","I":"zI","IIa":"z2a","IIb":"z2b","III":"z3","NC":"zN"}.get(z,"zN")
        risk_score = v["risk_adj"]
        if risk_score > 0.5: rating = '<span class="hot">🔴 هشاشة عالية جداً</span>'
        elif risk_score > 0.2: rating = '<span style="color:var(--orange)">🟠 هشاشة مرتفعة</span>'
        elif risk_score > 0.05: rating = '<span style="color:var(--amber)">🟡 هشاشة متوسطة</span>'
        else: rating = '<span class="safe">🟢 مقبول</span>'
        html += f'<tr><td>{i}</td><td><strong>{w}</strong></td><td><span class="zb {zclass}">{z}</span></td>'
        html += f'<td>{fmt_b(v["si"])}</td><td>{round(v["concentration"]*100,1)}%</td>'
        html += f'<td><strong>{risk_score:.3f}</strong></td>'
        html += f'<td>{fmt_b(v["pml_250"])}</td><td>{fmt_b(v["pml_475"])}</td>'
        html += f'<td>{rating}</td></tr>\n'

    html += """</tbody></table></div>

<div class="cg">
<div class="cc"><h3><span class="ic" style="background:rgba(239,68,68,.15)">📊</span> مؤشر الخطر المركب — أعلى 15 ولاية</h3><div class="cw t"><canvas id="vulnChart"></canvas></div></div>
<div class="cc"><h3><span class="ic" style="background:rgba(168,85,247,.15)">📊</span> رادار المخاطر حسب المنطقة</h3><div class="cw t"><canvas id="radarChart"></canvas></div></div>
</div>
</div>

<!-- ============ RECOMMENDATIONS ============ -->
<div id="t-recs" class="tc">
<div class="ab ab-s">
💡 <strong>مذكرة التوصيات الاستراتيجية</strong> — تستند هذه التوصيات إلى نتائج المراحل الثلاث من التحليل وتهدف إلى إعادة توازن المحفظة وتقليل المخاطر.
</div>

<h3 style="font-size:1.1rem;margin-bottom:18px;color:var(--red)">🔴 إجراءات عاجلة (0-3 أشهر)</h3>
<div class="rec-grid">
<div class="rec-card" style="border-color:var(--red)">
<span class="priority" style="background:rgba(239,68,68,.15);color:var(--red)">أولوية قصوى</span>
<h4>1. برنامج إعادة التأمين الكارثي</h4>
<p>ترتيب عقد إعادة تأمين Cat XL يغطي سيناريو 250 سنة على الأقل (~12.8 مليار د.ج). يُوصى بطبقات متعددة:<br>
• طبقة أولى: 0-500 مليون (احتفاظ)<br>
• طبقة ثانية: 500 مليون - 5 مليار<br>
• طبقة ثالثة: 5-15 مليار</p>
</div>
<div class="rec-card" style="border-color:var(--red)">
<span class="priority" style="background:rgba(239,68,68,.15);color:var(--red)">أولوية قصوى</span>
<h4>2. سقوف الاكتتاب — ولاية الجزائر</h4>
<p>فرض سقف اكتتاب فوري في ولاية الجزائر (37.9x قدرة الاحتفاظ). تحديد حد أقصى للقيمة المؤمنة الفردية في Zone III:<br>
• عقارات: 50 مليون د.ج<br>
• تجاري: 100 مليون د.ج<br>
• صناعي: 200 مليون د.ج</p>
</div>
<div class="rec-card" style="border-color:var(--red)">
<span class="priority" style="background:rgba(239,68,68,.15);color:var(--red)">أولوية قصوى</span>
<h4>3. استكمال البيانات الحرجة</h4>
<p>برنامج عاجل لاستكمال:<br>
• VALEUR_ASSURÉE لجميع الوثائق (100% ناقصة)<br>
• TYPE لـ 76.5% من الوثائق غير المصنفة<br>
• إضافة حقل نوع البناء (خرسانة، فولاذ، بناء تقليدي)</p>
</div>
</div>

<h3 style="font-size:1.1rem;margin-bottom:18px;margin-top:10px;color:var(--orange)">🟠 إجراءات متوسطة المدى (3-12 شهر)</h3>
<div class="rec-grid">
<div class="rec-card" style="border-color:var(--orange)">
<span class="priority" style="background:rgba(249,115,22,.15);color:var(--orange)">أولوية عالية</span>
<h4>4. سياسة التوزيع الجغرافي</h4>
<p>إعادة توازن المحفظة نحو التوزيع المستهدف:<br>
• <strong>تقليل:</strong> Zone III من 35.8% إلى 15% (-20.8pp)<br>
• <strong>تقليل:</strong> Zone IIb من 33.7% إلى 25% (-8.7pp)<br>
• <strong>زيادة:</strong> Zone 0+I من 11.7% إلى 30% (+18.3pp)<br>
• <strong>زيادة:</strong> Zone IIa من 18.5% إلى 25% (+6.5pp)</p>
</div>
<div class="rec-card" style="border-color:var(--orange)">
<span class="priority" style="background:rgba(249,115,22,.15);color:var(--orange)">أولوية عالية</span>
<h4>5. التسعير التفاضلي</h4>
<p>مراجعة التسعير لتعكس المخاطر الفعلية:<br>
• Zone III: زيادة 40-60% في الأقساط<br>
• Zone IIb: زيادة 20-30%<br>
• Zone 0+I: تخفيض 10-20% لتحفيز الطلب<br>
• تسعير إضافي حسب نوع البناء</p>
</div>
<div class="rec-card" style="border-color:var(--orange)">
<span class="priority" style="background:rgba(249,115,22,.15);color:var(--orange)">أولوية عالية</span>
<h4>6. شبكة وكلاء المناطق الآمنة</h4>
<p>تطوير شبكة التوزيع في المناطق ذات الخطورة المنخفضة:<br>
• ولايات الجنوب (Zone 0): حملات تسويق مستهدفة<br>
• ولايات الهضاب (Zone I): شراكات مع البنوك<br>
• تحفيز الوكلاء بعمولات أعلى في هذه المناطق</p>
</div>
</div>

<h3 style="font-size:1.1rem;margin-bottom:18px;margin-top:10px;color:var(--amber)">🟡 إجراءات طويلة المدى (12-36 شهر)</h3>
<div class="rec-grid">
<div class="rec-card" style="border-color:var(--amber)">
<span class="priority" style="background:rgba(245,158,11,.15);color:var(--amber)">استراتيجية</span>
<h4>7. نظام مراقبة التراكمات</h4>
<p>تطوير أداة آلية لمراقبة التراكمات في الوقت الحقيقي:<br>
• تنبيهات عند بلوغ 80% من سقف الاكتتاب<br>
• لوحة قيادة يومية للتراكمات<br>
• ربط بنظام المعلومات الجغرافية</p>
</div>
<div class="rec-card" style="border-color:var(--amber)">
<span class="priority" style="background:rgba(245,158,11,.15);color:var(--amber)">استراتيجية</span>
<h4>8. برنامج الوقاية من الأضرار</h4>
<p>إطلاق برنامج وقاية للمؤمن لهم في Zone III:<br>
• تدقيق زلزالي مجاني للمنشآت الصناعية<br>
• خصومات على الأقساط عند التقيد بمعايير RPA<br>
• شراكة مع CGS لتقييم المباني</p>
</div>
<div class="rec-card" style="border-color:var(--amber)">
<span class="priority" style="background:rgba(245,158,11,.15);color:var(--amber)">استراتيجية</span>
<h4>9. نمذجة كارثية متقدمة</h4>
<p>الاستثمار في نموذج PML متقدم:<br>
• استخدام بيانات CGS الحقيقية<br>
• نمذجة على مستوى البلدية<br>
• تكامل بيانات نوع البناء والعمر<br>
• سيناريوهات متعددة (بومرداس 2003, شلف 1980)</p>
</div>
</div>
</div>
</div>
"""

    # JavaScript
    vi_top15 = vi_sorted[:15]
    html += f"""
<script>
function st(id){{
document.querySelectorAll('.tc').forEach(t=>t.classList.remove('on'));
document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
document.getElementById('t-'+id).classList.add('on');
event.target.classList.add('on');
}}
Chart.defaults.color='#7c8db0';Chart.defaults.borderColor='rgba(255,255,255,0.05)';
Chart.defaults.font.family="'Cairo','Inter',sans-serif";

// Gap chart
new Chart(document.getElementById('gapChart'),{{
type:'bar',
data:{{
labels:{json.dumps([f"{zl[z]} ({zla[z]})" for z in zones])},
datasets:[
{{label:'الفعلي %',data:{json.dumps(actual_data)},backgroundColor:{json.dumps([zc[z]+'cc' for z in zones])},borderColor:{json.dumps([zc[z] for z in zones])},borderWidth:2,borderRadius:6,barPercentage:.35}},
{{label:'المستهدف %',data:{json.dumps(target_data)},backgroundColor:'rgba(255,255,255,0.1)',borderColor:'rgba(255,255,255,0.3)',borderWidth:2,borderRadius:6,borderDash:[4,4],barPercentage:.35}}
]
}},
options:{{responsive:true,maintainAspectRatio:false,
plugins:{{legend:{{position:'bottom',labels:{{usePointStyle:true}}}}}},
scales:{{y:{{beginAtZero:true,title:{{display:true,text:'%'}},grid:{{color:'rgba(255,255,255,0.04)'}}}},x:{{grid:{{display:false}}}}}}
}}
}});

// Accumulation chart
const accW={json.dumps([w for w,_ in top20])};
const accSI={json.dumps([round(d["si"]/1e6,1) for _,d in top20])};
const accC={json.dumps([zc.get(d["zone"],"#666") for _,d in top20])};
const retLine={COMPANY_RETENTION/1e6};
new Chart(document.getElementById('accumChart2'),{{
type:'bar',
data:{{labels:accW,datasets:[
{{label:'رأس المال (M DA)',data:accSI,backgroundColor:accC.map(c=>c+'cc'),borderColor:accC,borderWidth:2,borderRadius:5}},
{{label:'حد الاحتفاظ',data:Array(accW.length).fill(retLine),type:'line',borderColor:'#ef4444',borderWidth:2,borderDash:[6,3],pointRadius:0,fill:false}}
]}},
options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{usePointStyle:true}}}}}},scales:{{x:{{beginAtZero:true,title:{{display:true,text:'M DA'}},grid:{{color:'rgba(255,255,255,0.04)'}}}},y:{{grid:{{display:false}}}}}}}}
}});

// Geographic distribution (doughnut)
new Chart(document.getElementById('geoDistChart'),{{
type:'doughnut',
data:{{labels:{json.dumps([f"{zl[z]} ({zla[z]})" for z in zones])},
datasets:[{{data:{json.dumps([round(A["bz"].get(z,{"si":0})["si"]) for z in zones])},backgroundColor:{json.dumps([zc[z] for z in zones])},borderWidth:2,borderColor:'#141d30',hoverOffset:10}}]}},
options:{{responsive:true,maintainAspectRatio:false,cutout:'50%',plugins:{{legend:{{position:'bottom',labels:{{padding:10,usePointStyle:true}}}},tooltip:{{callbacks:{{label:ctx=>ctx.label+': '+(ctx.raw/1e9).toFixed(2)+' Mrd DA'}}}}}}}}
}});

// Zone × Type heatmap (stacked bar)
const ztD={json.dumps(zt_data)};
const tps=['Industrielle','Commerciale','Non classé'];
const tpLabels=['صناعية','تجارية','غير محدد'];
const tpC=['#3b82f6','#f59e0b','#6b7280'];
new Chart(document.getElementById('ztHeatChart'),{{
type:'bar',
data:{{labels:{json.dumps([f"{zl[z]}" for z in zones])},
datasets:tps.map((t,i)=>({{label:tpLabels[i],data:{json.dumps(zones)}.map(z=>ztD[z]?Math.round((ztD[z][t]||0)/1e6):0),backgroundColor:tpC[i]+'99',borderColor:tpC[i],borderWidth:1,borderRadius:3}}))}},
options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{usePointStyle:true}}}}}},scales:{{x:{{stacked:true,grid:{{display:false}}}},y:{{stacked:true,beginAtZero:true,title:{{display:true,text:'M DA'}},grid:{{color:'rgba(255,255,255,0.04)'}}}}}}}}
}});

// Vulnerability bar chart
const vulnW={json.dumps([w for w,_ in vi_top15])};
const vulnS={json.dumps([round(v["risk_adj"],4) for _,v in vi_top15])};
const vulnC={json.dumps([zc.get(v["zone"],"#666") for _,v in vi_top15])};
new Chart(document.getElementById('vulnChart'),{{
type:'bar',
data:{{labels:vulnW,datasets:[{{label:'مؤشر الخطر المركب',data:vulnS,backgroundColor:vulnC.map(c=>c+'cc'),borderColor:vulnC,borderWidth:2,borderRadius:5}}]}},
options:{{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{beginAtZero:true,title:{{display:true,text:'Risk-Adjusted Score'}},grid:{{color:'rgba(255,255,255,0.04)'}}}},y:{{grid:{{display:false}}}}}}}}
}});

// Radar chart
const radarZones={json.dumps([zl[z] for z in zones[:5]])};
new Chart(document.getElementById('radarChart'),{{
type:'radar',
data:{{labels:radarZones,datasets:[
{{label:'تركيز رأس المال %',data:{json.dumps([A["actual"].get(z,0) for z in zones[:5]])},borderColor:'#3b82f6',backgroundColor:'rgba(59,130,246,.12)',pointBackgroundColor:'#3b82f6'}},
{{label:'التوزيع المستهدف %',data:{json.dumps([A["target"].get(z,0) for z in zones[:5]])},borderColor:'#10b981',backgroundColor:'rgba(16,185,129,.08)',pointBackgroundColor:'#10b981',borderDash:[4,3]}},
{{label:'معامل الخطر (x10)',data:{json.dumps([ZONE_RISK_COEFF.get(z,0)*100 for z in zones[:5]])},borderColor:'#ef4444',backgroundColor:'rgba(239,68,68,.08)',pointBackgroundColor:'#ef4444'}}
]}},
options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{position:'bottom',labels:{{usePointStyle:true}}}}}},scales:{{r:{{beginAtZero:true,grid:{{color:'rgba(255,255,255,0.05)'}},angleLines:{{color:'rgba(255,255,255,0.05)'}},pointLabels:{{font:{{size:11}}}}}}}}}}
}});

// ========== LEAFLET MAP ==========
const mapData = {leaflet_data};
const zoneColors = {json.dumps(zc)};
const zoneLabelsFr = {json.dumps(zl)};
const zoneLabelsAr = {json.dumps(zla)};
const retention = {COMPANY_RETENTION};

const map = L.map('leaflet-map',{{
    center:[28.5, 2.5],
    zoom:5,
    zoomControl:true,
    attributionControl:true
}});

// Dark CartoDB tile layer
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/dark_all/{{z}}/{{x}}/{{y}}{{r}}.png', {{
    attribution:'&copy; <a href="https://carto.com">CARTO</a> | &copy; <a href="https://osm.org">OSM</a>',
    subdomains:'abcd',
    maxZoom:19
}}).addTo(map);

// Store all markers for filtering
const allMarkers = [];

mapData.forEach(d => {{
    const radius = Math.max(8, Math.min(50, Math.sqrt(d.si / 1e8) * 4.5));
    const color = zoneColors[d.zone] || '#666';
    const borderColor = d.exceeds ? '#ef4444' : 'rgba(255,255,255,0.4)';
    const borderWeight = d.exceeds ? 3 : 1;

    const siB = d.si >= 1e9 ? (d.si/1e9).toFixed(2)+' Mrd' : (d.si/1e6).toFixed(1)+' M';
    const primeM = (d.prime/1e6).toFixed(1);
    const retRatio = (d.si / retention).toFixed(1);
    const exceedBadge = d.exceeds
        ? '<span style="background:rgba(239,68,68,.2);color:#ef4444;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;">🔴 يتجاوز الاحتفاظ ('+retRatio+'x)</span>'
        : '<span style="background:rgba(16,185,129,.2);color:#10b981;padding:2px 8px;border-radius:10px;font-size:11px;font-weight:700;">🟢 ضمن الحدود</span>';

    const popup = `
        <div style="font-family:Cairo,sans-serif;direction:rtl;min-width:260px;">
            <div style="font-size:15px;font-weight:800;margin-bottom:6px;color:#1a1a2e;">${{d.name}}</div>
            <div style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:11px;font-weight:700;margin-bottom:8px;
                background:${{color}}22;color:${{color}};border:1px solid ${{color}}44;">Zone ${{d.zone}} — ${{zoneLabelsAr[d.zone] || ''}}</div>
            <table style="width:100%;border-collapse:collapse;margin-top:6px;font-size:12px;">
                <tr><td style="padding:4px 0;color:#666;">📋 عدد الوثائق</td><td style="padding:4px 0;font-weight:700;text-align:left;">${{d.n.toLocaleString()}}</td></tr>
                <tr><td style="padding:4px 0;color:#666;">💰 الأقساط</td><td style="padding:4px 0;font-weight:700;text-align:left;">${{primeM}} M DA</td></tr>
                <tr><td style="padding:4px 0;color:#666;">🏦 رأس المال المقدر</td><td style="padding:4px 0;font-weight:700;text-align:left;color:${{color}}">${{siB}} DA</td></tr>
                <tr><td style="padding:4px 0;color:#666;">🏘️ البلديات</td><td style="padding:4px 0;font-weight:700;text-align:left;">${{d.communes}}</td></tr>
            </table>
            <div style="margin-top:8px;text-align:center;">${{exceedBadge}}</div>
        </div>`;

    const marker = L.circleMarker([d.lat, d.lon], {{
        radius: radius,
        fillColor: color,
        color: borderColor,
        weight: borderWeight,
        opacity: 0.9,
        fillOpacity: 0.65,
        className: 'pulse-marker'
    }}).addTo(map);

    marker.bindPopup(popup, {{maxWidth:320, className:'custom-popup'}});
    marker.bindTooltip(d.name, {{permanent:false, direction:'top', offset:[0,-radius], className:'custom-tooltip'}});
    marker._data = d;
    allMarkers.push(marker);
}});

// Custom Legend Control
const legend = L.control({{position:'bottomright'}});
legend.onAdd = function() {{
    const div = L.DomUtil.create('div','leaflet-legend');
    div.style.cssText = 'background:rgba(10,15,30,.92);padding:14px 18px;border-radius:10px;border:1px solid rgba(255,255,255,.1);color:#e2e8f0;font-family:Cairo,sans-serif;font-size:12px;direction:rtl;backdrop-filter:blur(8px);';
    div.innerHTML = '<div style="font-weight:700;margin-bottom:8px;font-size:13px;">🗺️ دليل الخريطة</div>'
        + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;"><span style="width:14px;height:14px;border-radius:50%;background:#a855f7;display:inline-block;"></span> Zone III — مرتفعة جدًا</div>'
        + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;"><span style="width:14px;height:14px;border-radius:50%;background:#ef4444;display:inline-block;"></span> Zone IIb — مرتفعة</div>'
        + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;"><span style="width:14px;height:14px;border-radius:50%;background:#f97316;display:inline-block;"></span> Zone IIa — متوسطة</div>'
        + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;"><span style="width:14px;height:14px;border-radius:50%;background:#eab308;display:inline-block;"></span> Zone I — ضعيفة</div>'
        + '<div style="display:flex;align-items:center;gap:8px;margin-bottom:4px;"><span style="width:14px;height:14px;border-radius:50%;background:#22c55e;display:inline-block;"></span> Zone 0 — ضعيفة جدًا</div>'
        + '<div style="border-top:1px solid rgba(255,255,255,.1);margin-top:6px;padding-top:6px;">'
        + '<div style="display:flex;align-items:center;gap:8px;"><span style="width:14px;height:14px;border-radius:50%;border:2.5px solid #ef4444;display:inline-block;"></span> يتجاوز قدرة الاحتفاظ</div></div>'
        + '<div style="margin-top:6px;font-size:10px;color:#7c8db0;">حجم الدائرة = رأس المال المقدر</div>';
    return div;
}};
legend.addTo(map);

// Zone filter function
function filterZone(zone, btn) {{
    document.querySelectorAll('.map-filter-btn').forEach(b => b.style.opacity = '0.5');
    btn.style.opacity = '1';

    allMarkers.forEach(m => {{
        const d = m._data;
        let show = true;
        if (zone === 'all') show = true;
        else if (zone === 'exceed') show = d.exceeds;
        else show = d.zone === zone;

        if (show) {{
            m.setStyle({{fillOpacity: 0.65, opacity: 0.9}});
            m.setRadius(Math.max(8, Math.min(50, Math.sqrt(d.si / 1e8) * 4.5)));
        }} else {{
            m.setStyle({{fillOpacity: 0.08, opacity: 0.15}});
            m.setRadius(4);
        }}
    }});
}}

// Fix map size when tab becomes visible
const origSt = st;
function st(id) {{
    document.querySelectorAll('.tc').forEach(t=>t.classList.remove('on'));
    document.querySelectorAll('.tab').forEach(t=>t.classList.remove('on'));
    document.getElementById('t-'+id).classList.add('on');
    event.target.classList.add('on');
    if (id === 'map') setTimeout(() => map.invalidateSize(), 100);
}}
</script>
<style>
.custom-popup .leaflet-popup-content-wrapper {{background:white;border-radius:12px;box-shadow:0 8px 30px rgba(0,0,0,.25);border:1px solid rgba(0,0,0,.05);}}
.custom-popup .leaflet-popup-tip {{background:white;}}
.custom-tooltip {{background:rgba(10,15,30,.88)!important;color:#e2e8f0!important;border:1px solid rgba(255,255,255,.15)!important;border-radius:6px!important;font-family:Cairo,sans-serif!important;font-weight:600!important;font-size:11px!important;padding:4px 10px!important;box-shadow:0 4px 12px rgba(0,0,0,.3)!important;}}
.leaflet-control-zoom a {{background:rgba(10,15,30,.85)!important;color:#e2e8f0!important;border-color:rgba(255,255,255,.1)!important;}}
.leaflet-control-zoom a:hover {{background:rgba(139,92,246,.3)!important;}}
</style>
</body></html>"""

    with open(path,'w',encoding='utf-8') as f: f.write(html)
    print(f"  => Dashboard: {path}")

# ============================================================================
var_red = "var(--red)"

def main():
    print("="*60)
    print("Phase 3: Balance Diagnosis & Final Deliverables")
    print("="*60)
    recs = load_data()
    A = analyze(recs)
    base = os.path.dirname(os.path.abspath(__file__))
    out = os.path.join(base, "phase3_final_dashboard.html")
    gen_dashboard(A, out)
    print(f"\n  Total SI: {fmt_b(A['si'])} DA")
    print(f"  Gap Analysis:")
    for z in ["0","I","IIa","IIb","III"]:
        g = A["gap"].get(z,0)
        print(f"    Zone {z:4s}: Actual {A['actual'].get(z,0):5.1f}% | Target {A['target'].get(z,0):5.1f}% | Gap {'+' if g>0 else ''}{g:.1f}pp")
    print(f"\n  Open: {out}")

if __name__=="__main__":
    main()
