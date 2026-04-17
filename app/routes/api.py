import sqlite3
from flask import Blueprint, jsonify, request
from app.models import Dataset

api_bp = Blueprint('api', __name__, url_prefix='/api')

def get_db_connection(dataset_id):
    if not dataset_id:
        return None
    dataset = Dataset.query.get(dataset_id)
    if not dataset or not dataset.db_file:
        return None
    
    con = sqlite3.connect(dataset.db_file)
    con.row_factory = sqlite3.Row
    return con

@api_bp.route('/stats')
def stats():
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("SELECT COUNT(id) FROM policies")
        total_policies = cur.fetchone()[0] or 0

        cur.execute("SELECT SUM(capital_assure) FROM policies")
        total_capital = cur.fetchone()[0] or 0

        cur.execute("SELECT SUM(prime_nette) FROM policies WHERE prime_nette > 0")
        total_premium = cur.fetchone()[0] or 0
    finally:
        con.close()

    return jsonify({
        'total_policies': total_policies,
        'total_capital': round(total_capital),
        'total_premium': round(total_premium),
    })

@api_bp.route('/by-wilaya')
def by_wilaya():
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT wilaya, SUM(capital_assure) as total_capital, COUNT(id) as count
            FROM policies
            WHERE wilaya IS NOT NULL AND wilaya != ''
            GROUP BY wilaya
            ORDER BY total_capital DESC
        """)
        rows = cur.fetchall()
    finally:
        con.close()

    return jsonify([
        {'wilaya': r['wilaya'], 'total_capital': round(r['total_capital']), 'count': r['count']}
        for r in rows
    ])

@api_bp.route('/by-type')
def by_type():
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT type_installation, SUM(capital_assure) as total_capital, COUNT(id) as count
            FROM policies
            WHERE type_installation IS NOT NULL AND type_installation != ''
            GROUP BY type_installation
            ORDER BY total_capital DESC
        """)
        rows = cur.fetchall()
    finally:
        con.close()

    return jsonify([
        {'type': r['type_installation'], 'total_capital': round(r['total_capital']), 'count': r['count']}
        for r in rows
    ])

@api_bp.route('/by-year')
def by_year():
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT strftime('%Y', date_effet) as year, COUNT(id) as count, SUM(capital_assure) as total_capital
            FROM policies
            WHERE date_effet IS NOT NULL
            GROUP BY year
            ORDER BY year
        """)
        rows = cur.fetchall()
    finally:
        con.close()

    return jsonify([
        {'year': r['year'], 'count': r['count'], 'total_capital': round(r['total_capital'])}
        for r in rows
    ])

from app.rpa import get_zone_for_wilaya

@api_bp.route('/by-wilaya-map')
def by_wilaya_map():
    """Return per-wilaya aggregates (capital, count, premium) for the GIS map."""
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT wilaya,
                   SUM(capital_assure) AS total_capital,
                   COUNT(id)           AS count,
                   SUM(prime_nette)    AS total_premium
            FROM policies
            WHERE wilaya IS NOT NULL AND wilaya != ''
            GROUP BY wilaya
            ORDER BY total_capital DESC
        """)
        rows = cur.fetchall()
    finally:
        con.close()

    return jsonify([
        {
            'wilaya':        r['wilaya'],
            'total_capital': round(r['total_capital'] or 0),
            'count':         r['count'],
            'total_premium': round(r['total_premium'] or 0),
            'zone_rpa':      get_zone_for_wilaya(r['wilaya'])
        }
        for r in rows
    ])

@api_bp.route('/by-rpa-zone')
def by_rpa_zone():
    """Return aggregates grouped by RPA zone."""
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT wilaya, capital_assure, prime_nette, type_installation
            FROM policies
            WHERE wilaya IS NOT NULL AND wilaya != ''
        """)
        rows = cur.fetchall()
    finally:
        con.close()

    zones_data = {
        "0":   {"capital": 0, "premium": 0, "count": 0, "types": {}},
        "I":   {"capital": 0, "premium": 0, "count": 0, "types": {}},
        "IIa": {"capital": 0, "premium": 0, "count": 0, "types": {}},
        "IIb": {"capital": 0, "premium": 0, "count": 0, "types": {}},
        "III": {"capital": 0, "premium": 0, "count": 0, "types": {}},
        "Inconnue": {"capital": 0, "premium": 0, "count": 0, "types": {}}
    }

    for r in rows:
        z = get_zone_for_wilaya(r['wilaya'])
        cap = r['capital_assure'] or 0
        prm = r['prime_nette'] or 0
        typ = r['type_installation'] or "Inconnu"
        
        zones_data[z]["capital"] += cap
        zones_data[z]["premium"] += prm
        zones_data[z]["count"]   += 1
        zones_data[z]["types"][typ] = zones_data[z]["types"].get(typ, 0) + 1

    result = []
    for z, data in zones_data.items():
        if data["count"] > 0:
            result.append({
                "zone": z,
                "capital": round(data["capital"]),
                "premium": round(data["premium"]),
                "count": data["count"],
                "types": data["types"]
            })
            
    # Sort zones: 0, I, IIa, IIb, III, Inconnue
    order = {"0": 0, "I": 1, "IIa": 2, "IIb": 3, "III": 4, "Inconnue": 5}
    result.sort(key=lambda x: order.get(x["zone"], 6))

    return jsonify(result)


@api_bp.route('/all-policies')
def all_policies():
    """Return all policy rows for the datatable view."""
    ds_id = request.args.get('dataset_id', type=int)
    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT
                id,
                numero_police,
                code_sous_branche,
                num_avnt_cours,
                date_effet,
                date_expiration,
                type_installation,
                wilaya,
                commune,
                capital_assure,
                prime_nette
            FROM policies
            ORDER BY id
            LIMIT 100000
        """)
        rows = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM policies")
        total = cur.fetchone()[0]
    finally:
        con.close()

    return jsonify({
        'total': total,
        'rows': [dict(r) for r in rows]
    })


# ── PML Simulator ─────────────────────────────────────────────────────────────

# Damage ratios (taux de dommage) by zone and broad asset category.
# Source: RPA 99/2003 + standard actuarial vulnerability tables for Algeria.
# Keys are normalised lowercase substrings matched against type_installation.
PML_DAMAGE_RATIOS = {
    # zone  →  { category: ratio }
    "0":   {"immobilier": 0.00, "commercial": 0.00, "industriel": 0.00, "default": 0.00},
    "I":   {"immobilier": 0.05, "commercial": 0.07, "industriel": 0.08, "default": 0.06},
    "IIa": {"immobilier": 0.15, "commercial": 0.18, "industriel": 0.20, "default": 0.16},
    "IIb": {"immobilier": 0.30, "commercial": 0.33, "industriel": 0.35, "default": 0.31},
    "III": {"immobilier": 0.50, "commercial": 0.55, "industriel": 0.60, "default": 0.52},
    "Inconnue": {"immobilier": 0.15, "commercial": 0.18, "industriel": 0.20, "default": 0.16},
}

def _asset_category(type_installation: str) -> str:
    """Map a raw type_installation string to a broad damage-ratio category."""
    if not type_installation:
        return "default"
    t = type_installation.lower()
    if any(k in t for k in ["immobilier", "résidentiel", "residentiel", "habitation", "logement", "villa", "appartement"]):
        return "immobilier"
    if any(k in t for k in ["commercial", "commerce", "bureau", "magasin", "hotel", "hôtel"]):
        return "commercial"
    if any(k in t for k in ["industriel", "industri", "usine", "entrepôt", "entrepot", "stockage"]):
        return "industriel"
    return "default"


@api_bp.route('/pml-simulation')
def pml_simulation():
    """
    Returns PML data aggregated by wilaya and RPA zone.
    Optional query param: scenario (conservative | standard | severe)
    - conservative: ratios × 0.7
    - standard:     ratios × 1.0  (default)
    - severe:       ratios × 1.4
    """
    ds_id    = request.args.get('dataset_id', type=int)
    scenario = request.args.get('scenario', 'standard')

    con = get_db_connection(ds_id)
    if not con:
        return jsonify({'error': 'Invalid or missing dataset_id'}), 400

    multipliers = {'conservative': 0.70, 'standard': 1.00, 'severe': 1.40}
    mult = multipliers.get(scenario, 1.00)

    try:
        cur = con.cursor()
        cur.execute("""
            SELECT wilaya, type_installation,
                   SUM(capital_assure) AS capital,
                   COUNT(id)           AS count
            FROM policies
            WHERE wilaya IS NOT NULL AND wilaya != ''
            GROUP BY wilaya, type_installation
        """)
        rows = cur.fetchall()
        cur.execute("SELECT SUM(capital_assure) FROM policies")
        total_capital = cur.fetchone()[0] or 0
    finally:
        con.close()

    # Aggregate by wilaya
    wilaya_map = {}   # wilaya → {capital, pml, count, zone}
    zone_map   = {}   # zone   → {capital, pml, count}

    for r in rows:
        wilaya  = r['wilaya']
        zone    = get_zone_for_wilaya(wilaya)
        capital = r['capital'] or 0
        count   = r['count']   or 0
        cat     = _asset_category(r['type_installation'])

        base_ratio = PML_DAMAGE_RATIOS.get(zone, PML_DAMAGE_RATIOS["Inconnue"]).get(cat, 0.16)
        ratio      = min(base_ratio * mult, 1.0)
        pml        = capital * ratio

        if wilaya not in wilaya_map:
            wilaya_map[wilaya] = {'wilaya': wilaya, 'zone': zone,
                                  'capital': 0, 'pml': 0, 'count': 0}
        wilaya_map[wilaya]['capital'] += capital
        wilaya_map[wilaya]['pml']     += pml
        wilaya_map[wilaya]['count']   += count

        if zone not in zone_map:
            zone_map[zone] = {'zone': zone, 'capital': 0, 'pml': 0, 'count': 0}
        zone_map[zone]['capital'] += capital
        zone_map[zone]['pml']     += pml
        zone_map[zone]['count']   += count

    # Build sorted lists
    by_wilaya = sorted(wilaya_map.values(), key=lambda x: x['pml'], reverse=True)
    by_zone   = sorted(zone_map.values(),
                       key=lambda x: {"0":0,"I":1,"IIa":2,"IIb":3,"III":4}.get(x['zone'], 5))

    # Round values
    for row in by_wilaya:
        row['capital'] = round(row['capital'])
        row['pml']     = round(row['pml'])
        row['ratio']   = round(row['pml'] / row['capital'], 4) if row['capital'] else 0

    for row in by_zone:
        row['capital'] = round(row['capital'])
        row['pml']     = round(row['pml'])
        row['ratio']   = round(row['pml'] / row['capital'], 4) if row['capital'] else 0

    total_pml = sum(r['pml'] for r in by_zone)

    return jsonify({
        'scenario':      scenario,
        'multiplier':    mult,
        'total_capital': round(total_capital),
        'total_pml':     round(total_pml),
        'pml_ratio':     round(total_pml / total_capital, 4) if total_capital else 0,
        'by_wilaya':     by_wilaya,
        'by_zone':       by_zone,
        'damage_ratios': PML_DAMAGE_RATIOS,
    })

