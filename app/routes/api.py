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
