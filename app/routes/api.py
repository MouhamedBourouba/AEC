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
