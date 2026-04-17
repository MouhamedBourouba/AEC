import os
import csv
import sqlite3
import re
from datetime import datetime
from flask import (Blueprint, render_template, redirect, url_for,
                   request, flash, current_app)
from werkzeug.utils import secure_filename
from app.models import db, Dataset

views_bp = Blueprint('views', __name__)

POLICY_SCHEMA = """
CREATE TABLE IF NOT EXISTS policies (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    numero_police    TEXT,
    code_sous_branche TEXT,
    num_avnt_cours   TEXT,
    date_effet       TEXT,
    date_expiration  TEXT,
    type_installation TEXT,
    wilaya           TEXT,
    commune          TEXT,
    capital_assure   REAL DEFAULT 0,
    prime_nette      REAL DEFAULT 0
)
"""

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'


def parse_date(s):
    if not s:
        return None
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(s.strip(), fmt).strftime('%Y-%m-%d')
        except ValueError:
            pass
    return None


def parse_float(s):
    if not s:
        return 0.0
    try:
        return float(s.strip().replace(',', '.'))
    except ValueError:
        return 0.0


def safe_db_name(name: str) -> str:
    """Turn any string into a safe filename."""
    name = re.sub(r'[^\w\-]', '_', name)
    return name[:60]


# ── Home ──────────────────────────────────────────────────────────────────────
@views_bp.route('/')
def index():
    datasets = Dataset.query.order_by(Dataset.uploaded_at.desc()).all()
    return render_template('index.html', datasets=datasets)


# ── Upload ────────────────────────────────────────────────────────────────────
@views_bp.route('/upload', methods=['POST'])
def upload():
    if 'file' not in request.files:
        flash('No file selected.', 'error')
        return redirect(url_for('views.index'))

    file = request.files['file']
    dataset_name = request.form.get('name', '').strip() or file.filename

    if not file or file.filename == '':
        flash('No file selected.', 'error')
        return redirect(url_for('views.index'))

    if not allowed_file(file.filename):
        flash('Only CSV files are supported.', 'error')
        return redirect(url_for('views.index'))

    # Save uploaded CSV
    orig_filename = secure_filename(file.filename)
    upload_path = os.path.join(current_app.config['UPLOAD_FOLDER'], orig_filename)
    file.save(upload_path)

    # Determine the per-dataset SQLite path (in data/datasets/)
    safe_name = safe_db_name(dataset_name)
    ts = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    db_filename = f'{safe_name}_{ts}.db'
    db_path = os.path.join(current_app.config['DATASETS_FOLDER'], db_filename)

    # Parse CSV and write to dataset's own SQLite
    try:
        count = _populate_dataset_db(upload_path, db_path)
    except Exception as e:
        flash(f'Error processing file: {str(e)}', 'error')
        return redirect(url_for('views.index'))

    # Register in the registry DB
    dataset = Dataset(
        name=dataset_name,
        filename=orig_filename,
        db_file=db_path,
        record_count=count,
    )
    db.session.add(dataset)
    db.session.commit()

    return redirect(url_for('views.dashboard', dataset_id=dataset.id))


def _populate_dataset_db(csv_path: str, db_path: str) -> int:
    """Parse CSV and bulk-insert into a fresh SQLite file. Returns record count."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()
    cur.executescript(POLICY_SCHEMA)

    insert_sql = """
        INSERT INTO policies
          (numero_police, code_sous_branche, num_avnt_cours,
           date_effet, date_expiration, type_installation,
           wilaya, commune, capital_assure, prime_nette)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """

    batch = []
    count = 0
    with open(csv_path, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            batch.append((
                row.get('NUMERO_POLICE', ''),
                row.get('CODE_SOUS_BRANCHE', ''),
                row.get('NUM_AVNT_COURS', ''),
                parse_date(row.get('DATE_EFFET', '')),
                parse_date(row.get('DATE_EXPIRATION', '')),
                row.get('TYPE', ''),
                row.get('WILAYA', ''),
                row.get('COMMUNE', ''),
                parse_float(row.get('CAPITAL_ASSURE', '')),
                parse_float(row.get('PRIME_NETTE', '')),
            ))
            count += 1
            if len(batch) >= 1000:
                cur.executemany(insert_sql, batch)
                con.commit()
                batch = []

    if batch:
        cur.executemany(insert_sql, batch)
        con.commit()

    con.close()
    return count


# ── Delete dataset ────────────────────────────────────────────────────────────
@views_bp.route('/delete/<int:dataset_id>', methods=['POST'])
def delete_dataset(dataset_id):
    dataset = Dataset.query.get_or_404(dataset_id)
    # Remove the dataset SQLite file
    if os.path.exists(dataset.db_file):
        os.remove(dataset.db_file)
    db.session.delete(dataset)
    db.session.commit()
    flash(f'Dataset "{dataset.name}" deleted.', 'success')
    return redirect(url_for('views.index'))


# ── Dashboard Multi-Page ──────────────────────────────────────────────────────
def get_dashboard_context(endpoint):
    dataset_id = request.args.get('dataset_id', type=int)
    if not dataset_id:
        latest = Dataset.query.order_by(Dataset.uploaded_at.desc()).first()
        if not latest:
            flash('Upload a dataset first.', 'error')
            return redirect(url_for('views.index')), None, None
        return redirect(url_for(endpoint, dataset_id=latest.id)), None, None

    dataset = Dataset.query.get_or_404(dataset_id)
    all_datasets = Dataset.query.order_by(Dataset.uploaded_at.desc()).all()
    return None, dataset, all_datasets

@views_bp.route('/dashboard')
def dashboard():
    dataset_id = request.args.get('dataset_id', type=int)
    if dataset_id:
        return redirect(url_for('views.overview', dataset_id=dataset_id))
    return redirect(url_for('views.overview'))

@views_bp.route('/dashboard/overview')
def overview():
    redir, dataset, all_datasets = get_dashboard_context('views.overview')
    if redir: return redir
    return render_template('overview.html', dataset=dataset, all_datasets=all_datasets, active_page='overview')

@views_bp.route('/dashboard/wilaya')
def wilaya_exposure():
    redir, dataset, all_datasets = get_dashboard_context('views.wilaya_exposure')
    if redir: return redir
    return render_template('wilaya_exposure.html', dataset=dataset, all_datasets=all_datasets, active_page='wilaya')

@views_bp.route('/dashboard/type')
def type_exposure():
    redir, dataset, all_datasets = get_dashboard_context('views.type_exposure')
    if redir: return redir
    return render_template('type_exposure.html', dataset=dataset, all_datasets=all_datasets, active_page='type')

@views_bp.route('/dashboard/time')
def time_exposure():
    redir, dataset, all_datasets = get_dashboard_context('views.time_exposure')
    if redir: return redir
    return render_template('time_exposure.html', dataset=dataset, all_datasets=all_datasets, active_page='time')

@views_bp.route('/dashboard/map')
def gis_map():
    redir, dataset, all_datasets = get_dashboard_context('views.gis_map')
    if redir: return redir
    return render_template('map.html', dataset=dataset, all_datasets=all_datasets, active_page='map')
