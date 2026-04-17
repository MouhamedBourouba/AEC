import os
import csv
from datetime import datetime
from app import create_app
from app.models import db, Policy

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%d/%m/%Y').date()
    except:
        return None

def parse_float(val):
    if not val:
        return 0.0
    if isinstance(val, str):
        val = val.replace(',', '.')
    try:
        return float(val)
    except:
        return 0.0

def populate():
    app = create_app()
    basedir = os.path.abspath(os.path.dirname(__file__))
    csv_path = os.path.join(basedir, 'data', 'CATNAT_2023_2025.xlsx - 2023.csv')
    
    print(f"Reading CSV from {csv_path}...")
    
    with app.app_context():
        print("Creating tables...")
        db.create_all()
        
        print("Clearing existing data...")
        db.drop_all()
        db.create_all()
        
        print("Populating database...")
        policies = []
        
        with open(csv_path, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            for index, row in enumerate(reader):
                policy = Policy(
                    numero_police=row.get('NUMERO_POLICE'),
                    code_sous_branche=row.get('CODE_SOUS_BRANCHE'),
                    num_avnt_cours=row.get('NUM_AVNT_COURS'),
                    date_effet=parse_date(row.get('DATE_EFFET')),
                    date_expiration=parse_date(row.get('DATE_EXPIRATION')),
                    type_installation=row.get('TYPE'),
                    wilaya=row.get('WILAYA'),
                    commune=row.get('COMMUNE'),
                    capital_assure=parse_float(row.get('CAPITAL_ASSURE')),
                    prime_nette=parse_float(row.get('PRIME_NETTE'))
                )
                policies.append(policy)
                
                # Batch commit to keep memory usage low and insert faster
                if len(policies) >= 1000:
                    db.session.bulk_save_objects(policies)
                    db.session.commit()
                    policies = []
                    print(f"Inserted {index + 1} records...")
                    
            # Insert any remaining records
            if policies:
                db.session.bulk_save_objects(policies)
                db.session.commit()
                
        print("Database populated successfully! SQLite DB created at data/portfolio.db")

if __name__ == '__main__':
    populate()
