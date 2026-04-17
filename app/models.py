from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()


class Dataset(db.Model):
    """Registry of uploaded datasets. Each dataset has its own SQLite file."""
    __tablename__ = 'datasets'
    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(200), nullable=False)
    filename    = db.Column(db.String(200), nullable=False)   # original CSV name
    db_file     = db.Column(db.String(300), nullable=False)   # path to dataset .db
    uploaded_at = db.Column(db.DateTime, default=datetime.utcnow)
    record_count= db.Column(db.Integer, default=0)

    def __repr__(self):
        return f'<Dataset {self.name}>'
