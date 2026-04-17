import os
from flask import Flask
from .models import db


def create_app():
    app = Flask(__name__, template_folder='../templates', static_folder='../static')

    basedir  = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    data_dir = os.path.join(basedir, 'data')
    datasets_dir = os.path.join(data_dir, 'datasets')
    uploads_dir  = os.path.join(data_dir, 'uploads')

    os.makedirs(datasets_dir, exist_ok=True)
    os.makedirs(uploads_dir,  exist_ok=True)

    # Registry DB — stores only dataset metadata
    registry_db = os.path.join(data_dir, 'registry.db')
    app.config['SQLALCHEMY_DATABASE_URI']  = f'sqlite:///{registry_db}'
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['UPLOAD_FOLDER']   = uploads_dir
    app.config['DATASETS_FOLDER'] = datasets_dir
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100 MB
    app.secret_key = 'aec-hackathon-secret'

    db.init_app(app)
    with app.app_context():
        db.create_all()

    from .routes.views import views_bp
    from .routes.api   import api_bp
    app.register_blueprint(views_bp)
    app.register_blueprint(api_bp)

    return app
