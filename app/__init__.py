from flask import Flask

def create_app():
    # Initialize the app and point it to the templates and static folders in the project root
    app = Flask(__name__, template_folder='../templates', static_folder='../static')

    # Register blueprints
    from .routes.views import views_bp
    app.register_blueprint(views_bp)

    return app
