import logging

from flask import current_app, Flask, redirect, url_for

def create_app(config, debug=True, testing=False, config_overrides=None):
    app = Flask(__name__)
    app.config.from_object(config)

    app.debug = debug 
    app.testing = testing 

    if config_overrides:
        app.config.update(config_overrides)

    # configure logging
    if not app.testing:
        logging.basicConfig(level=logging.INFO)

    # Setup the data model
    with app.app_context():

        model = get_model()
        model.init_app(app)

    # register the app crud blueprint
    from .crud import crud
    app.register_blueprint(crud, url_prefix='/wine_reviews')

    # add a deafult route
    @app.route('/')
    def index():
        return redirect(url_for('crud.list'))

    # error handler
    @app.errorhandler(500)
    def server_error(e):
        return """
        An internal error ocurred: <pr>{}</pre>
        See logs fro full stacktrace.
        """.format(e), 500

    return app

def get_model():
    model_backend = current_app.config['DATA_BACKEND']
    if model_backend == 'mongodb':
        from . import model_mongodb
        model = model_mongodb
    else:
        raise ValueError(
            "No appropriate databackend configured. "
            "Please specify mongodb")
    return model
