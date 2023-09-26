from flask import Flask
from flask_caching import Cache
import logging


# Set up logging
logging.basicConfig(filename='trading_data_log.txt', filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
cache = Cache()
def create_app():
    app = Flask(__name__)

    app.config['CACHE_TYPE'] = 'simple'
    cache.init_app(app)
    
    from .routes import trading_bp
    
    app.register_blueprint(trading_bp, url_prefix='/trading')


    return app
