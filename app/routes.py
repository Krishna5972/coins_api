from flask import Blueprint, jsonify
from .services.trading import *
trading_bp = Blueprint('trading', __name__)


@trading_bp.route('/15m')
def fifteen_min():
    data = execute_and_get_results('15m')
    return jsonify(data)
    

@trading_bp.route('/30m')
def thirty_min():
    data = execute_and_get_results('30m')
    return jsonify(data)

@trading_bp.route('/1h')
def one_hour():
    data = execute_and_get_results('1h')
    return jsonify(data)

@trading_bp.route('/5m')
def five_minutes():
    data = execute_and_get_results('5m')
    return jsonify(data)

@trading_bp.route('/get_first_volatile/<int:n>')
def get_first_volatile_coin(n):
    coin = get_first_volatile_coin(n)
    data = {
        'coin' : coin,
        'time' : datetime.utcnow()
    }
    return jsonify(data)

@trading_bp.route('/get_coins_list')
def get_coins_list_endpoint():
    possible_long, possible_short, possible_volatile = get_coins_list()
    response_data = {
        'possible_long': possible_long,
        'possible_short': possible_short,
        'possible_volatile': possible_volatile
    }
    return jsonify(response_data)