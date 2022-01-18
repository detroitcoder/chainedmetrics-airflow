import logging
import sys
import os
import json
import psycopg2
import requests
import logging

from brownie import accounts, network, project, Contract
from collections import defaultdict

DEV_DATABASE = "chainedmetrics-dev-do-user-9754357-0.b.db.ondigitalocean.com"
PROD_DATABASE = "chainedmetrics-prod-do-user-9754357-0.b.db.ondigitalocean.com"
MAX_DEPTH = 20
POLYSCAN_API_KEY = "WC476C15XFJK19P3HS9UQFBYY38W4JGBFZ"

def open_orders(broker):
    '''
    Returns the open orders for the broker

    Arguments:
        broker (brownie.Contract): A broker for a given exchange
    
    Returns:
        orders (dict): Open orders for each beat/miss at each price including with
                        the corresponding address and quantity

    eg. 
    orders = {True:{
                10: [0x8..98f4, 100], {0x7..8fa2, 30}},
                50: [0x8f2..b4cc, 70]
            },
            False:{
                10: [0x6..32bc, 200]
            }
    '''

    logging.info(f'Getting Open Orders for {broker}')
    max_depth = MAX_DEPTH if MAX_DEPTH else broker.maxOrderDepth()
    prices = [10, 20, 30, 40, 50, 60, 70, 80, 90]

    order_book = defaultdict(lambda: defaultdict(list))

    orders = broker.orders
    for beat in (True, False):
        for p in prices:
            for i in range(MAX_DEPTH):
                try:
                    address, quantity = orders(beat, p, i)
                    order_book[beat][p].append((address, quantity))
                except ValueError:
                    break
    
    return order_book


def issued_tokens(token_address):
    '''
    Returns what addresses own the corresponding tokens and how much

    Arguments:
        token_address (string): The address for the token
    
    Returns:
        owners (dict): A dictionary of addresses and the quantity owned

    Notes:
        Cannot hit this endpoint more thean 5x/second
    '''

    logging.info(f'Getting Issuances for {token_address}')
    url = (
        "https://api.polygonscan.com/api?module=account&action=tokentx&"
        f"contractaddress={token_address}&sort=asc&"
        f"apikey={POLYSCAN_API_KEY}"
    )

    resp = requests.get(url)
    resp.raise_for_status()
    transaction_dict = resp.json()

    if len(transaction_dict['result']) > 1000:
        raise Exception('Need to iterate :)')

    transaction_history = defaultdict(lambda: 0)
    for txn in transaction_dict['result']:
        if txn['from'] == '0x0000000000000000000000000000000000000000':
            transaction_history[txn['to']] += int(txn['value'])
    
    return transaction_history
    

def get_all_markets(conn):

    cursor = conn.cursor()
    cursor.execute('SELECT * FROM market')

    headers =  [desc[0] for desc in cursor.description]
    market_dict = [dict(zip(headers, row)) for row in cursor]

    return market_dict

def setup_network():

    account = accounts.add(os.environ['PRIVATE_KEY'])
    project.load('.', 'NotificationAnalysis')
    network.connect('polygon-main')

    return account

def get_contracts(broker_address, beat_address, miss_address, market='binary', broker_abi=None, erc20_abi=None):

    abi_dict = {
        'binary': 'binary_market_abi.json',
        'scalar': 'scalar_abi.json'
    }
    
    assert market in abi_dict, 'market must be binary OR scalar'

    broker_abi_location = os.path.join(os.path.dirname(__file__), abi_dict[market])
    erc20_abi_location = os.path.join(os.path.dirname(__file__), 'erc20_abi.json')
    

    if not broker_abi:
        with open(broker_abi_location) as fil:
            broker_abi = json.load(fil)
    
    if not erc20_abi:
        with open(erc20_abi_location) as fil:
            erc20_abi = json.load(fil)

    broker = Contract.from_abi('Broker', broker_address, broker_abi)
    beat = Contract.from_abi('Beat', beat_address, erc20_abi)
    miss = Contract.from_abi('Miss', miss_address, erc20_abi)


    return broker, beat, miss

def get_connection():

    host = PROD_DATABASE if os.getenv('CHAINDEMETRICS_ENV') == 'Production' else DEV_DATABASE

    logging.info(f'Connecting to {host}')

    conn = psycopg2.connect(
        host=host, 
        port=25060,
        dbname="metrics", 
        user="flask_app", 
        password=os.getenv("DB_PASS")
    )

    return conn

def main():
    
    logging.info('Begginning analysis and connecting to database')
    conn = get_connection()

    logging.info('Accessing All Markets')
    markets = get_all_markets(conn)

    logging.info('Connecting to polygon network')
    account = setup_network()

    logging.info('Iterating over all Markets')
    all_data = []
    for m in markets:
        market_data = {}
        market_data['meta_data'] = m
        if m['broker_address']:
            broker, beat, miss = get_contracts(
                m['broker_address'], m['beat_address'], m['miss_address']
            )
            market_data['data'] = (open_orders(broker), issued_tokens(beat), issued_tokens(miss))
        else:
            market_data['data'] = None
        
        all_data.append(market_data)

    return all_data