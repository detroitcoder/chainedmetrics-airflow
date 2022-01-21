import psycopg2
import logging
import time
from utilities import send_email, email_table_template
from utilities.contract_analyzer import setup_network, get_erc20_contract
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from brownie.network.gas.strategies import GasNowScalingStrategy


def deploy_market(marketType='binary'):

    brownie_location = Variable.get('chainedmetrics_contract_location')
    pk = BaseHook.get_connection("private_key").password

    assert brownie_location, "chainedmetrics_contract_location is not set"
    logging.info(f'Connecting to network Brownie Project at {brownie_location}')

    account, project = setup_network(pk, brownie_location)
    starting_gas = account.balance() / 10 ** 18
    logging.info(f'Using Account: {account.address}')
    logging.info(f'Account Balance: {starting_gas }')

    #Loading the config and network after loading the config in the setup_network call
    from brownie import config, network
    from brownie.network.transaction import TransactionReceipt

    logging.info(f'Deploying {marketType} market type')
    if marketType == 'binary':
        kpi_markets_to_deploy = get_binary_markets_to_deploy()
    elif marketType == 'scalar':
        raise NotImplementedError("Scalar Markets are not Deployable Yet")
    else:
        raise Exception(f'Invalid Market Type: {marketType}')

    logging.info(f'Found {len(kpi_markets_to_deploy)} markets to deploy')

    for market in kpi_markets_to_deploy:

        logging.info(f'Deploying market for {market["ticker"]} | {market["fiscal_period"]}| {market["metric"]}')
        
        beat = deploy_contract(project.MetricToken, market['beat_description'], market['beat_symbol'], {'from': account})
        miss = deploy_contract(project.MetricToken, market['miss_description'], market['miss_symbol'], {'from': account})

        logging.info('Deployed tokens now deploying Broker')
        details = (
                market['strike_value'],
                market['url'],
                config['networks'][network.show_active()]['oracle'],
                config['networks'][network.show_active()]['jobId'],
                config['networks'][network.show_active()]['fee_decimals'],
                config['networks'][network.show_active()]['link_token'],
                config['networks'][network.show_active()]['cmetric'],
                beat.address,
                miss.address,
                {'from': account}
        )
        logging.info(details)
        if marketType == 'binary':
            broker = deploy_contract(
                project.BinaryMarket,
                market['strike_value'],
                market['url'],
                config['networks'][network.show_active()]['oracle'],
                config['networks'][network.show_active()]['jobId'],
                config['networks'][network.show_active()]['fee_decimals'],
                config['networks'][network.show_active()]['link_token'],
                config['networks'][network.show_active()]['cmetric'],
                beat.address,
                miss.address,
                {'from': account},
            )
        elif marketType == 'scalar':
            broker = deploy_contract(
                project.ScalarMarket,
                market['high'],
                market['low'],
                market['url'],
                config['networks'][network.show_active()]['oracle'],
                config['networks'][network.show_active()]['jobId'],
                config['networks'][network.show_active()]['fee_decimals'],
                config['networks'][network.show_active()]['link_token'],
                config['networks'][network.show_active()]['cmetric'],
                beat.address,
                miss.address,
                {'from': account}
            )

        logging.info("Market Created. Granting Broker Role")
        granting_attempts = 3
        while granting_attempts >= 0:
            try:
                beat.grantRole(beat.BROKER_ROLE(), broker.address, {'from': account}, gas_price=GasNowScalingStrategy(initial_speed="fast", increment=1.2))
                miss.grantRole(miss.BROKER_ROLE(), broker.address, {'from': account}, gas_price=GasNowScalingStrategy(initial_speed="fast", increment=1.2))
                break
            except Exception as e:
                logging.exception(f"Error granting permissions: {str(e)}")
                granting_attempts -= 1
                if granting_attempts == 0:
                    raise
                time.sleep(5)

        logging.info("Broker role is granted")

        logging.info("Transfering .001 LINK to the broker to pay for lookup fees")
        
        # Occasionally the nonce is too low and the transaction needs to be retried
        transfer_attempts = 3
        while transfer_attempts > 0:
            link_contract = get_erc20_contract(config['networks'][network.show_active()]['link_token'])
            try:
                link_contract.transfer(
                    broker.address, 
                    10 ** (18 - config['networks'][network.show_active()]['fee_decimals']),
                    {'from': account},
                    gas_price=GasNowScalingStrategy(max_speed="fast", increment=1.2)
                )
                break
            except ValueError as e:
                if transfer_attempts == 1:
                    logging.exception("Raising Link Transfer Exception after multiple attempts")
                    raise
                elif 'nonce too low' in str(e):
                    logging.error(f'Nonce too low on Link transfer. Sleeping 15 seconds. Attempt: {transfer_attempts}')
                    transfer_attempts -= 1
                    time.sleep(15)
                else:
                    raise  

        logging.info("Updating Broker Address in Database")
        update_broker_addresses([
            [market['id'], broker.address, beat.address, miss.address]
        ])
        logging.info("Address Updated")

    ending_gas = account.balance() / 10**18
    title = "Chained Metrics Binary KPIs Deployed And Ready to Trade"
    description = (
        f"{len(kpi_markets_to_deploy)} new markets were deployed in production (below). In total "
        f"{starting_gas - ending_gas:.4f} MATIC was used in gas for this release and there is {ending_gas:.4f} "
        f"remaining in the deployment account. These are now live. This email is for devs only"
        f"and a second version will be used for users requesting market notifications"
    )

    headers = ['Ticker', 'KPI', 'Symbol', 'Strike', 'Period']
    rows = [[
        m['ticker'], m['metric'], m['beat_symbol'], m['strike_value'], m['fiscal_period']
    ] for m in kpi_markets_to_deploy]


    template = email_table_template(title, description, headers, rows)

    email_connection = BaseHook.get_connection("email")
    recipients = ['michael@chainedmetrics.com', 'jamal@chainedmetrics.com', 'dillon@chainedmetrics.com', 'nick@chainedmetrics.com', 'sachin@chainedmetrics.com']
    subject = "NEW! Binary Markets Deployed"
    text = f'There are {len(kpi_markets_to_deploy)} newly deployed binary markets'
    send_email(recipients, email_connection.login, template, subject, text, email_connection.password)

def get_binary_markets_to_deploy():
    '''
    A function that looks up binary markets to deply from the Markets database and returns a list
    of KPI markets that should be deployed in a list of di
    '''

    connection = BaseHook.get_connection("chainedmetrics_prod_deployment")

    logging.info(f"Connecting to database: {connection}")
    conn = psycopg2.connect(
        host=connection.host, 
        port=connection.port,
        dbname="metrics", 
        user=connection.login, 
        password=connection.password
    )

    logging.info(f"Connection established")
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, ticker, fiscal_period, metric, value, value_string, metric_symbol from market 
            where (broker_address is null OR broker_address='') AND value is not null""")

        clean_metric_list = []
        for row in cur:
            _id, ticker, fiscal_period, metric, strike_value, strike_string, metric_symbol = row
            market = get_market_data(_id, ticker, fiscal_period, metric, strike_value, strike_string, metric_symbol)
            clean_metric_list.append(market)
    except Exception:

        logging.exception("Error Connecting and reading from Market")
        raise

    finally:
        conn.close()

    return clean_metric_list

def update_broker_addresses(markets_created):
    '''
    Updates the broker, beat, and miss addresses for the newly created markets

    Args:
        markets_created (list): A list of tuples that contains the:
                                    1. MarketID
                                    2. Broker Address
                                    3. Beat Address
                                    4. Miss Address
    Returns:
        None

    Example:
        markets_lst = [(1, "0x123", "0xabc", "0xNYC")]
        update_broker_addresses(markets_lst)
    '''

    connection = BaseHook.get_connection("chainedmetrics_prod_deployment")

    logging.info(f"Updating database for below markets: {markets_created}")
    conn = psycopg2.connect(
        host=connection.host, 
        port=connection.port,
        dbname="metrics", 
        user=connection.login, 
        password=connection.password
    )
    
    try:
        cur = conn.cursor()
        for (_id, broker, beat, miss) in markets_created:
            cur.execute(f'''
                UPDATE market
                SET broker_address='{broker}', beat_address='{beat}', miss_address='{miss}'
                WHERE id={_id};
                commit;
            ''')

    except Exception:
        logging.exception('Error updating database')
        raise

    finally:
        conn.close()


def get_market_data(id, ticker, fiscal_period, metric, strike_value, strike_string, metric_symbol,
    
    url_prefix='https://api.chainedmetrics.com/markets'):
    '''
    Formats the KPI markets into a structure that can be used directly to deply a market including
    formatting the name of the ticker, symbpol, etc.
    '''

    market_dict = {
        'beat_symbol': metric_symbol.upper() + "/B",
        'beat_description': format_description(ticker, fiscal_period, metric, strike_string, 'BEAT'),
        'miss_symbol': metric_symbol.upper() + "/M",
        'miss_description': format_description(ticker, fiscal_period, metric, strike_string, 'MISS'),
        'strike_value': strike_value,
        'url': f"{url_prefix}/{id}/{ticker}/{fiscal_period}/{metric}",
        'metric': metric,
        'ticker': ticker,
        'fiscal_period': fiscal_period,
        'id': id
    }

    return market_dict
    
def format_symbol(ticker, fiscal_period, metric, outcome):
    '''
    Formats the symbol that will be used for the Beat and Miss Tokens and 
    returns the string

    Args:
        ticker (str):           The ticker symbol
        fiscal_period (str):    The fiscal period for this market (eg. 'FQ1 2022)
        metric (str):           The metric used for this market
        outcome (str):          Either 'BEAT' or 'MISS'

    Returns:
        str: The formatted symbol for this market
    '''

    assert outcome.upper() in ('BEAT', 'MISS')
    assert ticker.strip().isalpha()
    assert fiscal_period.replace(" ", "").replace("_", "").isalnum()

    return '{ticker}/{fiscal_period}/{metric}/{outcome}'.format(
        ticker=ticker.strip().upper(),
        fiscal_period=fiscal_period.strip().upper().replace(" ", ""),
        metric=metric.strip().upper().replace(" ", "_"),
        outcome=outcome.upper()
    )


def format_description(ticker, fiscal_period, metric, strike_string, outcome):
    '''
    Formats the description that will be used for the Beat and Miss Tokens and 
    returns the string

    Args:
        ticker (str):           The ticker symbol
        fiscal_period (str):    The fiscal period for this market (eg. 'FQ1 2022)
        metric (str):           The metric used for this market
        strike_string(str):     A formated string of the strike price
        outcome (str):          Either 'BEAT' or 'MISS'

    Returns:
        str: The formatted descrition for this market
    '''

    assert outcome.upper() in ('BEAT', 'MISS')
    assert ticker.strip().isalpha()
    assert fiscal_period.replace(" ", "").replace("_", "").isalnum()

    return (
        'Chained Metrics KPI Token for {ticker} {fiscal_period} {metric} {outcome} with a '
        'strike value of {strike_string}. See https://chainedmetrics.com for details.').format(
        ticker=ticker.strip().upper(),
        fiscal_period=fiscal_period.strip(),
        metric=metric.strip(),
        outcome=outcome.title(),
        strike_string=strike_string
    )

def deploy_contract(contract, *args, retry=3):
    '''
    Handles retry logic for contract deploys up to 3 times
    '''

    while retry >= 0:
        try:
            args[-1]['gas_price'] = GasNowScalingStrategy(max_speed="fast", increment=1.2)
            instance = contract.deploy(*args, publish_source=True)
            if isinstance(instance, TransactionReceipt):
                logging.error(f"Contract not deployed and recieved a receipt with this info: {instance.info()}")
                raise Exception('Recieved a Transaction Receipt Error')
            return instance

        except Exception as e:
            logging.exception(f'Failed to deploy {contract} with args: {args}\nerror={str(e)}')
            logging.info(f'Retrying retry={retry}')
            if retry == 0:
                raise

            retry -= 1

def email_failure(context):

    title = "Failure Deploying Binary KPIs"
    description = f"There was a failure deploying KPIs and the job may need to be restarted. Please go to Airflow to restart"
    headers = ['Context', 'Details']
    rows = [[k, str(v)] for k, v in context.items()]
    template = email_table_template(title, description, headers, rows)

    email_connection = BaseHook.get_connection("email")
    recipients = ['michael@chainedmetrics.com', 'jamal@chainedmetrics.com', 'dillon@chainedmetrics.com', 'nick@chainedmetrics.com', 'sachin@chainedmetrics.com']
    subject = "Failure Deploying Binary Markets"
    text = f'Failure Deploying Binary Markets'
    send_email(recipients, email_connection.login, template, subject, text, email_connection.password)


dag = DAG(
    dag_id='deploy_binary_markets',
    schedule_interval='0 2 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["deployment", "admin"],
    on_failure_callback=email_failure,
)

python_report_operator = PythonOperator(
    task_id='deploy_binary_market',
    python_callable=deploy_market,
    dag=dag)