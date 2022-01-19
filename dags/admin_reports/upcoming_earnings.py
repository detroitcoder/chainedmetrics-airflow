import enum
import psycopg2
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
from utilities import send_email, email_table_template
from utilities.contract_analyzer import issued_tokens, open_orders, setup_network, get_contracts


def anchor_text(text, id):

    return f'<a href="https://chainedmetrics.com/trade/{id}" target="_blank">{text}</a>'

def get_issuance_and_volume(addresses):
    '''
    Uses the utilities.contract_analyzer functions to look up issuance quantity
    and total quantity in depth of book

    Args:
        addresses (list of strs): List of Broker, Beat, and Miss Contract

    Returns:
        volumes (list): List of ints with volumes
    '''

    volumes = []
    for (broker_addr, beat_addr, miss_addr) in addresses:
        if all((broker_addr, beat_addr, miss_addr)):
            broker, beat, miss = get_contracts(broker_addr, beat_addr, miss_addr)
            orders = open_orders(broker)
            
            order_quantity = 0
            for b in (True, False):
                for (_, quantity) in orders[b].values():
                    order_quantity += quantity

            beat_holders = issued_tokens(beat_addr)
            issuance_quantity = sum(beat_holders.values())
            volumes.append([issuance_quantity, order_quantity])
        else:
            volumes.append(['N/A', 'N/A'])
    
    return volumes

def lookup_upcoming_earnings_dates():
    connection = BaseHook.get_connection("chainedmetrics_prod")
    email_conneciton = BaseHook.get_connection("email")
    
    conn = psycopg2.connect(
        host=connection.host, 
        port=connection.port,
        dbname="metrics", 
        user=connection.login, 
        password=connection.password
    )

    try:
        cur = conn.cursor()
        cur.execute((
            'Select * from market '
            "where expected_reporting_date >= Date((NOW())) AND "
            "expected_reporting_date < Date((NOW() + interval '7 day')) "
            "ORDER BY expected_reporting_date ASC;"
        ))

        headers =  [desc[0] for desc in cur.description]
        market_list = [dict(zip(headers, row)) for row in cur]
    finally:
        conn.close()
    
    kpi_list = [[anchor_text(i['metric_symbol'], i['id']), i['expected_reporting_date'], i['metric']] for i in market_list]

    account = setup_network()
    volumes = get_issuance_and_volume([(m['broker_address'], m['beat_address'], m['miss_address']) for m in market_list])

    for i, v in enumerate(volumes):
        kpi_list[i].extend(v)

    description = (
        f"There are {len(market_list)} KPIs reporting in the next 7 days and are listed below. "
        "It is important to monitor these closely as they report and update the resolved values "
        "in the oracle as each company announces"
    )

    html = email_table_template(
        "Upcoming Earnings with KPIs This Week",
        description,
        ["Symbol", "Earnings Date", "KPI", "Issued Tokens", "Open Order Quantity"],
        kpi_list
    )

    text = f'There are {len(kpi_list)} KPIs reporting this week in Chained Metrics'
    to = [
        'michael@chainedmetrics.com', 'jamal@chainedmetrics.com',
        'nick@chainedmetrics.com', 'sachin@chainedmetrics.com', 'baleskyn@msu.edu'
    ]

    # to = ['michael@chainedmetrics.com']

    from_email = 'serviceaccount@chainedmetrics.com'    
    email_pass = email_conneciton.password
    subject = 'Reporting KPIs This Week'
    send_email(to, from_email, html, subject, text, email_pass)
    logging.info('done')

dag = DAG(
    dag_id='upcoming_earnings',
    schedule_interval='0 9 * * SAT',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["reporting", "admin"]
)

python_report_operator = PythonOperator(
    task_id='upcoming_earnings',
    python_callable=lookup_upcoming_earnings_dates,
    dag=dag)
    