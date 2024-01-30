from flask import Flask, request, abort, jsonify
from flask_cors import CORS
import pytz
import datetime
# from celery import Celery
# from celery.result import AsyncResult
# import celery_config
# import mysql
import mysql.connector
import os


app = Flask(__name__)
# CORS(app, resources={r'/api/*': {'origins': ['http://localhost:3000', 'https://joshuahallam127.github.io/']}})
CORS(app, resources={r'/api/*': {'origins': '*'}})
# celery = Celery()
# celery.config_from_object(celery_config)
# conn = sqlite3.connect('database.db')

def connect_to_mysql():
    conn = mysql.connector.connect(
        user='avnadmin',
        password=os.environ['AVNADMIN_PASSWORD'],
        host='mysql-database-trading-simulator.a.aivencloud.com',
        port='25687',
        database='defaultdb',
    )
    cursor = conn.cursor()
    return conn, cursor

def close_mysql_connection(conn, cursor):
    conn.commit()
    cursor.close()
    conn.close()

# @celery.task(bind=True)
# def celery_download_data(self, ticker):
#     # download data using other python program
#     os.system('test.py')
#     # os.system(f'python download_data.py {ticker.lower()} -w')

# @app.route('/api/download_data', methods=['GET'])
# def download_data():
#     ticker = request.args.get('ticker', None)
#     start_month = request.args.get('startMonth', None)
#     end_month = request.args.get('endMonth', None)
#     cost = request.args.get('cost', None)
#     if ticker is None or start_month is None or end_month is None or cost is None:
#         abort(400, 'Missing arguments')

#     # make sure we have enough api calls left to do this
#     if calls_remaining < int(cost):
#         return jsonify('Sorry, that operation is too expensive, please wait until tomorrow for the api \
#                        calls to reset to 25.')
    
#     # DOWNLOAD DATA CODE HERE
#     task = celery_download_data.apply_async(args=[ticker])

#     # tell user data is downloading, let the client query until data is downloaded
#     return jsonify({
#         'message': 'Downloading data. Please wait...', 
#         'status': 'downloading', 
#         'task_id': str(task.id)})

# @app.route('/api/check_task_status', methods=['GET'])
# def check_task_status():
#     task_id = request.args.get('task_id', None)
#     if task_id is None:
#         abort(400, 'Task ID paramater is missing')
    
#     result = AsyncResult(task_id)
#     if result.ready():
#         return jsonify('COMPLETED')
#     else:
#         return jsonify('IN_PROGRESS')
    
@app.route('/', methods=['GET'])
def index():
    return 'Hello World'

@app.route('/api/get_remaining_calls', methods=['GET'])
def get_remaining_calls():
    # get how many api calls we have for the rest of the day stored in mysql database, update if need be
    conn, cursor = connect_to_mysql()
    cursor.execute('select * from calls')
    _, last_call_date, calls_remaining = cursor.fetchone()
    eastern = pytz.timezone('US/Eastern')
    now = datetime.datetime.now()
    now = now.replace(tzinfo=pytz.utc).astimezone(eastern)
    date = f'{now.year}{now.month}{now.day}'
    if last_call_date != date:
        calls_remaining = 25
        cursor.execute('update calls set lastQueryDate=%s, callsRemaining=%s', (date, calls_remaining))
        conn.commit()
        cursor = conn.cursor()
    close_mysql_connection(conn, cursor)

    # return the number of calls we have left
    return jsonify(calls_remaining)

@app.route('/api/get_months_data', methods=['GET'])
def get_months_data():
    # get the arguments passed
    ticker = request.args.get('ticker', None)
    if ticker is None:
        abort(400, 'Ticker paramater is missing')

    loaded_datasets = list_ticker_options().json
    # return an array of empty months if we don't have the data
    if ticker not in loaded_datasets:
        return jsonify([])
    
    # get the months that we have data for for the stock from mysql database
    conn, cursor = connect_to_mysql()
    cursor.execute('select min(date) from stocks where ticker=%s', (ticker,))
    first_date = cursor.fetchone()[0]
    cursor.execute('select max(date) from stocks where ticker=%s', (ticker,))
    last_date = cursor.fetchone()[0]
    close_mysql_connection(conn, cursor)
    return jsonify([first_date.split()[0], last_date.split()[0]])

@app.route('/api/get_data', methods=['GET'])
def get_data():
    # get the arguments parsed
    ticker = request.args.get('ticker', None)
    if ticker is None:
        abort(400, 'Ticker or interval paramater is missing')

    loaded_datasets = list_ticker_options().json
    # return empty arrays if no data
    if ticker not in loaded_datasets:
        return jsonify({'1minute': [], '1day': []})
    
    # get the data from the mysql database
    conn, cursor = connect_to_mysql()
    cursor.execute('select date, intervalLabel, close from stocks where ticker=%s order by intervalLabel, date asc', (ticker,))
    data = cursor.fetchall()
    data_1min = []
    data_1day = []
    for line in data:
        if line[1] == '1min':
            data_1min.append([line[0], line[2]])
        elif line[1] == '1day':
            data_1day.append([line[0], line[2]])
    close_mysql_connection(conn, cursor)
    return jsonify({'1min': data_1min, '1day': data_1day})

@app.route('/api/list_ticker_options', methods=['GET'])
def list_ticker_options():
    # return all the loaded datasets from the mysql database
    conn, cursor = connect_to_mysql()
    cursor.execute('select distinct ticker from stocks')
    loaded_datasets = [ticker[0] for ticker in cursor.fetchall()]
    close_mysql_connection(conn, cursor)
    return jsonify(loaded_datasets)

if __name__ == '__main__':
    app.run(debug=True)