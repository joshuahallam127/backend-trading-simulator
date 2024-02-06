from flask import Flask, request, abort, jsonify
from flask_cors import CORS, cross_origin
import pytz
import datetime
from dotenv import load_dotenv
import mysql.connector
import os
from celery import Celery, Task
import requests

load_dotenv()
app = Flask(__name__)
CORS(app, resources={r'/api/*': { 'origins': ['http://localhost:3000', 'https://joshuahallam127.github.io/']}})
app.config.from_mapping(
    CELERY=dict(
        broker_url="redis://localhost",
        result_backend="redis://localhost",
        task_ignore_result=True,
    ),
)

def celery_init_app(app: Flask) -> Celery:
    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object) -> object:
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app = Celery(app.name, task_cls=FlaskTask)
    celery_app.config_from_object(app.config["CELERY"])
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app

celery_app = celery_init_app(app)

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
@cross_origin()
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
@cross_origin()
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
@cross_origin()
def get_data():
    # get the arguments parsed
    ticker = request.args.get('ticker', None)
    start_month = request.args.get('startMonth', None)
    end_month = request.args.get('endMonth', None)
    if ticker is None or start_month is None or end_month is None:
        abort(400, 'Missing parameters')
    
    # get the data from the mysql database
    conn, cursor = connect_to_mysql()
    cursor.execute('''
                   select date, intervalLabel, close 
                    from stocks 
                    where ticker=%s and date>=%s and date<=%s 
                    order by intervalLabel, date asc
                   ''', (ticker, start_month, end_month))
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
@cross_origin()
def list_ticker_options():
    # return all the loaded datasets from the mysql database
    conn, cursor = connect_to_mysql()
    cursor.execute('select distinct ticker from stocks')
    loaded_datasets = [ticker[0] for ticker in cursor.fetchall()]
    close_mysql_connection(conn, cursor)
    return jsonify(loaded_datasets)

@app.route('/api/download_data', methods=['GET'])
@cross_origin()
def download_data():
    # get ticker argument
    ticker = request.args.get('ticker', None)
    start_month_str = request.args.get('startMonth', None)
    end_month_str = request.args.get('endMonth', None)
    if ticker is None or start_month_str is None or end_month_str is None:
        return abort(400, 'A parameter is missing')
    
    # download data using alpha vantage api, store in mysql database
    # url default stuff
    start = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
    args = '&interval=1min&adjusted=false&extended_hours=false&outputsize=full&datatype=csv'    

    # get all months we need to download
    months = []
    start_year, start_month = int(start_month_str.split('-')[0]), int(start_month_str.split('-')[1])
    end_year, end_month = int(end_month_str.split('-')[0]), int(end_month_str.split('-')[1])
    # handle differently depending on if we only getting data from same year or not
    if start_year == end_year:
        for i in range(start_month, end_month + 1):
            months.append(f'{start_year}-{i:02}')
    else:
        for i in range(start_year, end_year + 1):
            if i == start_year:
                for j in range(start_month, 13):
                    months.append(f'{i}-{j:02}')
            elif i == end_year:
                for j in range(1, end_month + 1):
                    months.append(f'{i}-{j:02}')
            else:
                for j in range(1, 13):
                    months.append(f'{i}-{j:02}')

    # remove data from first month if exists in the database
    conn, cursor = connect_to_mysql()
    cursor.execute('delete from stocks where ticker=%s and date like %s', (ticker, f'{start_year}-{start_month:02}%'))

    batch_data = []

    # download the data
    for month in months:
        url = f'{start}&symbol={ticker}&apikey={os.environ["ALPHA_VANTAGE_API_KEY"]}&month={month}{args}'
        r = requests.get(url)
        data = r.text
        arr = data.split('\r\n')[1:]
        arr.reverse()
        if not arr:
            abort(400, 'ran out of api calls')
        arr.pop(0)
        data = [line.split(',') for line in arr]

        # clean up 1min data
        new_data = []

        new_data.append(data[0])
        prev_datetime, prev_close = data[0][0], data[0][4]
        prev_date, prev_time = prev_datetime.split()
        prev_hour, prev_min = int(prev_time.split(':')[0]), int(prev_time.split(':')[1])

        # go through all lines and check for missing minutes
        i = 1
        while i < len(data):
            curr_datetime, close = data[i][0], data[i][4]
            date, time = curr_datetime.split()
            hour, minute = int(time.split(':')[0]), int(time.split(':')[1])

            # see if we've missed a minute
            if prev_min == 59 and (minute == 00 or minute == 30) or prev_min + 1 == minute:
                new_data.append(data[i])
                prev_date, prev_time, prev_close, prev_hour, prev_min = date, time, close, hour, minute
                i += 1
            elif prev_min + 1 != minute:
                # check if we're at the end of an hour
                if prev_min == 59:
                    # check if we're at the end of a day
                    if prev_hour == 15:
                        return jsonify(f'There is no volume at the beginning of the day at {date} {time}')
                    else:
                        # add in the missing minute
                        prev_hour += 1
                        prev_min = 0
                        new_data.append([prev_date, f'{prev_hour:02}:{prev_min:02}:00', prev_close, prev_close, prev_close, prev_close, 0])
                else:
                    # add in the missing minute
                    prev_min += 1
                    new_data.append([prev_date, f'{prev_hour:02}:{prev_min:02}:00', prev_close, prev_close, prev_close, prev_close, 0])
        for values in new_data:
            batch_data.append((ticker, '1min', values[0], values[1], values[2], values[3], values[4], values[5]))
        
        # get 1day data
        data_1day = []
        datetime_curr, open_price, high, low, close, volume = new_data[0]
        volume = float(volume)
        date, time = datetime_curr.split()
        for arr in new_data[1:]:
            if arr[0].split()[0] != date:
                data_1day.append([datetime_curr, open_price, high, low, close, volume])
                datetime_curr, open_price, high, low, close, volume = arr
                volume = float(volume)
                date, time = datetime_curr.split()
            else:
                high = max(high, arr[3])
                low = min(low, arr[4])
                close = arr[4]
                volume += float(arr[5])
        data_1day.append([datetime_curr, open_price, high, low, close, volume])

        for values in data_1day:
            batch_data.append((ticker, '1day', values[0], values[1], values[2], values[3], values[4], values[5]))

        # return jsonify({'1day': data_1day, '1min': new_data})

        if len(batch_data) > 100000:
            print('batching data')
            cursor.executemany('insert into stocks values (%s, %s, %s, %s, %s, %s, %s, %s)', batch_data)
            batch_data = []

    if batch_data:
        cursor.executemany('insert into stocks values (%s, %s, %s, %s, %s, %s, %s, %s)', batch_data)
    
    # update calls remaining
    cursor.execute('select * from calls')
    calls_remaining = cursor.fetchone()[2]
    eastern = pytz.timezone('US/Eastern')
    now = datetime.datetime.now()
    now = now.replace(tzinfo=pytz.utc).astimezone(eastern)
    date = f'{now.year}{now.month}{now.day}'
    cursor.execute('update calls set lastQueryDate=%s, callsRemaining=%s', (date, calls_remaining - len(months)))

    close_mysql_connection(conn, cursor)
    return jsonify('succesffuly downloaded data')



if __name__ == '__main__':
    app.run(debug=True)