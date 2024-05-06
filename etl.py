import os
import collections
from session import db_session
from contextlib import asynccontextmanager
from cassandra.concurrent import execute_concurrent_with_args
import aiohttp
from dotenv import load_dotenv
import datetime
import ssl
import certifi

load_dotenv()


async def fetch_data(payload):
    session_timeout = aiohttp.ClientTimeout(15)
    context = ssl.create_default_context(cafile=certifi.where())
    try:
        async with aiohttp.ClientSession(timeout=session_timeout) as async_session:
            async with await async_session.request(
                    method='post',
                    ssl=context,
                    url=os.environ.get('BASE_DATA_SRC_URL'),
                    data=payload,
            ) as resp:
                json_resp = await resp.json()
                return json_resp
    except Exception as exc:
        raise exc


async def parse_date_parts(date_str):
    date_part, time_part = date_str.split(' ')
    year, month, day = date_part.split('-')[:3]
    return int(year), int(month), int(day)


async def date_to_timestamp(date_str):
    return datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S.%f').timestamp()


async def transform2(data: dict):
    # Dictionary to hold sum of values and count of entries for each day
    daily_values = collections.defaultdict(lambda: {'sum': 0, 'count': 0})

    for entry in data.values():
        value = entry['value']
        if value is None:
            continue  # Skip this entry if the value is None
        value = float(value)
        year, month, d_day = await parse_date_parts(entry['date'])
        date = datetime.datetime.strptime(entry['date'], '%Y-%m-%d %H:%M:%S.%f')
        day = datetime.datetime(date.year, date.month, date.day, 12).timestamp() * 1000
        daily_values[day]['year'] = year
        daily_values[day]['month'] = month
        daily_values[day]['day'] = d_day
        daily_values[day]['sum'] += value
        daily_values[day]['count'] += 1

    # Calculating averages
    daily_averages = [{'timestamp': day, 'avg_val': values['sum'] / values['count'], 'year': values['year'], 'month': values['month'], 'day': values['day']} for day, values in daily_values.items() if values['count'] > 0]
    return daily_averages


async def transform(data: dict):
    try:
        for k, v in data.items():
            v['year'], v['month'], v['day'] = await parse_date_parts(v['date'])
            v['date'] = await date_to_timestamp(v['date'])
            v['value'] = float(v['value']) if v['value'] is not None else None
        return data
    except Exception as e:
        raise e


# async def load_to_db(data: dict, variable: str, station_id: str, station_name: str):
#     async with asynccontextmanager(db_session)() as session:
#         insert_stmt = session.prepare("""
#             INSERT INTO weather_data (station_id,station_name, variable,year,month,day, timestamp, value)
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#         """)
#         try:
#             batch = BatchStatement()
#             for key,record in data.items():
#                 # Convert the timestamp string to a datetime object if necessary
#
#                 batch.add(insert_stmt, (station_id, station_name, variable, record['year'], record['month'], record['day'], record['date'], record["value"]))
#
#                 # Check if the batch size is large enough to send
#                 if len(batch) >= 300:  # Adjust this number based on your performance tests
#                     await session.execute_async(batch)
#                     batch = BatchStatement()  # Resetting the batch after execution
#
#             # Execute any remaining queries in the batch
#             if len(batch) > 0:
#                 await session.execute_async(batch)
#         except Exception as e:
#             raise e
#
#     return

async def load_to_db(data: dict, variable: str, station_id: str, station_eovx: str, station_eovy: str, station_name: str):
    async with asynccontextmanager(db_session)() as session:
        insert_stmt = session.prepare("""
            INSERT INTO weather_data (station_id, station_name,station_eovx , station_eovy, variable, year, month, day, timestamp, value)
            VALUES (?, ? ,?, ?, ?, ?, ?, ?, ?, ?)
        """)
        batch = []
        for record in data:
            # timestamp = datetime.datetime.strptime(record["timestamp"], "%Y-%m-%d %H:%M:%S")
            batch.append((station_id, station_name, station_eovx, station_eovy, variable, record['year'], record['month'], record['day'], record['timestamp'], record["avg_val"]))

            if len(batch) >= 300:  # Check if the batch size is large enough to send
                # print(batch,'\n\n\n')
                await execute_batch(session, insert_stmt, batch)
                batch = []  # Resetting the batch after execution

        if batch:
            await execute_batch(session, insert_stmt, batch)


async def execute_batch(session, statement, batch):
    # Execute the batch concurrently and await its completion
    results = execute_concurrent_with_args(session, statement, batch)
    # Await results if needed or process them directly
    for success, result_or_exc in results:
        if not success:
            print("Query failed:", result_or_exc)
