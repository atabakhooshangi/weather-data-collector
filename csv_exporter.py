import datetime
import os
import time
from contextlib import asynccontextmanager
import pandas as pd
import argparse
import asyncio
from session import db_session


async def fetch_data(session, query):
    # Execute the query and fetch all data
    rows = session.execute(query)
    return pd.DataFrame(list(rows))


async def export_data(df, flag):
    today = datetime.datetime.today()
    csv_store_path = './csv_store'
    if not os.path.exists(csv_store_path):
        os.makedirs(csv_store_path)
        print(f"Created missing '{csv_store_path}' directory for storing CSV.")
    else:
        print(f"'{csv_store_path}' directory already exists.")
    if flag == 'all':
        # Export all data into one CSV
        file_name = f'{csv_store_path}/hungary_weather_data_{today.year}_{today.month}_{today.day}.csv'
        if os.path.exists(file_name):
            raise Exception("A CSV file with same name and data already exists")
        df.to_csv(file_name, index=False)
        print(f"All data exported to {file_name}")
    elif flag == 'partial':
        # Export data for each station into separate CSV files
        for station_name, group in df.groupby('station_name'):
            filename = f"{csv_store_path}/{station_name}_{today.year}_{today.month}_{today.day}.csv"
            group = group.sort_values(by='timestamp', ascending=True)
            group.to_csv(filename, index=False)
            print(f"Data for {station_name} exported to '{filename}'")
            break
    elif flag == 'wide':
        try:
            for station_name, group in df.groupby('station_name'):
                filename = f"{csv_store_path}/pivoted_{station_name}.csv"
                group = group.sort_values(by='timestamp', ascending=True)
                # Pivot the DataFrame
                pivoted_data = group.pivot_table(
                    index=['station_id', 'station_name', 'timestamp', 'year', 'month', 'day', 'station_eovx',
                           'station_eovy'],
                    columns='variable',
                    values='value'
                ).reset_index()
                pivoted_data.columns.name = None

                # Display the pivoted DataFrame

                print(pivoted_data.head())

                # Optionally, save the pivoted data to a new CSV file
                pivoted_data.to_csv(filename, index=False)
                print(f"Data for {station_name} exported to '{filename}'")
        except Exception as e:
            raise e
    elif flag == 'wide_all':
        try:
            filename = f"{csv_store_path}/pivoted_all_data.csv"
            df = df.sort_values(by=['station_name', 'timestamp'], ascending=True)
            # Pivot the DataFrame
            # df.dropna(inplace=True)

            # pivoted_data = df.pivot_table(
            #     index=['station_id', 'station_name', 'timestamp', 'year', 'month', 'day', 'station_eovx',
            #            'station_eovy'],
            #     columns='variable',
            #     values='value'
            # ).reset_index()
            # pivoted_data.columns.name = None
            # pivoted_data.rename(columns={
            #     'Levegőhőmérséklet': 'air_temperature',
            #     'Relatív páratartalom': 'relative_humidity',
            #     'Talajhőmérséklet': 'soil_temperature',
            #     'Talajnedvesség': 'soil_moisture'
            # }, inplace=True)
            #
            #
            # # Optionally, save the pivoted data to a new CSV file
            # time.sleep(3)
            # pivoted_data.to_csv(filename, index=False)
            # print(f"All data exported to '{filename}'")

            # with chunk
            chunk_size = 5000
            for start in range(0, len(df), chunk_size):
                end = start + chunk_size
                chunk = df.iloc[start:end]
                pivoted_chunk = chunk.pivot_table(
                    index=['station_id', 'station_name', 'timestamp', 'year', 'month', 'day', 'station_eovx',
                           'station_eovy'],
                    columns='variable',
                    values='value'
                ).reset_index()
                pivoted_chunk.columns.name = None
                pivoted_chunk.rename(columns={
                    'Levegőhőmérséklet': 'air_temperature',
                    'Relatív páratartalom': 'relative_humidity',
                    'Talajhőmérséklet': 'soil_temperature',
                    'Talajnedvesség': 'soil_moisture'
                }, inplace=True)
                mode = 'w' if start == 0 else 'a'
                header = True if start == 0 else False
                pivoted_chunk.to_csv(filename, index=False, mode=mode, header=header)
                print(f"Processed chunk {start} to {end}")

        except Exception as e:
            raise e


async def main(all_: str = 'all'):
    async with asynccontextmanager(db_session)() as session:
        # Modify the query as per your data retrieval needs
        query = 'SELECT * FROM weather_data'

        data = await fetch_data(session, query)
        print(len(data))
        await export_data(data, all_)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Export Data to csv')
    parser.add_argument('--all', action='store_true', help='Export all the data in single CSV file')
    parser.add_argument('--wide', action='store_true',
                        help='Export all the data in station name CSV file as wide format')
    parser.add_argument('--wide_all', action='store_true',
                        help='Export all the data in station name CSV file as wide format')
    args = parser.parse_args()
    all_flag = 'all' if args.all else 'wide' if args.wide else 'wide_all' if args.wide_all else 'partial'

    asyncio.run(main(all_flag))

    # Command-line argument setup
