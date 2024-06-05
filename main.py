import os
import asyncio
from etl import fetch_data, load_to_db, transform2
import datetime
from dotenv import load_dotenv

load_dotenv()


async def main():
    while True:
        target_variables = os.environ.get('VARIABLES').split(',')
        target_levels = os.environ.get('LEVELS').split(',')
        existing_variables = await fetch_data({'view': 'getvariables'})
        variables_data = {}
        for var in existing_variables['entries']:
            keys = var.keys()
            if var['name'] in target_variables:
                if 'level' in keys:
                    if var['level'] in target_levels:
                        variables_data[var['varid']] = var['name']
                else:
                    variables_data[var['varid']] = var['name']

        stations = await fetch_data({'view': 'getstations'})
        print(len(stations['entries']))
        for station in stations['entries']:
            start_from = datetime.date.today() - datetime.timedelta(int(os.environ.get('START_FROM')))
            for var_id, var_name in variables_data.items():
                payload = {
                    'view': 'getmeas',
                    'varid': str(var_id),
                    'fromdate': str(start_from),
                    'todate': str(datetime.date.today()),
                    'statid': station['statid']
                }
                response = await fetch_data(payload)
                data = response['entries'][0]
                transformed_data = await transform2(data)
                await load_to_db(transformed_data, var_name, station['statid'], station['eovx'], station['eovy'],
                                 station['name'])
                await asyncio.sleep(0.1)
                print(f"Data for {station['name']}{var_name} loaded successfully")


if __name__ == "__main__":
    asyncio.run(main())
