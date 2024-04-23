import os
from pathlib import Path
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()




def create_connection():
    # Setup the connection to the Cassandra cluster
    # Adjust the contact_points and authentication as necessary
    cluster = Cluster(
        contact_points=[os.environ.get('CASSANDRA_SERVER_CONTACT_POINTS')],  # Replace with your cluster's IPs
        auth_provider=PlainTextAuthProvider(username=os.environ.get('CASSANDRA_USER'), password=os.environ.get('CASSANDRA_PASSWORD'))  # Adjust as needed
    )
    session = cluster.connect()

    return cluster, session


def create_key_space(session):
    cql_keyspace = """
        CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {{
          'class': 'SimpleStrategy',
          'replication_factor': '1'
        }} AND durable_writes = true;
        """.format(os.environ.get('KEY_SPACE'))
    try:
        session.execute(cql_keyspace)
        # session.execute(cql_table)
        print("key space created successfully.")
    except Exception as e:
        print("An error occurred:", e)


def create_table(session):
    # CQL to create the table

    cql_table = """
    CREATE TABLE IF NOT EXISTS weather_data (
        station_id text,
        station_name text,
        variable text,
        timestamp timestamp,
        year int,
        month int,
        day int,
        value double,
        PRIMARY KEY ((station_id,variable), timestamp)
    ) WITH CLUSTERING ORDER BY (timestamp DESC);
    """
    try:
        session.execute(cql_table)
        # session.execute(cql_table)
        print("Table created successfully.")
    except Exception as e:
        print("An error occurred:", e)


def main():
    cluster, session = create_connection()
    create_key_space(session)

    session.set_keyspace(os.environ.get('KEY_SPACE'))
    create_table(session)

    # Close the connection
    cluster.shutdown()


if __name__ == '__main__':
    main()
