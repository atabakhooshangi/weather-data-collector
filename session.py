import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()


async def db_session():
    session = None
    try:
        cluster = Cluster(
            contact_points=[os.environ.get('CASSANDRA_SERVER_CONTACT_POINTS')],  # Replace with your cluster's IPs
            auth_provider=PlainTextAuthProvider(username=os.environ.get('CASSANDRA_USER'), password=os.environ.get('CASSANDRA_PASSWORD'))  # Adjust as needed
        )
        session = cluster.connect()
        session.set_keyspace(os.environ.get('KEY_SPACE'))
        yield session
    except Exception as e:
        print("DB error", e)
    finally:
        if session:
            session.shutdown()


