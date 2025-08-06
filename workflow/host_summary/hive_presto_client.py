import logging
import threading
import time
from airbnb_identity import context as aic
from pyhive import presto

host = "presto-gateway-production.presto-gateway-production"
port = 6375
catalog = "silver"


# Thread-local storage for connections
_thread_local = threading.local()
CONNECTION_TIMEOUT = 300

def _create_new_connection():
    """Create a new connection and store it in thread-local storage"""
    try:
        conn = presto.connect(
            host=host,
            port=port,
            catalog=catalog,
            requests_kwargs={"timeout": CONNECTION_TIMEOUT},
        )
        _thread_local.connection = conn
        _thread_local.created_at = time.time()
        _thread_local.use_count = 0
        return conn
    except Exception as e:
        logging.error(f"CGE - Error creating new connection: {e}")
        raise

def create_trino_hive_client_cursor():
    context = aic.current_context()
    logging.info(
        "Setting up Trino Hive Credential for %s",
        context
    )

    if context.is_interactive:
        from airbnb_trino_client import create_client
        ret_client = create_client()
        ret_cursor = ret_client.conn.cursor()
        logging.info("Using interactive credentials for Trino Hive client")
    else:
        ret_client = _create_new_connection()
        ret_cursor = ret_client.cursor()
        logging.info("Using AirMesh service-to-service for Trino Hive Client")

    return ret_cursor