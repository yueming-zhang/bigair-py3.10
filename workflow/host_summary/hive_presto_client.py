import logging
from airbnb_identity import context as aic
from pyhive import presto

host = "presto-gateway-production.presto-gateway-production"
port = 6375
catalog = "silver"

CONNECTION_TIMEOUT = 300

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
        conn = presto.connect(
            host=host,
            port=port,
            catalog=catalog,
            requests_kwargs={"timeout": CONNECTION_TIMEOUT},
        )
        ret_cursor = conn.cursor()
        logging.info("Using AirMesh service-to-service for Trino Hive Client")

    return ret_cursor