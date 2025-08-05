import logging
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from airbnb_identity import context as aic
from pyhive import presto

host = "presto-gateway-production.presto-gateway-production"
port = 6375
catalog = "silver"
yesterday_ds = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

# Thread-local storage for connections
_thread_local = threading.local()

# Connection cache settings
CONNECTION_TIMEOUT = 300
MAX_CONNECTION_AGE = 600
MAX_REUSE_COUNT = 10  # Maximum number of times to reuse a connection


def _get_thread_connection():
    """Get or create a connection for the current thread"""
    if (
        not hasattr(_thread_local, "connection")
        or not hasattr(_thread_local, "created_at")
        or not hasattr(_thread_local, "use_count")
    ):
        return None

    if (
        _thread_local.connection is None
        or _thread_local.created_at is None
        or _thread_local.use_count is None
    ):
        return None

    # Check if connection is too old
    if time.time() - _thread_local.created_at > MAX_CONNECTION_AGE:
        _close_thread_connection()
        return None

    # Check if connection has been used too many times
    if _thread_local.use_count >= MAX_REUSE_COUNT:
        _close_thread_connection()
        return None

    # Check if connection is still alive
    try:
        if _thread_local.connection and hasattr(_thread_local.connection, "_client"):
            # Simple ping test - create a cursor and check if it's usable
            cursor = _thread_local.connection.cursor()
            cursor.close()
            # Increment use count
            _thread_local.use_count += 1
            return _thread_local.connection
    except Exception as e:
        logging.debug(f"CGE - Thread connection test failed: {e}")
        _close_thread_connection()
        return None

    return None


def _close_thread_connection():
    """Close the thread-local connection"""
    if hasattr(_thread_local, "connection") and _thread_local.connection:
        try:
            _thread_local.connection.close()
        except Exception as e:
            logging.debug(f"CGE - Error closing thread connection: {e}")

    # Reset all thread-local attributes
    if hasattr(_thread_local, "connection"):
        delattr(_thread_local, "connection")
    if hasattr(_thread_local, "created_at"):
        delattr(_thread_local, "created_at")
    if hasattr(_thread_local, "use_count"):
        delattr(_thread_local, "use_count")


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


@contextmanager
def get_presto_connection():
    """Thread-safe connection manager with connection caching

    Uses thread-local storage to cache connections per thread, improving performance
    for multiple queries within the same thread while maintaining thread safety.
    """
    # Try to get existing thread connection
    conn = _get_thread_connection()

    # Create new connection if needed
    if conn is None:
        conn = _create_new_connection()

    try:
        yield conn
    except Exception as e:
        # If there's an error, close the cached connection as it might be corrupted
        logging.warning(
            f"CGE - Error during connection usage, closing cached connection: {e}"
        )
        _close_thread_connection()
        raise


def cleanup_thread_connection():
    """
    Cleanup function to properly close thread-local connections.
    Should be called when a thread is finishing or when explicit cleanup is needed.
    """
    _close_thread_connection()
    logging.debug("CGE - Thread connection cleaned up")


def get_connection_stats():
    """
    Get connection statistics for monitoring and debugging.

    Returns:
        dict: Connection statistics for the current thread
    """
    stats = {
        "has_connection": hasattr(_thread_local, "connection")
        and _thread_local.connection is not None,
        "connection_age": None,
        "use_count": None,
        "thread_id": threading.current_thread().ident,
    }

    if hasattr(_thread_local, "created_at") and _thread_local.created_at:
        stats["connection_age"] = time.time() - _thread_local.created_at

    if hasattr(_thread_local, "use_count"):
        stats["use_count"] = _thread_local.use_count

    return stats


def execute_hive_query(sql):
    try:
        with get_presto_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)

            if cursor.description:
                desc = cursor.description
                column_names = [col[0] for col in desc]
                data = [dict(zip(column_names, row)) for row in cursor.fetchall()]
                return data
            else:
                return None
    except Exception as e:
        logging.error(f"CGE - Error executing Hive operation: {e}")
        raise
    finally:
        if 'cursor' in locals() and cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass

# Thread-safe cache for table columns
_table_columns_cache = {}
_cache_lock = threading.Lock()


def get_table_columns(table_name):
    """
    Get table columns with their data types.

    Returns:
        list: List of dictionaries with 'column_name' and 'data_type' keys
    """
    if table_name in _table_columns_cache:
        return _table_columns_cache[table_name].copy()

    # If not in cache, fetch from database
    try:
        with get_presto_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DESCRIBE tmp.{table_name}")

            if not cursor.description:
                raise ValueError(
                    f"Could not retrieve schema for table tmp.{table_name}"
                )

            schema_columns = [col[0] for col in cursor.description]
            schema_rows = cursor.fetchall()

            table_columns = []
            for row in schema_rows:
                col_data = dict(zip(schema_columns, row))
                col_name = col_data.get("column_name") or col_data.get("Column")
                col_type = (
                    col_data.get("data_type")
                    or col_data.get("Type")
                    or col_data.get("type")
                )
                if col_name:
                    table_columns.append(
                        {"column_name": col_name, "data_type": col_type or "string"}
                    )

            # Cache the result (with lock)
            with _cache_lock:
                _table_columns_cache[table_name] = table_columns.copy()

            return table_columns
    except Exception as e:
        logging.error(f"CGE - Error getting table columns for {table_name}: {e}")
        raise
    finally:
        if 'cursor' in locals() and cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass


def create_hive_table(table_name):
    sql_file_path = Path(__file__).parent / "hive_sql" / f"{table_name}.sql"

    try:
        with open(sql_file_path, "r") as sql_file:
            sql = sql_file.read()
        execute_hive_query(sql)
        logging.info(f"CGE - Hive table {table_name} created successfully")
    except FileNotFoundError:
        logging.error(f"CGE - SQL file {sql_file_path} not found")
    except Exception as e:
        logging.error(f"CGE - Error creating Hive table {table_name}: {e}")
        raise


def insert_data(table_name, json_data, lead_id=None):
    """
    Insert JSON data into the specified Hive table.
    Dynamically determines table schema and maps JSON fields (max 2 nested levels).

    Args:
        table_name (str): Name of the table to insert data into
        json_data (dict): JSON data to insert
    """
    try:
        table_columns = get_table_columns(table_name)

        # logging.info(f"CGE - table_columns = {table_columns} \n\r json_data = {json_data}")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_date = datetime.now().strftime("%Y-%m-%d")

        column_names = []
        column_values = []

        for col_info in table_columns:
            col_name = col_info["column_name"]
            col_type = col_info["data_type"]
            column_names.append(col_name)

            # Handle special columns
            if col_name == "id":
                column_values.append(f"'{str(uuid.uuid4())}'")
            elif col_name == "leadid" and lead_id is not None:
                column_values.append(f"'{lead_id}'")
            elif col_name in ["createdat", "updatedat"]:
                column_values.append(f"CAST('{current_time}' AS TIMESTAMP)")
            elif col_name == "dt":
                column_values.append(f"'{yesterday_ds}'")
            else:
                value = get_json_value(json_data, col_name)

                if col_type in ["integer", "bigint", "double", "float"]:
                    column_values.append(
                        str(value) if value is not None and value != "" else "NULL"
                    )
                else:
                    if value is None:
                        value = ""
                    escaped_value = str(value).replace("'", "''")
                    column_values.append(f"'{escaped_value}'")

        columns_str = ", ".join(column_names)
        values_str = ", ".join(column_values)

        insert_sql = f"""
        INSERT INTO tmp.{table_name} 
        ({columns_str})
        VALUES 
        ({values_str})
        """

        execute_hive_query(insert_sql)

    except Exception as e:
        logging.error(f"CGE - Error inserting data into {table_name}: {e}")
        raise


def insert_data_batch(table_name, json_data_array, lead_id=None):
    """
    Insert multiple JSON data records into the specified Hive table in a single batch operation.
    Dynamically determines table schema and maps JSON fields (max 2 nested levels).

    Args:
        table_name (str): Name of the table to insert data into
        json_data_array (list): List of JSON data dictionaries to insert
        lead_id (str, optional): Lead ID to use for all records
    """
    try:
        if not json_data_array or len(json_data_array) == 0:
            logging.warning(f"CGE - No data to insert into {table_name}")
            return

        table_columns = get_table_columns(table_name)

        # logging.info(f"CGE - table_columns = {table_columns} \n\r json_data_array length = {len(json_data_array)}")

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_date = datetime.now().strftime("%Y-%m-%d")

        # Get column names for the INSERT statement
        column_names = [col_info["column_name"] for col_info in table_columns]
        columns_str = ", ".join(column_names)

        # Generate VALUES clauses for all records
        all_values = []

        for json_data in json_data_array:
            column_values = []

            for col_info in table_columns:
                col_name = col_info["column_name"]
                col_type = col_info["data_type"]

                # Handle special columns
                if col_name == "id":
                    column_values.append(f"'{str(uuid.uuid4())}'")
                elif col_name == "leadid" and lead_id is not None:
                    column_values.append(f"'{lead_id}'")
                elif col_name in ["createdat", "updatedat"]:
                    column_values.append(f"CAST('{current_time}' AS TIMESTAMP)")
                elif col_name == "dt":
                    column_values.append(f"'{yesterday_ds}'")
                else:
                    value = get_json_value(json_data, col_name)
                    if value is None:
                        value = ""

                    if col_type in ["integer", "bigint", "double", "float"]:
                        column_values.append(str(value))
                    else:
                        escaped_value = str(value).replace("'", "''")
                        column_values.append(f"'{escaped_value}'")

            values_str = "(" + ", ".join(column_values) + ")"
            all_values.append(values_str)

        # Create the batch INSERT SQL
        all_values_str = ",\n        ".join(all_values)

        insert_sql = f"""
        INSERT INTO tmp.{table_name} 
        ({columns_str})
        VALUES 
        {all_values_str}
        """

        execute_hive_query(insert_sql)
        logging.info(
            f"CGE - Successfully inserted {len(json_data_array)} records into {table_name}"
        )

    except Exception as e:
        logging.error(f"CGE - Error batch inserting data into {table_name}: {e}")
        raise


def get_json_value(data, field_name):
    """
    Extract value from JSON data with max 2 nested levels.
    hive table field_name is always in lowercase, but data contains camelCase keys.
    """
    # Create lowercase mapping of JSON keys for comparison
    lower_data_map = {k.lower(): k for k in data.keys()}

    # Try direct match (case-insensitive)
    if field_name.lower() in lower_data_map:
        original_key = lower_data_map[field_name.lower()]
        return data[original_key]

    # Handle nested fields (parent_child format)
    if "_" in field_name:
        parts = field_name.split("_", 1)  # Split only on first underscore
        parent, child = parts[0], parts[1]

        # Find parent object (case-insensitive)
        parent_obj = None
        for key, value in data.items():
            if key.lower() == parent.lower() and isinstance(value, dict):
                parent_obj = value
                break

        if parent_obj:
            # Create lowercase mapping for nested object
            lower_parent_map = {k.lower(): k for k in parent_obj.keys()}

            # Try to find child (case-insensitive)
            if child.lower() in lower_parent_map:
                original_child_key = lower_parent_map[child.lower()]
                return parent_obj[original_child_key]

    return None

def create_trino_hive_client():
    context = aic.current_context()
    logging.info(
        "Setting up Trino Hive Credential for %s",
        context
    )

    if context.is_interactive:
        from airbnb_trino_client import create_client
        ret_client = create_client()
        logging.info("Using interactive credentials for Trino Hive client")
    else:
        ret_client = _create_new_connection()
        logging.info("Using AirMesh service-to-service for Trino Hive Client")

    return ret_client

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