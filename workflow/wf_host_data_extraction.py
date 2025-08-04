from bigair import step, workflow
from typing import List
import pwd
import json
import os

@step
def query_hive_data() -> str:
    """Execute a Hive query to get sample data from homes.listing__dim_active table."""
    try:
        from . import hive_presto_client
        sql = "select * from homes.listing__dim_active LIMIT 10"
        result = hive_presto_client.execute_hive_query(sql)
        
        if result:
            print(f"Query executed successfully. Retrieved {len(result)} rows:")
            for i, row in enumerate(result, 1):
                print(f"Row {i}: {row}")
            return f"Successfully queried homes.listing__dim_active table, got {len(result)} rows"
        else:
            print("Query executed but returned no data")
            return "Query executed but returned no data"

    except Exception as e:
        print(f"Error executing Hive query: {e}")
        return f"Failed to execute query: {str(e)}"


@workflow
def host_data_extraction_workflow():
    _ = query_hive_data()