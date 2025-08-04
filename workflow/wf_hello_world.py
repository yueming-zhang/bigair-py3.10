from bigair import step, workflow, OutputTuple
from typing import List
import pwd
import json
import os

@step
def print_runtime_info() -> str:
    import boto3
    """Print runtime information including service account and AWS role details."""
    # Get current user/service account
    try:
        current_user = pwd.getpwuid(os.getuid()).pw_name
        print(f"Running as user: {current_user}")
    except Exception as e:
        print(f"Error getting current user: {e}")
        current_user = "Unknown"

    # Print environment variables related to AWS (names only, not values for security)
    aws_env_var_names = [k for k in os.environ.keys() if k.startswith("AWS_")]
    print(f"AWS environment variable names: {json.dumps(aws_env_var_names, indent=2)}")

    # Get AWS role info using boto3
    try:
        sts_client = boto3.client("sts")
        identity = sts_client.get_caller_identity()
        print(f"AWS Identity Information:")
        print(f"  Account: {identity.get('Account')}")
        print(f"  UserId: {identity.get('UserId')}")
        print(f"  ARN: {identity.get('Arn')}")
    except Exception as e:
        print(f"Error getting AWS identity: {e}")

    return "Runtime info task completed"

@step
def step_one() -> OutputTuple(greeting=str, repeat=int):
    return "hello world", 100


@step
def step_two(greeting: str, repeat: int) -> List[str]:
    return [greeting] * repeat


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
            
    except ImportError as e:
        # Fallback to inline implementation if import fails
        print(f"Import failed ({e}), using inline implementation")
        from pyhive import presto
        
        try:
            # Connection settings
            host = "presto-gateway-production.presto-gateway-production"
            port = 6375
            catalog = "silver"
            
            # Create connection
            conn = presto.connect(
                host=host,
                port=port,
                catalog=catalog,
                requests_kwargs={"timeout": 300}
            )
            
            # Execute query
            sql = "select * from homes.listing__dim_active LIMIT 10"
            cursor = conn.cursor()
            cursor.execute(sql)
            
            if cursor.description:
                desc = cursor.description
                column_names = [col[0] for col in desc]
                data = [dict(zip(column_names, row)) for row in cursor.fetchall()]
                
                print(f"Query executed successfully. Retrieved {len(data)} rows:")
                for i, row in enumerate(data, 1):
                    print(f"Row {i}: {row}")
                    
                cursor.close()
                conn.close()
                
                return f"Successfully queried homes.listing__dim_active table, got {len(data)} rows"
            else:
                cursor.close()
                conn.close()
                print("Query executed but returned no data")
                return "Query executed but returned no data"
                
        except Exception as e2:
            print(f"Error executing Hive query: {e2}")
            return f"Failed to execute query: {str(e2)}"
            
    except Exception as e:
        print(f"Error executing Hive query: {e}")
        return f"Failed to execute query: {str(e)}"


@workflow
def hello_workflow():
    _ = print_runtime_info()
    _ = query_hive_data()
    s1o = step_one()
    _ = step_two(greeting=s1o.greeting, repeat=s1o.repeat)