from bigair import step, workflow

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


@step(conda_env="devel/ming.yuezhang/host_summary:0.2")      
def run_host_qa() -> str:
    """Run host QA analysis to get deactivation insights."""
    try:
        # print out folder structure recursively for debugging
        # from pathlib import Path
        # print("-----Current directory structure:")
        # for path in Path('.').rglob('*'):
        #     print(path)
        # print("-----Current directory structure end")
        import os

        # Override cache locations to use /tmp
        os.environ["XDG_CACHE_HOME"] = "/tmp/.cache"
        os.environ["TRANSFORMERS_CACHE"] = "/tmp/.cache/transformers"
        os.environ["HF_HOME"] = "/tmp/.cache/huggingface"
        os.environ["TORCH_HOME"] = "/tmp/.cache/torch"
        os.environ["ONNX_HOME"] = "/tmp/.cache/onnx"

        from workflow.host_summary.host_qa import get_host_summary_qa
        qa = get_host_summary_qa()

        qa.register_hive_table('itx.dim_salesforce_account_update')
        qa.register_hive_table('host_quality.listing__dim_quality_scores_v3')
        qa.register_hive_table('host_growth.listing__fct_deactivation_types_and_reasons')

        host_ids = [217570714,199055975,263502162,122382567,242933544,145244100, 218928815,
                1684752, 1982737, 5266238, 5710846, 6940936, 11256892, 1236025]
        
        result = qa.ask("what are the top 10 common deactivation types since 2025-07-01?")
        print(f"---{result}")
        
        return f"Host QA analysis completed successfully: {result}"
        
    except Exception as e:
        print(f"Error running host QA analysis: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return f"Failed to run host QA analysis: {str(e)}"



# @workflow(attachments=["../vanna/**/*.py", "../host_summary/**/*.py"])
# @workflow(attachments=["requirements.txt"])
@workflow(attachments=["./vanna/**/*.py", "./host_summary/**/*.py"])
def host_data_extraction_workflow():
    _ = query_hive_data()
    _ = run_host_qa()