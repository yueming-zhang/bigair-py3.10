from bigair import step, workflow

@step(conda_env="devel/ming.yuezhang/host_summary:0.2")      
def initialize_hive() -> str:
    try:
        from workflow.host_summary.hive_init import HiveInitializer

        hive_initializer = HiveInitializer()
        result = hive_initializer.create_all_hive_views()

        if result:
            return "Hive views created successfully"
        else:
            return "Failed to create Hive views"
    except Exception as e:
        return f"Failed to create Hive views: {str(e)}"


@step(conda_env="devel/ming.yuezhang/host_summary:0.2")      
def process_data(result:str) -> str:
    """Run host QA analysis to get deactivation insights."""
    try:
        from workflow.host_summary.hive_qa import get_host_summary_qa
        qa = get_host_summary_qa()

        qa.register_hive_table('itx.dim_salesforce_account_update')
        qa.register_hive_table('host_quality.listing__dim_quality_scores_v3')
        qa.register_hive_table('host_growth.listing__fct_deactivation_types_and_reasons')

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
    result = initialize_hive()
    _ = process_data(result = result)