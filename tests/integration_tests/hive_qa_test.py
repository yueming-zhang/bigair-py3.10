import pytest
import logging
import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))
from workflow.host_summary.hive_qa import get_host_summary_qa

# Configure logging for test visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def test_host_qa_deactivation_analysis():
    """
    Integration test for host QA deactivation analysis.
    
    This test will:
    1. Initialize the host QA system
    2. Register required Hive tables
    3. Ask about deactivation types since 2025-07-01
    4. Verify that the result is not empty
    """
    logger.info("Starting host QA deactivation analysis integration test")
    
    try:
        # Import and initialize the host QA system
        from workflow.host_summary.hive_qa import get_host_summary_qa
        qa = get_host_summary_qa()
        logger.info("Host QA system initialized successfully")

        # Register required Hive tables
        tables_to_register = [
            'itx.dim_salesforce_account_update',
            'host_quality.listing__dim_quality_scores_v3',
            'host_growth.listing__fct_deactivation_types_and_reasons'
        ]
        
        for table in tables_to_register:
            logger.info(f"Registering Hive table: {table}")
            qa.register_hive_table(table)
        
        logger.info("All Hive tables registered successfully")

        # Define host IDs for testing
        host_ids = [217570714, 199055975, 263502162, 122382567, 242933544, 145244100, 218928815,
                   1684752, 1982737, 5266238, 5710846, 6940936, 11256892, 1236025]
        
        logger.info(f"Testing with {len(host_ids)} host IDs")
        
        # Ask the question about deactivation types
        question = "what are the top 10 common deactivation types since 2025-07-01?"
        logger.info(f"Asking question: {question}")
        
        result = qa.ask(question)
        
        # Log the result for debugging
        logger.info(f"QA result: {result}")
        
        # Verify that the result is not empty
        assert result is not None, "QA result should not be None"
        assert result != "", "QA result should not be empty string"
        assert len(str(result).strip()) > 0, "QA result should contain meaningful content"
        
        logger.info("Host QA deactivation analysis test completed successfully")
        
    except Exception as e:
        logger.error(f"Host QA integration test failed: {e}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        raise


if __name__ == '__main__':
    # Run the integration tests with pytest
    logger.info("Starting Host QA integration tests with pytest")
    pytest.main([__file__, "-v"])