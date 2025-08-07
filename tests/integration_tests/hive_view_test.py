import pytest
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))
from workflow.host_summary.hive_init import HiveInitializer, HIVE_VIEW_QUERIES

# Configure logging for test visibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@pytest.fixture
def hive_initializer():
    """Fixture to provide a HiveInitializer instance for tests."""
    logger.info("Setting up HiveInitializer for testing")
    return HiveInitializer()


def test_create_all_hive_views(hive_initializer):
    result = hive_initializer.create_all_hive_views()
    assert result is True, "create_all_hive_views() should return True on success"

# @pytest.mark.parametrize("query_index", range(len(HIVE_VIEW_QUERIES)))
# def test_create_individual_hive_view(hive_initializer, query_index):
#     logger.info(f"Creating Hive view with index {query_index}")
    
#     result = hive_initializer.create_hive_view(query_index)
    
#     assert result is True, f"create_hive_view({query_index}) should return True on success"


def test_query_list_not_empty():
    assert len(HIVE_VIEW_QUERIES) > 0, "HIVE_VIEW_QUERIES should not be empty"
    logger.info(f"Confirmed HIVE_VIEW_QUERIES contains {len(HIVE_VIEW_QUERIES)} queries")


if __name__ == '__main__':
    logger.info("Starting Hive view integration tests with pytest")
    pytest.main([__file__, "-v"])
