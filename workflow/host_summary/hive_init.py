import logging
from .conn_factory import ConnectionFactory

logger = logging.getLogger(__name__)

# Global list of Hive view queries
HIVE_VIEW_QUERIES = [
    """
    CREATE OR REPLACE VIEW crm_growth_eng.listing__dim_active_curr__v AS
    SELECT
      id_host,
      COUNT(*) AS total_listings,
      SUM(CASE WHEN dim_is_active = 1 THEN 1 ELSE 0 END) AS active_listings,
      SUM(CASE WHEN dim_is_active = 0 THEN 1 ELSE 0 END) AS inactive_listings,
      ROUND(100.0 * SUM(CASE WHEN dim_is_active = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS active_listing_percentage,
      ROUND(100.0 * SUM(CASE WHEN dim_is_active = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS inactive_listing_percentage
    FROM
      homes.listing__dim_active
    WHERE
      CAST(ds AS DATE) = date_add('day', -2, current_date)
    GROUP BY
      id_host
    """
]


class HiveInitializer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_hive_view(self, query_index=0):
        try:
            if query_index >= len(HIVE_VIEW_QUERIES):
                raise IndexError(f"Query index {query_index} is out of range. Available queries: {len(HIVE_VIEW_QUERIES)}")
            
            query = HIVE_VIEW_QUERIES[query_index].strip()
            result = ConnectionFactory.execute_hive_query(query)
            self.logger.info("Hive view created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create Hive view: {e}")
            raise
    
    def create_all_hive_views(self):
        try:
            for i in range(len(HIVE_VIEW_QUERIES)):
                self.create_hive_view(i)
                
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create all Hive views: {e}")
            raise