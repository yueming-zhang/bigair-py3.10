import logging
from .conn_factory import ConnectionFactory

logger = logging.getLogger(__name__)

# Global dictionary of Hive view queries - view name as key, SQL as value
HIVE_VIEW_QUERIES = {
    "crm_growth_eng.listing__dim_active_curr__v": """
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
}


class HiveInitializer:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def create_hive_view(self, view_name):
        try:
            if view_name not in HIVE_VIEW_QUERIES:
                available_views = list(HIVE_VIEW_QUERIES.keys())
                raise KeyError(f"View '{view_name}' not found. Available views: {available_views}")
            
            query = HIVE_VIEW_QUERIES[view_name].strip()
            result = ConnectionFactory.execute_hive_query(query)

            verification_query = f"SELECT * FROM {view_name} LIMIT 1"
            verification_result = ConnectionFactory.execute_hive_query(verification_query)
            
            if verification_result and len(verification_result) > 0:
                self.logger.info(f"View '{view_name}' verification successful - contains data")
                return True
            else:
                self.logger.warning(f"View '{view_name}' verification failed - no data returned")
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to create or verify Hive view '{view_name}': {e}")
            raise
    
    def create_all_hive_views(self):
        try:
            view_names = list(HIVE_VIEW_QUERIES.keys())
            self.logger.info(f"Creating {len(view_names)} Hive views: {view_names}")
            
            success_count = 0
            for view_name in view_names:
                try:
                    if self.create_hive_view(view_name):
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"Failed to create view '{view_name}': {e}")
                    # Continue with other views even if one fails
                    continue
            
            if success_count == len(view_names):
                self.logger.info(f"All {len(view_names)} Hive views created and verified successfully")
                return True
            else:
                self.logger.warning(f"Only {success_count}/{len(view_names)} views created successfully")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to create all Hive views: {e}")
            raise
    
    def get_available_views(self):
        """
        Get list of available view names.
        
        Returns:
            list: List of available view names
        """
        return list(HIVE_VIEW_QUERIES.keys())