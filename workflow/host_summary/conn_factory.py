import logging
import threading
from airbnb_identity import context as aic

logger = logging.getLogger(__name__)

class ConnectionFactory:
    _provider = None
    _initialized = False
    _lock = threading.Lock()
    
    @classmethod
    def _ensure_initialized(cls):
        if cls._initialized:
            return
            
        with cls._lock:
            if cls._initialized:
                return
                
            try:
                context = aic.current_context()
                if context.is_interactive:
                    import sys
                    import os
                    # Add the parent directory to Python path to access obsolete module
                    parent_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                    if parent_dir not in sys.path:
                        sys.path.insert(0, parent_dir)
                    
                    from obsolete.conn_interactive import ConnInteractive
                    cls._provider = ConnInteractive()
                    logger.info("ConnectionFactory initialized with ConnInteractive")
                else:
                    from .conn_service import ConnService
                    cls._provider = ConnService()
                    logger.info("ConnectionFactory initialized with ConnService")
                    
                cls._initialized = True
                
            except Exception as e:
                logger.error("Failed to initialize ConnectionFactory: %s", e)
                raise RuntimeError(f"ConnectionFactory initialization failed: {e}") from e
    
    @classmethod
    def create_trino_hive_cursor(cls):
        cls._ensure_initialized()
        return cls._provider.create_trino_hive_cursor()

    @classmethod
    def create_trino_hive_client(cls):
        cls._ensure_initialized()
        return cls._provider.create_trino_hive_client()

    @classmethod
    def create_openai_client(cls):
        cls._ensure_initialized()
        return cls._provider.create_openai_client()
    
    @classmethod
    def reset(cls):
        with cls._lock:
            cls._provider = None
            cls._initialized = False
            logger.info("ConnectionFactory reset")


    def execute_hive_query(sql):
        try:
            cursor = ConnectionFactory.create_trino_hive_cursor()
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
            if cursor:
                cursor.close()