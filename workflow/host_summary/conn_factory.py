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
    def create_openai_client(cls):
        cls._ensure_initialized()
        return cls._provider.create_openai_client()
    
    @classmethod
    def reset(cls):
        with cls._lock:
            cls._provider = None
            cls._initialized = False
            logger.info("ConnectionFactory reset")
