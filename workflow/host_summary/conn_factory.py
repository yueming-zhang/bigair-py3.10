import logging
import threading
from airbnb_identity import context as aic

logger = logging.getLogger(__name__)

class ConnectionFactory:
    """Singleton factory for creating database and API connections.
    
    Automatically detects environment (interactive vs service) and provides
    appropriate connection implementations. Thread-safe singleton implementation.
    """
    
    _provider = None
    _initialized = False
    _lock = threading.Lock()
    
    @classmethod
    def _ensure_initialized(cls):
        """Ensure the factory is initialized with the appropriate provider.
        
        Thread-safe initialization that detects the environment and creates
        the appropriate connection provider instance.
        """
        if cls._initialized:
            return
            
        with cls._lock:
            # Double-check locking pattern
            if cls._initialized:
                return
                
            try:
                context = aic.current_context()
                logger.info("Initializing ConnectionFactory for context: %s", context)
                
                if context.is_interactive:
                    from .conn_interactive import ConnInteractive
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
        """Create and return a Trino/Hive cursor for database operations.
        
        Returns:
            Database cursor object for executing queries
            
        Raises:
            RuntimeError: If factory initialization fails
            Exception: If cursor creation fails
        """
        cls._ensure_initialized()
        return cls._provider.create_trino_hive_cursor()
    
    @classmethod
    def create_openai_client(cls):
        """Create and return an Azure OpenAI client.
        
        Returns:
            AzureOpenAI client instance
            
        Raises:
            RuntimeError: If factory initialization fails
            Exception: If client creation fails
        """
        cls._ensure_initialized()
        return cls._provider.create_openai_client()
    
    @classmethod
    def reset(cls):
        """Reset the factory state. Primarily for testing purposes."""
        with cls._lock:
            cls._provider = None
            cls._initialized = False
            logger.info("ConnectionFactory reset")
