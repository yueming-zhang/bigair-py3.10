from abc import ABC, abstractmethod
import logging

# Shared constants extracted from existing files
DEFAULT_QUERY = {"azure-resource-bucket": "prototype", "region": "global"}
FACADE_ROUTE = "/api/v2/proxy/azure/oai"
OPENAI_API_VERSION = "2024-10-21"
OPENAI_API_TYPE = "azure"

# Presto/Trino constants
PRESTO_HOST = "presto-gateway-production.presto-gateway-production"
PRESTO_PORT = 6375
PRESTO_CATALOG = "silver"
CONNECTION_TIMEOUT = 300


class ConnBase(ABC):
    """Abstract base class for connection providers."""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    

    @abstractmethod
    def create_trino_hive_client(self):
        pass

    @abstractmethod
    def create_trino_hive_cursor(self):
        pass
    
    @abstractmethod
    def create_openai_client(self):
        pass
