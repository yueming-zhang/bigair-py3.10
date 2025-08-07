import logging
import os
from pyhive import presto
from openai import AzureOpenAI

from .conn_base import (
    ConnBase, DEFAULT_QUERY, FACADE_ROUTE, OPENAI_API_VERSION, OPENAI_API_TYPE,
    PRESTO_HOST, PRESTO_PORT, PRESTO_CATALOG, CONNECTION_TIMEOUT
)


class ConnService(ConnBase):
    """Connection provider for service environments (production/staging)."""
    
    def create_trino_hive_cursor(self):
        """Create Trino/Hive cursor using service-to-service authentication."""
        self.logger.info("Using AirMesh service-to-service for Trino Hive Client")
        
        try:
            conn = presto.connect(
                host=PRESTO_HOST,
                port=PRESTO_PORT,
                catalog=PRESTO_CATALOG,
                requests_kwargs={"timeout": CONNECTION_TIMEOUT},
            )
            ret_cursor = conn.cursor()
            return ret_cursor
        except Exception as e:
            self.logger.error("Failed to create service Trino cursor: %s", e)
            raise
    
    def create_openai_client(self) -> AzureOpenAI:
        """Create Azure OpenAI client using service-to-service authentication."""
        import openai
        
        self.logger.info("Setting up OpenAI credentials for service environment and openai==%s", openai.__version__)
        
        openai.api_type = OPENAI_API_TYPE
        openai.api_version = OPENAI_API_VERSION
        
        try:
            self.logger.info("Using AirMesh service-to-service for the LFM Facade")
            # https://git.musta.ch/airbnb/llm-fusion-hub/blob/master/_infra/mesh.yml
            endpoint = f"http://llm-fusion-hub-production.llm-fusion-hub-production:11000{FACADE_ROUTE}"
            # staging or airdev endpoint
            # endpoint = (
            #     f"http://llm-fusion-hub-staging.llm-fusion-hub-staging:11000{_FACADE_ROUTE}"
            # )
            
            os.environ["AZURE_OPENAI_ENDPOINT"] = endpoint
            os.environ["AZURE_OPENAI_API_KEY"] = "any"
            os.environ["OPENAI_API_VERSION"] = openai.api_version
            
            return openai.AzureOpenAI(default_query=DEFAULT_QUERY)
        except Exception as e:
            self.logger.error("Failed to create service OpenAI client: %s", e)
            raise

    def create_trino_hive_client(self):
        """Create Trino/Hive client using service-to-service authentication."""
        self.logger.info("Using AirMesh service-to-service for Trino Hive Client")
        
        try:
            conn = presto.connect(
                host=PRESTO_HOST,
                port=PRESTO_PORT,
                catalog=PRESTO_CATALOG,
                requests_kwargs={"timeout": CONNECTION_TIMEOUT},
            )
            return conn
        except Exception as e:
            self.logger.error("Failed to create service Trino client: %s", e)
            raise
