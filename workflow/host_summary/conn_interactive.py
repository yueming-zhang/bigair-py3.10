import logging
import os
from airbnb_identity import Credential, GoogleIapCredential
from openai import AzureOpenAI

from .conn_base import ConnBase, DEFAULT_QUERY, FACADE_ROUTE, OPENAI_API_VERSION, OPENAI_API_TYPE


class ConnInteractive(ConnBase):
    """Connection provider for interactive environments (development/local)."""
    
    def create_trino_hive_cursor(self):
        """Create Trino/Hive cursor using interactive credentials."""
        self.logger.info("Using interactive credentials for Trino Hive client")
        
        try:
            from airbnb_trino_client import create_client
            ret_client = create_client()
            ret_cursor = ret_client.conn.cursor()
            return ret_cursor
        except Exception as e:
            self.logger.error("Failed to create interactive Trino cursor: %s", e)
            raise
    
    def create_openai_client(self) -> AzureOpenAI:
        """Create Azure OpenAI client using IAP authentication."""
        import openai
        
        self.logger.info("Setting up OpenAI credentials for interactive environment and openai==%s", openai.__version__)
        
        openai.api_type = OPENAI_API_TYPE
        openai.api_version = OPENAI_API_VERSION
        
        try:
            credential: Credential = GoogleIapCredential()
            self.logger.info("Using IAP identity to authenticate against LFM Facade")
            endpoint = f"https://llm-fusion-hub.a.musta.ch{FACADE_ROUTE}"
            auth = credential.authenticate(endpoint)
            jwt = auth.headers["Authorization"].split()[-1]
            os.environ["AZURE_OPENAI_ENDPOINT"] = endpoint
            os.environ["AZURE_OPENAI_AD_TOKEN"] = jwt
            os.environ["OPENAI_API_VERSION"] = openai.api_version
            
            return openai.AzureOpenAI(default_query=DEFAULT_QUERY)
        except Exception as e:
            self.logger.error("Failed to create interactive OpenAI client: %s", e)
            raise
