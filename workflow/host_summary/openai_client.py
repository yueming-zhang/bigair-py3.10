import logging
import os

from airbnb_identity import Credential, GoogleIapCredential
from airbnb_identity import context as aic
from openai import AzureOpenAI

log = logging.getLogger(__name__)


DEFAULT_QUERY = {"azure-resource-bucket": "prototype", "region": "global"}
_FACADE_ROUTE = "/api/v2/proxy/azure/oai"


def create_openai_client(
) -> AzureOpenAI:
    import openai

    context = aic.current_context()
    log.info("Setting up OpenAI credentials for %s and openai==%s", context, openai.__version__)

    openai.api_type = "azure"
    openai.api_version = "2024-10-21"

    if context.is_interactive:
        credential: Credential = GoogleIapCredential()
        log.info("Using IAP identity to authenticate against LFM Facade")
        endpoint = f"https://llm-fusion-hub.a.musta.ch{_FACADE_ROUTE}"
        auth = credential.authenticate(endpoint)
        jwt = auth.headers["Authorization"].split()[-1]
        os.environ["AZURE_OPENAI_ENDPOINT"] = endpoint
        os.environ["AZURE_OPENAI_AD_TOKEN"] = jwt

    else:
        log.info("Using AirMesh service-to-service for the LFM Facade")
        # https://git.musta.ch/airbnb/llm-fusion-hub/blob/master/_infra/mesh.yml
        endpoint = f"http://llm-fusion-hub-production.llm-fusion-hub-production:11000{_FACADE_ROUTE}"
        # staging or airdev endpoint
        # endpoint = (
        #     f"http://llm-fusion-hub-staging.llm-fusion-hub-staging:11000{_FACADE_ROUTE}"
        # )

        os.environ["AZURE_OPENAI_ENDPOINT"] = endpoint
        os.environ["AZURE_OPENAI_API_KEY"] = "any"

    os.environ["OPENAI_API_VERSION"] = openai.api_version

    return openai.AzureOpenAI(default_query=DEFAULT_QUERY)
