"""
Functionality related to [Airbnb's OpenAI proxy service](https://air.bb/openaiproxy/).
ref from:
https://git.musta.ch/airbnb/onebrain-projects/blob/master/utilities/hermod/hermod/openai.py
"""

import logging
import os
from typing import Optional

from airbnb_identity import Credential, GoogleIapCredential
from airbnb_identity import context as aic
from openai import AzureOpenAI

log = logging.getLogger(__name__)


DEFAULT_QUERY = {"azure-resource-bucket": "prototype", "region": "global"}
"""
Default query parameters expected by the LFM Facade's proxy endpoint.
"""

_FACADE_ROUTE = "/api/v2/proxy/azure/oai"


def openai_setup(
    credential: Credential = GoogleIapCredential(),
    *,
    # https://learn.microsoft.com/en-us/azure/ai-services/openai/api-version-deprecation
    api_version: str = "2024-10-21",
    context: Optional[aic.Contexts] = None,
) -> AzureOpenAI:
    """
    Call this method once to set up authentication and the appropriate endpoint
    for calling [Airbnb's LLM proxy service](https://air.bb/openai/).

    Needs the [OpenAI Python SDK](https://pypi.org/project/openai/) installed.

    ```python
    client = openai_setup()
    client.chat.completions.create(...)
    ```

    If using this method with [LangChain](https://python.langchain.com/), you
    need to run it BEFORE even importing from `langchain`.

    Returns working `AzureOpenAI` object with `default_query` already set.
    """
    import openai

    context = context or aic.current_context()
    log.info(
        "Setting up OpenAI credentials for %s and openai==%s",
        context,
        openai.__version__,
    )
    openai.api_type = "azure"
    openai.api_version = api_version

    if context.is_interactive:
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
