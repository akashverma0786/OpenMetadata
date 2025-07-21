#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Epic FHIR connection implementation
"""
from typing import Optional

import requests
from requests import Session
from requests.auth import HTTPBasicAuth

from metadata.generated.schema.entity.automations.workflow import (
    Workflow as AutomationWorkflow,
)
from metadata.generated.schema.entity.services.connections.database.epicConnection import (
    AuthType,
    EpicConnection,
)
from metadata.ingestion.connections.test_connections import test_connection_steps
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class EpicFHIRClient:
    """
    Epic FHIR API Client wrapper
    """

    def __init__(self, config: EpicConnection):
        self.config = config
        self.base_url = str(config.fhirServerUrl).rstrip("/")
        self.session = self._create_session()

    def _create_session(self) -> Session:
        """Create authenticated session based on auth type"""
        session = Session()

        if self.config.authType == AuthType.BasicAuth:
            session.auth = HTTPBasicAuth(self.config.username, self.config.password)
        elif self.config.authType in [AuthType.OAuth2, AuthType.SmartOnFhir]:
            # OAuth2 implementation
            token = self._get_oauth_token()
            session.headers.update({"Authorization": f"Bearer {token}"})

        # Set common headers
        session.headers.update(
            {"Accept": "application/fhir+json", "Content-Type": "application/fhir+json"}
        )

        return session

    def _get_oauth_token(self) -> str:
        """Get OAuth2 token"""
        token_data = {
            "grant_type": "client_credentials",
            "client_id": self.config.clientId,
            "client_secret": self.config.clientSecret,
            "scope": self.config.scope,
        }

        response = requests.post(self.config.tokenUrl, data=token_data)
        response.raise_for_status()
        return response.json()["access_token"]

    def get_capability_statement(self) -> dict:
        """Get FHIR server capability statement"""
        response = self.session.get(f"{self.base_url}/metadata")
        response.raise_for_status()
        return response.json()

    def get_resource_count(self, resource_type: str) -> int:
        """Get count of resources of a specific type"""
        response = self.session.get(
            f"{self.base_url}/{resource_type}", params={"_summary": "count"}
        )
        response.raise_for_status()
        return response.json().get("total", 0)

    def search_resources(
        self, resource_type: str, params: Optional[dict] = None
    ) -> dict:
        """Search for resources with parameters"""
        response = self.session.get(
            f"{self.base_url}/{resource_type}", params=params or {}
        )
        response.raise_for_status()
        return response.json()

    def get_resource_definition(self, resource_type: str) -> dict:
        """Get structure definition for a resource type"""
        response = self.session.get(
            f"{self.base_url}/StructureDefinition/{resource_type}"
        )
        if response.status_code == 200:
            return response.json()
        return {}


def get_connection(connection: EpicConnection) -> EpicFHIRClient:
    """
    Create connection to Epic FHIR server
    """
    return EpicFHIRClient(connection)


def test_connection(
    metadata: OpenMetadata,
    client: EpicFHIRClient,
    service_connection: EpicConnection,
    automation_workflow: Optional[AutomationWorkflow] = None,
) -> None:
    """
    Test connection to Epic FHIR server
    """

    def custom_test_connection():
        """Test the FHIR server connection"""
        try:
            capability = client.get_capability_statement()
            if not capability.get("fhirVersion"):
                raise Exception("Invalid capability statement")
        except Exception as exc:
            logger.error(f"Failed to connect to Epic FHIR server: {exc}")
            raise

    test_fn = {
        "CheckAccess": custom_test_connection,
    }

    test_connection_steps(
        metadata=metadata,
        test_fn=test_fn,
        service_type=service_connection.type.value,
        automation_workflow=automation_workflow,
    )
