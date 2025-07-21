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
Test Epic FHIR connection and client
"""
from unittest import TestCase
from unittest.mock import Mock, patch

import pytest
import requests

from metadata.generated.schema.entity.services.connections.database.epicConnection import (
    AuthType,
    EpicConnection,
)
from metadata.ingestion.source.database.epic.connection import (
    EpicFHIRClient,
    get_connection,
    test_connection,
)


class TestEpicFHIRClient(TestCase):
    """
    Test Epic FHIR client functionality
    """

    def setUp(self):
        """Set up test fixtures"""
        self.config = EpicConnection(
            fhirServerUrl="https://epictest.org/fhir",
            authType=AuthType.OAuth2,
            clientId="test_client",
            clientSecret="test_secret",
            tokenUrl="https://epictest.org/oauth/token",
            scope="system/*.read",
        )

    @patch("requests.post")
    def test_oauth2_authentication(self, mock_post):
        """Test OAuth2 token retrieval"""
        # Mock token response
        mock_response = Mock()
        mock_response.json.return_value = {"access_token": "test_token_123"}
        mock_response.raise_for_status = Mock()
        mock_post.return_value = mock_response

        # Create client
        client = EpicFHIRClient(self.config)

        # Verify token request
        mock_post.assert_called_once_with(
            "https://epictest.org/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "test_client",
                "client_secret": "test_secret",
                "scope": "system/*.read",
            },
        )

        # Verify session headers
        assert client.session.headers["Authorization"] == "Bearer test_token_123"
        assert client.session.headers["Accept"] == "application/fhir+json"

    def test_basic_authentication(self):
        """Test Basic Auth setup"""
        config = EpicConnection(
            fhirServerUrl="https://epictest.org/fhir",
            authType=AuthType.BasicAuth,
            username="testuser",
            password="testpass",
        )

        client = EpicFHIRClient(config)

        # Verify basic auth is set
        assert client.session.auth is not None
        assert client.session.auth.username == "testuser"
        assert client.session.auth.password == "testpass"

    @patch("requests.Session.get")
    def test_get_capability_statement(self, mock_get):
        """Test fetching capability statement"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "resourceType": "CapabilityStatement",
            "fhirVersion": "4.0.1",
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()  # Use mock session to avoid OAuth call
        client.session.get = mock_get

        result = client.get_capability_statement()

        mock_get.assert_called_once_with("https://epictest.org/fhir/metadata")
        assert result["fhirVersion"] == "4.0.1"

    @patch("requests.Session.get")
    def test_get_resource_count(self, mock_get):
        """Test getting resource count"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {"total": 1234}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        count = client.get_resource_count("Patient")

        mock_get.assert_called_once_with(
            "https://epictest.org/fhir/Patient", params={"_summary": "count"}
        )
        assert count == 1234

    @patch("requests.Session.get")
    def test_search_resources(self, mock_get):
        """Test resource search"""
        # Mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            "resourceType": "Bundle",
            "type": "searchset",
            "total": 5,
            "entry": [{"resource": {"resourceType": "Patient", "id": "123"}}],
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        result = client.search_resources("Patient", {"name": "Smith"})

        mock_get.assert_called_once_with(
            "https://epictest.org/fhir/Patient", params={"name": "Smith"}
        )
        assert result["total"] == 5

    @patch("requests.Session.get")
    def test_get_resource_definition(self, mock_get):
        """Test fetching resource structure definition"""
        # Mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "resourceType": "StructureDefinition",
            "type": "Patient",
        }
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        result = client.get_resource_definition("Patient")

        mock_get.assert_called_once_with(
            "https://epictest.org/fhir/StructureDefinition/Patient"
        )
        assert result["type"] == "Patient"

    def test_get_connection(self):
        """Test connection factory"""
        client = get_connection(self.config)
        assert isinstance(client, EpicFHIRClient)
        assert client.base_url == "https://epictest.org/fhir"

    @patch("requests.Session.get")
    def test_connection_test_success(self, mock_get):
        """Test successful connection test"""
        # Mock successful capability statement
        mock_response = Mock()
        mock_response.json.return_value = {
            "resourceType": "CapabilityStatement",
            "fhirVersion": "4.0.1",
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        metadata = Mock()

        # Should not raise exception
        test_connection(metadata, client, self.config)

    @patch("requests.Session.get")
    def test_connection_test_failure(self, mock_get):
        """Test failed connection test"""
        # Mock connection failure
        mock_get.side_effect = requests.ConnectionError("Connection refused")

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        metadata = Mock()

        # Should raise exception
        with pytest.raises(Exception):
            test_connection(metadata, client, self.config)

    def test_url_trailing_slash_handling(self):
        """Test that trailing slashes are handled correctly"""
        config_with_slash = EpicConnection(
            type="Epic",
            fhirServerUrl="https://epictest.org/fhir/",
            authType="BasicAuth",
            username="test",
            password="test",
        )

        client = EpicFHIRClient(config_with_slash)
        assert client.base_url == "https://epictest.org/fhir"  # No trailing slash

    @patch("requests.post")
    def test_oauth_error_handling(self, mock_post):
        """Test OAuth error handling"""
        # Mock failed token response
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError(
            "401 Unauthorized"
        )
        mock_post.return_value = mock_response

        # Should raise exception when creating client
        with pytest.raises(requests.HTTPError):
            EpicFHIRClient(self.config)

    @patch("requests.Session.get")
    def test_pagination_parameters(self, mock_get):
        """Test pagination support in search"""
        mock_response = Mock()
        mock_response.json.return_value = {"resourceType": "Bundle", "total": 100}
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        client = EpicFHIRClient(self.config)
        client.session = Mock()
        client.session.get = mock_get

        # Search with pagination
        client.search_resources(
            "Patient", {"_count": 50, "_offset": 100, "name": "Smith"}
        )

        mock_get.assert_called_once_with(
            "https://epictest.org/fhir/Patient",
            params={"_count": 50, "_offset": 100, "name": "Smith"},
        )
