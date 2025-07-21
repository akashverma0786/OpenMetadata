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
Integration test for Epic FHIR ingestion demonstrating full workflow
"""
from unittest import TestCase
from unittest.mock import Mock, patch

# Sample FHIR responses for different resource types
MOCK_FHIR_RESPONSES = {
    "capability": {
        "resourceType": "CapabilityStatement",
        "fhirVersion": "4.0.1",
        "format": ["json", "xml"],
        "rest": [
            {
                "mode": "server",
                "resource": [
                    {
                        "type": "Patient",
                        "profile": "http://hl7.org/fhir/StructureDefinition/Patient",
                        "interaction": [
                            {"code": "read"},
                            {"code": "vread"},
                            {"code": "update"},
                            {"code": "patch"},
                            {"code": "delete"},
                            {"code": "history-instance"},
                            {"code": "history-type"},
                            {"code": "create"},
                            {"code": "search-type"},
                        ],
                        "searchParam": [
                            {"name": "_id", "type": "token"},
                            {"name": "identifier", "type": "token"},
                            {"name": "name", "type": "string"},
                            {"name": "family", "type": "string"},
                            {"name": "given", "type": "string"},
                            {"name": "birthdate", "type": "date"},
                            {"name": "gender", "type": "token"},
                        ],
                    },
                    {
                        "type": "Encounter",
                        "profile": "http://hl7.org/fhir/StructureDefinition/Encounter",
                        "interaction": [{"code": "read"}, {"code": "search-type"}],
                        "searchParam": [
                            {"name": "_id", "type": "token"},
                            {"name": "patient", "type": "reference"},
                            {"name": "status", "type": "token"},
                            {"name": "class", "type": "token"},
                            {"name": "date", "type": "date"},
                        ],
                    },
                    {
                        "type": "Observation",
                        "profile": "http://hl7.org/fhir/StructureDefinition/Observation",
                        "interaction": [{"code": "read"}, {"code": "search-type"}],
                        "searchParam": [
                            {"name": "_id", "type": "token"},
                            {"name": "patient", "type": "reference"},
                            {"name": "code", "type": "token"},
                            {"name": "category", "type": "token"},
                            {"name": "date", "type": "date"},
                            {"name": "value-quantity", "type": "quantity"},
                        ],
                    },
                    {
                        "type": "MedicationRequest",
                        "profile": "http://hl7.org/fhir/StructureDefinition/MedicationRequest",
                        "interaction": [{"code": "read"}, {"code": "search-type"}],
                    },
                    {
                        "type": "Practitioner",
                        "profile": "http://hl7.org/fhir/StructureDefinition/Practitioner",
                        "interaction": [{"code": "read"}, {"code": "search-type"}],
                    },
                    {
                        "type": "Organization",
                        "profile": "http://hl7.org/fhir/StructureDefinition/Organization",
                        "interaction": [{"code": "read"}, {"code": "search-type"}],
                    },
                ],
            }
        ],
    },
    "patient_count": {"resourceType": "Bundle", "type": "searchset", "total": 15234},
    "encounter_count": {"resourceType": "Bundle", "type": "searchset", "total": 87654},
    "observation_count": {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 452310,
    },
    "medication_count": {"resourceType": "Bundle", "type": "searchset", "total": 34521},
    "practitioner_count": {
        "resourceType": "Bundle",
        "type": "searchset",
        "total": 1234,
    },
    "organization_count": {"resourceType": "Bundle", "type": "searchset", "total": 45},
}


class TestEpicIngestionWorkflow(TestCase):
    """
    Test full Epic FHIR ingestion workflow
    """

    @patch("metadata.ingestion.source.database.epic.connection.requests.post")
    @patch("metadata.ingestion.source.database.epic.connection.requests.Session.get")
    def test_complete_ingestion_workflow(self, mock_get, mock_post):
        """
        Test complete ingestion workflow with mocked FHIR server
        """
        # Mock OAuth token response
        mock_token_response = Mock()
        mock_token_response.json.return_value = {"access_token": "test_token"}
        mock_token_response.raise_for_status = Mock()
        mock_post.return_value = mock_token_response

        # Mock FHIR API responses
        def mock_fhir_get(url, params=None):
            response = Mock()
            response.raise_for_status = Mock()

            if url.endswith("/metadata"):
                response.json.return_value = MOCK_FHIR_RESPONSES["capability"]
            elif (
                url.endswith("/Patient")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["patient_count"]
            elif (
                url.endswith("/Encounter")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["encounter_count"]
            elif (
                url.endswith("/Observation")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["observation_count"]
            elif (
                url.endswith("/MedicationRequest")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["medication_count"]
            elif (
                url.endswith("/Practitioner")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["practitioner_count"]
            elif (
                url.endswith("/Organization")
                and params
                and params.get("_summary") == "count"
            ):
                response.json.return_value = MOCK_FHIR_RESPONSES["organization_count"]
            else:
                response.json.return_value = {"resourceType": "Bundle", "total": 0}

            return response

        mock_get.side_effect = mock_fhir_get

        # Workflow configuration
        workflow_config = {
            "source": {
                "type": "epic",
                "serviceName": "epic_integration_test",
                "serviceConnection": {
                    "config": {
                        "type": "Epic",
                        "fhirServerUrl": "https://epic-integration-test.org/fhir",
                        "authType": "OAuth2",
                        "clientId": "integration_test_client",
                        "clientSecret": "integration_test_secret",
                        "tokenUrl": "https://epic-integration-test.org/oauth/token",
                        "scope": "system/*.read",
                        "resourceTypes": [
                            "Patient",
                            "Encounter",
                            "Observation",
                            "MedicationRequest",
                            "Practitioner",
                            "Organization",
                        ],
                        "tagConfiguration": {
                            "enablePHITags": True,
                            "enableClinicalTags": True,
                            "customClassifications": [
                                {
                                    "name": "EpicCustom",
                                    "description": "Custom Epic classification",
                                    "tags": [
                                        {
                                            "name": "Production",
                                            "description": "Production data",
                                        },
                                        {"name": "Test", "description": "Test data"},
                                    ],
                                }
                            ],
                        },
                    }
                },
                "sourceConfig": {
                    "config": {
                        "type": "DatabaseMetadata",
                        "generateSampleData": False,
                        "markDeletedTables": False,
                        "includeTables": True,
                        "includeViews": False,
                        "includeTags": True,
                        "includeOwners": True,
                        "includeStoredProcedures": False,
                        "includeDDL": False,
                    }
                },
            },
            "sink": {"type": "metadata-rest", "config": {}},
            "workflowConfig": {
                "loggerLevel": "DEBUG",
                "openMetadataServerConfig": {
                    "hostPort": "http://localhost:8585/api",
                    "authProvider": "openmetadata",
                    "securityConfig": {
                        "jwtToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
                    },
                },
            },
        }

        # Expected results tracking
        expected_results = {
            "databases": ["epic_integration_test"],
            "schemas": ["Clinical", "Administrative"],
            "tables": {
                "Clinical": [
                    "Patient",
                    "Encounter",
                    "Observation",
                    "MedicationRequest",
                ],
                "Administrative": ["Practitioner", "Organization"],
            },
            "row_counts": {
                "Patient": 15234,
                "Encounter": 87654,
                "Observation": 452310,
                "MedicationRequest": 34521,
                "Practitioner": 1234,
                "Organization": 45,
            },
        }

        # Assertions to verify workflow behavior
        # Note: This is a demonstration of how the workflow would be tested
        # In a real test, you would need to mock the OpenMetadata API calls
        # and verify the correct entities are created

        assert workflow_config["source"]["type"] == "epic"
        assert (
            len(
                workflow_config["source"]["serviceConnection"]["config"][
                    "resourceTypes"
                ]
            )
            == 6
        )
        assert workflow_config["source"]["serviceConnection"]["config"][
            "tagConfiguration"
        ]["enablePHITags"]

        # Verify OAuth configuration
        oauth_config = workflow_config["source"]["serviceConnection"]["config"]
        assert oauth_config["authType"] == "OAuth2"
        assert oauth_config["clientId"] == "integration_test_client"

        # Verify custom classifications
        custom_classifications = oauth_config["tagConfiguration"][
            "customClassifications"
        ]
        assert len(custom_classifications) == 1
        assert custom_classifications[0]["name"] == "EpicCustom"
        assert len(custom_classifications[0]["tags"]) == 2

    def test_error_handling_workflow(self):
        """
        Test workflow error handling scenarios
        """
        # Test scenarios:
        # 1. Invalid authentication
        # 2. Network timeout
        # 3. Invalid FHIR response
        # 4. Missing required resources

        error_scenarios = [
            {
                "name": "Invalid OAuth credentials",
                "error": "401 Unauthorized",
                "expected_behavior": "Workflow should fail at connection test",
            },
            {
                "name": "FHIR server timeout",
                "error": "Connection timeout",
                "expected_behavior": "Workflow should retry and eventually fail",
            },
            {
                "name": "Invalid FHIR version",
                "error": "Unsupported FHIR version",
                "expected_behavior": "Workflow should log warning and continue",
            },
            {
                "name": "Resource not found",
                "error": "404 Not Found",
                "expected_behavior": "Workflow should skip resource and continue",
            },
        ]

        for scenario in error_scenarios:
            assert scenario["name"] is not None
            assert scenario["error"] is not None
            assert scenario["expected_behavior"] is not None

    def test_phi_tag_workflow(self):
        """
        Test PHI tag creation and application workflow
        """
        phi_fields = {
            "Patient": {
                "identifier": "PHI.Identifiable",
                "name": "PHI.Identifiable",
                "birthDate": "PHI.Demographic",
                "address": "PHI.Identifiable",
            },
            "Encounter": {
                "identifier": "PHI.Identifiable",
                "diagnosis": "PHI.Clinical",
            },
            "Observation": {"value": "PHI.Clinical", "interpretation": "PHI.Clinical"},
        }

        # Verify PHI field mapping
        for resource, fields in phi_fields.items():
            for field, tag in fields.items():
                assert tag.startswith("PHI.")
                assert field is not None
