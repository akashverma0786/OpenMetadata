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
Test Epic FHIR connector using mocked API responses
"""
from unittest import TestCase
from unittest.mock import Mock, patch

from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import DataType, TableType
from metadata.generated.schema.metadataIngestion.workflow import (
    OpenMetadataWorkflowConfig,
)
from metadata.generated.schema.type.basic import EntityName
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.epic.metadata import EpicSource
from metadata.ingestion.source.database.epic.models import ResourceCategory

# Mock FHIR API Responses
MOCK_CAPABILITY_STATEMENT = {
    "resourceType": "CapabilityStatement",
    "fhirVersion": "4.0.1",
    "format": ["json"],
    "rest": [
        {
            "mode": "server",
            "resource": [
                {
                    "type": "Patient",
                    "interaction": [{"code": "read"}, {"code": "search-type"}],
                },
                {
                    "type": "Encounter",
                    "interaction": [{"code": "read"}, {"code": "search-type"}],
                },
                {
                    "type": "Observation",
                    "interaction": [{"code": "read"}, {"code": "search-type"}],
                },
            ],
        }
    ],
}

MOCK_PATIENT_BUNDLE = {
    "resourceType": "Bundle",
    "type": "searchset",
    "total": 1523,
    "entry": [
        {
            "resource": {
                "resourceType": "Patient",
                "id": "example-1",
                "identifier": [
                    {"system": "http://hospital.org/mrn", "value": "123456"}
                ],
                "active": True,
                "name": [{"family": "Doe", "given": ["John", "Robert"]}],
                "gender": "male",
                "birthDate": "1990-01-01",
            }
        }
    ],
}

MOCK_ENCOUNTER_BUNDLE = {
    "resourceType": "Bundle",
    "type": "searchset",
    "total": 8734,
    "entry": [],
}

MOCK_OBSERVATION_BUNDLE = {
    "resourceType": "Bundle",
    "type": "searchset",
    "total": 45231,
    "entry": [],
}


mock_epic_config = {
    "source": {
        "type": "epic",
        "serviceName": "epic_test",
        "serviceConnection": {
            "config": {
                "type": "Epic",
                "fhirServerUrl": "https://epictest.org/fhir",
                "authType": "OAuth2",
                "clientId": "test_client",
                "clientSecret": "test_secret",
                "tokenUrl": "https://epictest.org/oauth/token",
                "scope": "system/*.read",
                "resourceTypes": ["Patient", "Encounter", "Observation"],
                "tagConfiguration": {
                    "enablePHITags": True,
                    "enableClinicalTags": False,
                },
            }
        },
        "sourceConfig": {
            "config": {
                "type": "DatabaseMetadata",
            }
        },
    },
    "sink": {"type": "metadata-rest", "config": {}},
    "workflowConfig": {
        "openMetadataServerConfig": {
            "hostPort": "http://localhost:8585/api",
            "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"},
        }
    },
}


class EpicUnitTest(TestCase):
    """
    Test Epic FHIR connector
    """

    @patch(
        "metadata.ingestion.source.database.epic.metadata.EpicSource.test_connection"
    )
    def __init__(self, methodName, test_connection):
        """
        Set up test Epic source
        """
        super().__init__(methodName)
        test_connection.return_value = False

        self.config = OpenMetadataWorkflowConfig.model_validate(mock_epic_config)

        # Mock metadata
        self.metadata = Mock(spec=OpenMetadata)

        # Create source instance
        self.epic_source = EpicSource.create(
            mock_epic_config["source"],
            self.metadata,
        )

        # Mock the Epic FHIR client
        self.epic_source.client = Mock()

        # Set up mock responses
        self.epic_source.client.get_capability_statement.return_value = (
            MOCK_CAPABILITY_STATEMENT
        )
        self.epic_source.client.get_resource_count = self._mock_resource_count

        # Set up context with mock entities
        mock_context = Mock()
        mock_context.database_service = "epic_test"
        mock_context.database = "epic"
        mock_context.database_schema = "Clinical"
        self.epic_source.context = Mock()
        self.epic_source.context.get = Mock(return_value=mock_context)
        self.epic_source.context.fqn_build = Mock(return_value=None)

        # Initialize status with a mock that has a filter method
        self.epic_source.status = Mock()
        self.epic_source.status.filter = Mock()

        # Mock database entity
        mock_db = Mock(spec=Database)
        mock_db.id = "mock_db_id"
        mock_db.name = EntityName("epic")
        mock_db.service = Mock()
        mock_db.service.id = "mock_service_id"

        # Mock schema entity
        mock_schema = Mock(spec=DatabaseSchema)
        mock_schema.id = "mock_schema_id"
        mock_schema.name = EntityName("Clinical")
        mock_schema.database = Mock()
        mock_schema.database.id = "mock_db_id"

        # Mock metadata operations
        self.epic_source.metadata.get_by_name = Mock()
        self.epic_source.metadata.get_by_name.side_effect = self._mock_get_by_name
        self.epic_source.metadata.create_or_update_classification = Mock()
        self.epic_source.metadata.create_or_update_tag = Mock()

    def _mock_resource_count(self, resource_type: str) -> int:
        """Mock resource count based on type"""
        counts = {
            "Patient": 1523,
            "Encounter": 8734,
            "Observation": 45231,
        }
        return counts.get(resource_type, 0)

    def _mock_get_by_name(self, entity, fqn):
        """Mock get_by_name for tags"""
        if "PHI" in fqn:
            # Return mock tag
            mock_tag = Mock()
            mock_tag.fullyQualifiedName = fqn
            return mock_tag
        return None

    def test_database_names(self):
        """
        Test yielding database names
        """
        databases = list(self.epic_source.get_database_names())
        assert databases == ["epic"]

    def test_yield_database(self):
        """
        Test database creation request
        """
        results = list(self.epic_source.yield_database("epic"))

        assert len(results) == 1
        assert isinstance(results[0], CreateDatabaseRequest)
        assert results[0].name == EntityName("epic")
        assert "Epic FHIR Server" in results[0].description.root

    def test_database_schema_names(self):
        """
        Test schema discovery (resource categories)
        """
        schemas = list(self.epic_source.get_database_schema_names())

        # Should have Clinical category for Patient, Encounter, Observation
        assert ResourceCategory.CLINICAL in schemas
        assert len(schemas) == 1  # All test resources are clinical

    def test_yield_schema(self):
        """
        Test schema creation request
        """
        self.epic_source.context.get().database = "epic"

        results = list(self.epic_source.yield_database_schema("Clinical"))

        assert len(results) == 1
        assert isinstance(results[0], CreateDatabaseSchemaRequest)
        assert results[0].name == EntityName("Clinical")
        assert "FHIR Resource Category: Clinical" in results[0].description.root

    def test_table_names(self):
        """
        Test table discovery (FHIR resources)
        """
        self.epic_source.context.get().database_schema = "Clinical"

        tables = list(self.epic_source.get_tables_name_and_type())

        # Should have all clinical resources
        table_names = [t[0] for t in tables]
        assert "Patient" in table_names
        assert "Encounter" in table_names
        assert "Observation" in table_names
        assert all(t[1] == TableType.Regular for t in tables)

    def test_yield_table_patient(self):
        """
        Test Patient table creation with columns
        """
        self.epic_source.context.get().database_schema = "Clinical"

        results = list(self.epic_source.yield_table(("Patient", TableType.Regular)))

        assert len(results) == 1
        table_request = results[0]
        assert isinstance(table_request, CreateTableRequest)
        assert table_request.name == EntityName("Patient")
        assert table_request.tableType == TableType.Regular

        # Check columns
        column_names = [col.name for col in table_request.columns]
        assert "id" in column_names
        assert "identifier" in column_names
        assert "name" in column_names
        assert "birthDate" in column_names
        assert "gender" in column_names

        # Check data types
        id_col = next(col for col in table_request.columns if col.name == "id")
        assert id_col.dataType == DataType.STRING
        assert not id_col.nullable  # Required field

        birth_col = next(
            col for col in table_request.columns if col.name == "birthDate"
        )
        assert birth_col.dataType == DataType.DATE
        assert birth_col.nullable  # Optional field

    def test_phi_tags_enabled(self):
        """
        Test PHI tag creation and application when enabled
        """
        self.epic_source.context.get().database_schema = "Clinical"

        # Mock PHI tag creation
        with patch.object(
            self.epic_source, "_create_phi_classification"
        ) as mock_create_phi:
            results = list(self.epic_source.yield_table(("Patient", TableType.Regular)))

            # PHI classification should be created
            mock_create_phi.assert_called_once()

            # Check that PHI tags are applied to appropriate columns
            table_request = results[0]

            # Name column should have PHI.Identifiable tag
            name_col = next(col for col in table_request.columns if col.name == "name")
            assert name_col.tags is not None
            assert any(tag.tagFQN.root == "PHI.Identifiable" for tag in name_col.tags)

            # BirthDate should have PHI.Demographic tag
            birth_col = next(
                col for col in table_request.columns if col.name == "birthDate"
            )
            assert birth_col.tags is not None
            assert any(tag.tagFQN.root == "PHI.Demographic" for tag in birth_col.tags)

    def test_phi_tags_disabled(self):
        """
        Test that PHI tags are not created when disabled
        """
        # Disable PHI tags
        self.epic_source.tag_configuration.enablePHITags = False
        self.epic_source.context.get().database_schema = "Clinical"

        results = list(self.epic_source.yield_table(("Patient", TableType.Regular)))

        table_request = results[0]

        # No columns should have tags
        for col in table_request.columns:
            assert col.tags is None

    def test_table_profiling(self):
        """
        Test table profile generation
        """
        self.epic_source.context.get().database_schema = "Clinical"

        profiles = list(self.epic_source.yield_table_profile("Patient"))

        assert len(profiles) == 1
        profile_request = profiles[0]

        assert profile_request.tableProfile.rowCount == 1523
        assert profile_request.tableProfile.columnCount > 0

    def test_resource_filtering(self):
        """
        Test resource type filtering
        """
        # Update config to only include Patient
        self.epic_source.service_connection.resourceTypes = ["Patient"]
        self.epic_source.context.get().database_schema = "Clinical"

        tables = list(self.epic_source.get_tables_name_and_type())

        # Should only have Patient
        assert len(tables) == 1
        assert tables[0][0] == "Patient"

    def test_different_resource_categories(self):
        """
        Test resources in different categories
        """
        # Add administrative resource
        self.epic_source.service_connection.resourceTypes = [
            "Patient",
            "Practitioner",
            "Coverage",
        ]

        schemas = list(self.epic_source.get_database_schema_names())

        # Should have multiple categories now
        assert ResourceCategory.CLINICAL in schemas
        assert ResourceCategory.ADMINISTRATIVE in schemas
        assert ResourceCategory.FINANCIAL in schemas

    def test_fhir_type_mapping(self):
        """
        Test FHIR to OpenMetadata data type mapping
        """
        from metadata.ingestion.source.database.epic.metadata import FHIR_TYPE_MAP

        # Test basic types
        assert FHIR_TYPE_MAP["string"] == DataType.STRING
        assert FHIR_TYPE_MAP["boolean"] == DataType.BOOLEAN
        assert FHIR_TYPE_MAP["integer"] == DataType.INT
        assert FHIR_TYPE_MAP["date"] == DataType.DATE
        assert FHIR_TYPE_MAP["dateTime"] == DataType.DATETIME

        # Test complex types
        assert FHIR_TYPE_MAP["CodeableConcept"] == DataType.JSON
        assert FHIR_TYPE_MAP["Reference"] == DataType.STRING
        assert FHIR_TYPE_MAP["Attachment"] == DataType.BLOB

    def test_connection_error_handling(self):
        """
        Test error handling for connection issues
        """
        # Mock connection failure
        self.epic_source.client.get_capability_statement.side_effect = Exception(
            "Connection refused"
        )

        # Should handle gracefully
        databases = list(self.epic_source.get_database_names())
        assert databases == ["epic"]  # Still returns database name

    def test_custom_database_name(self):
        """
        Test custom database name configuration
        """
        self.epic_source.service_connection.databaseName = "my_epic_system"

        databases = list(self.epic_source.get_database_names())
        assert databases == ["my_epic_system"]
