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
Epic FHIR source ingestion
"""
import traceback
from typing import Any, Iterable, List, Optional, Tuple

from metadata.generated.schema.api.classification.createClassification import (
    CreateClassificationRequest,
)
from metadata.generated.schema.api.classification.createTag import CreateTagRequest
from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
from metadata.generated.schema.api.data.createDatabaseSchema import (
    CreateDatabaseSchemaRequest,
)
from metadata.generated.schema.api.data.createStoredProcedure import (
    CreateStoredProcedureRequest,
)
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.api.data.createTableProfile import (
    CreateTableProfileRequest,
)
from metadata.generated.schema.entity.data.database import Database
from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
from metadata.generated.schema.entity.data.table import (
    Column,
    Constraint,
    DataType,
    Table,
    TableProfile,
    TableType,
)
from metadata.generated.schema.entity.services.connections.database.epicConnection import (
    EpicConnection,
)
from metadata.generated.schema.entity.services.ingestionPipelines.status import (
    StackTraceError,
)
from metadata.generated.schema.metadataIngestion.databaseServiceMetadataPipeline import (
    DatabaseServiceMetadataPipeline,
)
from metadata.generated.schema.metadataIngestion.workflow import (
    Source as WorkflowSource,
)
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
)
from metadata.generated.schema.type.tagLabel import TagLabel
from metadata.ingestion.api.models import Either
from metadata.ingestion.api.steps import InvalidSourceException
from metadata.ingestion.models.ometa_classification import OMetaTagAndClassification
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.connections import get_connection
from metadata.ingestion.source.database.database_service import DatabaseServiceSource
from metadata.ingestion.source.database.epic.connection import EpicFHIRClient
from metadata.ingestion.source.database.epic.models import (
    PHI_FIELD_MAPPING,
    RESOURCE_CATEGORY_MAPPING,
    ResourceCategory,
)
from metadata.utils import fqn
from metadata.utils.filters import filter_by_schema, filter_by_table
from metadata.utils.logger import ingestion_logger
from metadata.utils.tag_utils import get_tag_label

logger = ingestion_logger()

# FHIR data type to OpenMetadata data type mapping
FHIR_TYPE_MAP = {
    "string": DataType.STRING,
    "boolean": DataType.BOOLEAN,
    "integer": DataType.INT,
    "decimal": DataType.DECIMAL,
    "uri": DataType.STRING,
    "url": DataType.STRING,
    "canonical": DataType.STRING,
    "base64Binary": DataType.BLOB,
    "instant": DataType.TIMESTAMP,
    "date": DataType.DATE,
    "dateTime": DataType.DATETIME,
    "time": DataType.TIME,
    "code": DataType.STRING,
    "oid": DataType.STRING,
    "id": DataType.STRING,
    "markdown": DataType.TEXT,
    "unsignedInt": DataType.INT,
    "positiveInt": DataType.INT,
    "uuid": DataType.UUID,
    "Reference": DataType.STRING,
    "Coding": DataType.JSON,
    "CodeableConcept": DataType.JSON,
    "Quantity": DataType.JSON,
    "Period": DataType.JSON,
    "Range": DataType.JSON,
    "Attachment": DataType.BLOB,
    "Identifier": DataType.JSON,
    "HumanName": DataType.JSON,
    "Address": DataType.JSON,
    "ContactPoint": DataType.JSON,
}


class EpicSource(DatabaseServiceSource):
    """
    Implements the necessary methods to extract
    Database metadata from Epic FHIR Source
    """

    def __init__(self, config: WorkflowSource, metadata: OpenMetadata):
        super().__init__()
        self.config = config
        self.source_config: DatabaseServiceMetadataPipeline = (
            self.config.sourceConfig.config
        )
        self.metadata = metadata
        self.service_connection = self.config.serviceConnection.root.config
        self.client: EpicFHIRClient = get_connection(self.service_connection)
        self.database_name = self.service_connection.databaseName or "epic"
        self.tag_configuration = self.service_connection.tagConfiguration
        self.created_classifications = set()

    @classmethod
    def create(
        cls, config_dict, metadata: OpenMetadata, pipeline_name: Optional[str] = None
    ):
        config: WorkflowSource = WorkflowSource.model_validate(config_dict)
        connection: EpicConnection = config.serviceConnection.root.config
        if not isinstance(connection, EpicConnection):
            raise InvalidSourceException(
                f"Expected EpicConnection, but got {connection}"
            )
        return cls(config, metadata)

    def get_database_names(self) -> Iterable[str]:
        """
        Yield the database name (Epic organization)
        """
        try:
            yield self.database_name
        except Exception as exc:
            logger.error(f"Error getting database name: {exc}")
            logger.debug(traceback.format_exc())

    def yield_database(self, database_name: str) -> Iterable[CreateDatabaseRequest]:
        """
        Yield database request
        """
        yield CreateDatabaseRequest(
            name=EntityName(database_name),
            service=FullyQualifiedEntityName(self.context.get().database_service),
            description=Markdown(
                f"Epic FHIR Server - {self.service_connection.fhirServerUrl}"
            ),
        )

    def get_database_schema_names(self) -> Iterable[str]:
        """
        Yield schema names (resource categories)
        """
        try:
            # Get unique categories from configured resource types
            categories = set()
            for resource_type in self.service_connection.resourceTypes:
                category = RESOURCE_CATEGORY_MAPPING.get(
                    resource_type, ResourceCategory.CLINICAL
                )
                categories.add(category)

            for category in categories:
                if filter_by_schema(
                    self.source_config.schemaFilterPattern, schema_name=category
                ):
                    self.status.filter(
                        category,
                        "Schema (Resource Category) filtered out",
                    )
                    continue
                yield category

        except Exception as exc:
            logger.error(f"Error getting schema names: {exc}")
            logger.debug(traceback.format_exc())

    def yield_database_schema(
        self, schema_name: str
    ) -> Iterable[CreateDatabaseSchemaRequest]:
        """
        Yield schema request
        """
        yield CreateDatabaseSchemaRequest(
            name=EntityName(schema_name),
            database=FullyQualifiedEntityName(
                fqn.build(
                    metadata=self.metadata,
                    entity_type=Database,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                )
            ),
            description=Markdown(f"FHIR Resource Category: {schema_name}"),
        )

    def get_tables_name_and_type(self) -> Optional[Iterable[Tuple[str, str]]]:
        """
        Yield table names (FHIR resources)
        """
        schema_name = self.context.get().database_schema

        try:
            for resource_type in self.service_connection.resourceTypes:
                # Check if resource belongs to current schema
                category = RESOURCE_CATEGORY_MAPPING.get(
                    resource_type, ResourceCategory.CLINICAL
                )
                if category != schema_name:
                    continue

                if filter_by_table(
                    self.source_config.tableFilterPattern, table_name=resource_type
                ):
                    self.status.filter(
                        resource_type,
                        "Table (Resource Type) filtered out",
                    )
                    continue

                yield resource_type, TableType.Regular

        except Exception as exc:
            logger.error(f"Error getting table names: {exc}")
            logger.debug(traceback.format_exc())

    def yield_table(
        self, table_name_and_type: Tuple[str, TableType]
    ) -> Iterable[CreateTableRequest]:
        """
        Yield table request for each FHIR resource
        """
        table_name, table_type = table_name_and_type

        try:
            # Get resource metadata
            columns = self._get_resource_columns(table_name)

            # Create table request
            table_request = CreateTableRequest(
                name=EntityName(table_name),
                databaseSchema=FullyQualifiedEntityName(
                    fqn.build(
                        metadata=self.metadata,
                        entity_type=DatabaseSchema,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=self.context.get().database_schema,
                    )
                ),
                tableType=table_type,
                columns=columns,
                description=Markdown(f"FHIR Resource: {table_name}"),
            )

            yield table_request

        except Exception as exc:
            yield Either(
                left=StackTraceError(
                    name=table_name,
                    error=f"Error yielding table [{table_name}]: {exc}",
                    stackTrace=traceback.format_exc(),
                )
            )

    def _get_resource_columns(self, resource_type: str) -> List[Column]:
        """
        Get columns for a FHIR resource
        """
        columns = []

        # Standard FHIR resource fields
        standard_fields = [
            ("id", "id", "Resource identifier", "1..1"),
            ("meta", "Meta", "Resource metadata", "0..1"),
            ("implicitRules", "uri", "Rules for processing the resource", "0..1"),
            ("language", "code", "Language of the resource content", "0..1"),
        ]

        # Add resource-specific fields based on resource type
        resource_fields = self._get_resource_specific_fields(resource_type)
        all_fields = standard_fields + resource_fields

        for field_name, field_type, description, cardinality in all_fields:
            # Map FHIR type to OpenMetadata type
            data_type = FHIR_TYPE_MAP.get(field_type, DataType.STRING)

            # Determine if field is required
            is_nullable = not cardinality.startswith("1..")

            # Create column
            column = Column(
                name=field_name,
                dataType=data_type,
                description=description,
                constraint=Constraint.NULL if is_nullable else Constraint.NOT_NULL,
            )

            # Add PHI tags if configured
            if self.tag_configuration and self.tag_configuration.enablePHITags:
                tags = self._get_phi_tags_for_field(resource_type, field_name)
                if tags:
                    column.tags = tags

            columns.append(column)

        return columns

    def _get_resource_specific_fields(self, resource_type: str) -> List[Tuple]:
        """
        Get resource-specific fields based on resource type
        """
        # This is a simplified mapping - in production, this would be
        # dynamically retrieved from StructureDefinition
        resource_fields = {
            "Patient": [
                ("identifier", "Identifier", "Patient identifiers", "0..*"),
                ("active", "boolean", "Whether patient record is active", "0..1"),
                ("name", "HumanName", "Patient names", "0..*"),
                ("telecom", "ContactPoint", "Contact details", "0..*"),
                ("gender", "code", "Gender", "0..1"),
                ("birthDate", "date", "Date of birth", "0..1"),
                ("deceased", "boolean", "Indicates if patient is deceased", "0..1"),
                ("address", "Address", "Addresses", "0..*"),
                ("maritalStatus", "CodeableConcept", "Marital status", "0..1"),
                ("contact", "BackboneElement", "Patient contacts", "0..*"),
            ],
            "Encounter": [
                ("identifier", "Identifier", "Encounter identifiers", "0..*"),
                ("status", "code", "Encounter status", "1..1"),
                ("class", "Coding", "Classification of encounter", "1..1"),
                ("type", "CodeableConcept", "Type of encounter", "0..*"),
                ("subject", "Reference", "Patient reference", "0..1"),
                ("participant", "BackboneElement", "Participants", "0..*"),
                ("period", "Period", "Encounter period", "0..1"),
                ("reasonCode", "CodeableConcept", "Reason for encounter", "0..*"),
                ("diagnosis", "BackboneElement", "Encounter diagnoses", "0..*"),
                (
                    "hospitalization",
                    "BackboneElement",
                    "Hospitalization details",
                    "0..1",
                ),
            ],
            "Observation": [
                ("identifier", "Identifier", "Observation identifiers", "0..*"),
                ("status", "code", "Observation status", "1..1"),
                (
                    "category",
                    "CodeableConcept",
                    "Classification of observation",
                    "0..*",
                ),
                ("code", "CodeableConcept", "Type of observation", "1..1"),
                ("subject", "Reference", "Who/what this is about", "0..1"),
                ("encounter", "Reference", "Healthcare event", "0..1"),
                ("effective", "dateTime", "Clinically relevant time", "0..1"),
                ("issued", "instant", "Date/time issued", "0..1"),
                ("value", "Quantity", "Actual result", "0..1"),
                (
                    "interpretation",
                    "CodeableConcept",
                    "Clinical interpretation",
                    "0..*",
                ),
            ],
        }

        return resource_fields.get(resource_type, [])

    def _get_phi_tags_for_field(
        self, resource_type: str, field_name: str
    ) -> Optional[List[TagLabel]]:
        """
        Get PHI tags for a field based on PHI mapping
        """
        tags = []

        # Create PHI classification if not already created
        if "PHI" not in self.created_classifications:
            self._create_phi_classification()
            self.created_classifications.add("PHI")

        # Find matching PHI category for field
        for phi_category, fields in PHI_FIELD_MAPPING.items():
            if field_name in fields:
                tag_label = get_tag_label(
                    metadata=self.metadata,
                    tag_name=phi_category.split(".")[-1],
                    classification_name="PHI",
                )
                if tag_label:
                    tags.append(tag_label)
                break

        return tags or None

    def _create_phi_classification(self):
        """
        Create PHI classification and tags dynamically
        """
        try:
            # Create PHI classification
            classification_request = CreateClassificationRequest(
                name=EntityName("PHI"),
                description=Markdown(
                    "Protected Health Information as defined by HIPAA"
                ),
            )

            classification = self.metadata.create_or_update_classification(
                classification_request
            )

            # Create PHI tags
            phi_tags = [
                ("Identifiable", "Direct identifiers including names, IDs, etc."),
                ("Demographic", "Demographic information that could be PHI"),
                ("Clinical", "Clinical health information"),
                ("Financial", "Healthcare payment and insurance information"),
            ]

            for tag_name, tag_description in phi_tags:
                tag_request = CreateTagRequest(
                    name=EntityName(tag_name),
                    description=Markdown(tag_description),
                    classification=FullyQualifiedEntityName("PHI"),
                )
                self.metadata.create_or_update_tag(tag_request)

        except Exception as exc:
            logger.warning(f"Failed to create PHI classification: {exc}")

    def yield_table_tag(self, table_name: str) -> Iterable[OMetaTagAndClassification]:
        """
        Yield tags to be added to the table
        """
        # Skip tag generation - handled at column level
        return []

    def yield_column_tag(self, column: Column) -> Iterable[OMetaTagAndClassification]:
        """
        Yield tags for columns - already handled in _get_resource_columns
        """
        return []

    def get_stored_procedures(self) -> Iterable[Any]:
        """No stored procedures in FHIR"""
        return []

    def yield_stored_procedure(
        self, stored_procedure: Any
    ) -> Iterable[Either[CreateStoredProcedureRequest]]:
        """No stored procedures in FHIR"""
        return []

    def yield_tag(
        self, schema_name: str
    ) -> Iterable[Either[OMetaTagAndClassification]]:
        """Custom tag logic can be implemented here if needed"""
        return []

    def yield_view_lineage(self) -> Iterable[Either[Any]]:
        """No views in FHIR"""
        return []

    def yield_table_profile(
        self, table_name: str
    ) -> Iterable[CreateTableProfileRequest]:
        """
        Yield table profile data
        """
        try:
            # Get resource count from FHIR server
            row_count = self.client.get_resource_count(table_name)

            table_profile = CreateTableProfileRequest(
                tableProfile=TableProfile(
                    timestamp=None,  # Will be set by the system
                    rowCount=row_count,
                    columnCount=len(self._get_resource_columns(table_name)),
                ),
                table=FullyQualifiedEntityName(
                    fqn.build(
                        metadata=self.metadata,
                        entity_type=Table,
                        service_name=self.context.get().database_service,
                        database_name=self.context.get().database,
                        schema_name=self.context.get().database_schema,
                        table_name=table_name,
                    )
                ),
            )

            yield table_profile

            # Run data quality tests if enabled
            if (
                self.service_connection.tagConfiguration
                and self.service_connection.tagConfiguration.enableDataQualityTests
            ):
                from metadata.ingestion.source.database.epic.quality_runner import (
                    yield_data_quality_results,
                )

                table_fqn = fqn.build(
                    metadata=self.metadata,
                    entity_type=Table,
                    service_name=self.context.get().database_service,
                    database_name=self.context.get().database,
                    schema_name=self.context.get().database_schema,
                    table_name=table_name,
                )

                # Yield data quality results
                for result in yield_data_quality_results(
                    metadata=self.metadata,
                    service_connection=self.service_connection,
                    client=self.client,
                    table_fqn=table_fqn,
                    resource_type=table_name,
                ):
                    if result.right:
                        logger.info(f"Data quality tests completed for {table_name}")
                        logger.debug(result.right.get("report", ""))
                    else:
                        logger.error(f"Data quality tests failed: {result.left}")

        except Exception as exc:
            logger.warning(f"Failed to get profile for {table_name}: {exc}")

    def close(self):
        """Close any connections"""
        pass

    def get_config_schema(cls) -> str:
        """Get config schema"""
        return EpicConnection.schema_json()

    def test_connection(self) -> None:
        """Test connection to Epic FHIR server"""
        from metadata.ingestion.source.database.epic.connection import test_connection

        test_connection(
            metadata=self.metadata,
            client=self.client,
            service_connection=self.service_connection,
        )
