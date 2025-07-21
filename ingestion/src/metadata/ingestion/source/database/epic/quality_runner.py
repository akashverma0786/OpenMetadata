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
Healthcare data quality runner for Epic FHIR data
"""
from typing import Dict, Iterable, List, Optional

from metadata.generated.schema.api.tests.createTestCase import CreateTestCaseRequest
from metadata.generated.schema.api.tests.createTestDefinition import (
    CreateTestDefinitionRequest,
)
from metadata.generated.schema.api.tests.createTestSuite import CreateTestSuiteRequest
from metadata.generated.schema.entity.data.table import Table
from metadata.generated.schema.entity.services.connections.database.epicConnection import (
    EpicConnection,
)
from metadata.generated.schema.tests.basic import TestCaseResult
from metadata.generated.schema.tests.testCase import TestCaseParameterValue
from metadata.generated.schema.tests.testDefinition import EntityType, TestPlatform
from metadata.generated.schema.tests.testSuite import TestSuiteExecutableEntityReference
from metadata.generated.schema.type.basic import (
    EntityName,
    FullyQualifiedEntityName,
    Markdown,
)
from metadata.ingestion.api.models import Either
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.ingestion.source.database.epic.connection import EpicFHIRClient
from metadata.ingestion.source.database.epic.data_quality import (
    HealthcareDataQualityTests,
    get_healthcare_test_definitions,
)
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


class HealthcareDataQualityRunner:
    """
    Runner for healthcare-specific data quality tests
    """

    def __init__(
        self,
        metadata: OpenMetadata,
        service_connection: EpicConnection,
        client: EpicFHIRClient,
    ):
        self.metadata = metadata
        self.service_connection = service_connection
        self.client = client
        self.quality_tests = HealthcareDataQualityTests()
        self._test_definitions = {}
        self._test_suite = None

    def create_test_definitions(self) -> None:
        """
        Create healthcare-specific test definitions in OpenMetadata
        """
        try:
            for test_def in get_healthcare_test_definitions():
                request = CreateTestDefinitionRequest(
                    name=EntityName(test_def["name"]),
                    displayName=test_def["displayName"],
                    description=Markdown(test_def["description"]),
                    testPlatforms=[TestPlatform.OpenMetadata],
                    entityType=EntityType.TABLE,
                    parameterDefinition=test_def.get("parameterDefinition", []),
                )

                definition = self.metadata.create_or_update(request)
                self._test_definitions[test_def["name"]] = definition
                logger.info(f"Created test definition: {test_def['name']}")

        except Exception as e:
            logger.error(f"Error creating test definitions: {e}")

    def create_test_suite(self, table_fqn: str) -> Optional[str]:
        """
        Create a test suite for healthcare data quality
        """
        try:
            table = self.metadata.get_by_name(entity=Table, fqn=table_fqn)
            if not table:
                logger.warning(f"Table not found: {table_fqn}")
                return None

            request = CreateTestSuiteRequest(
                name=EntityName(f"{table.name.__root__}_healthcare_quality"),
                displayName=f"Healthcare Data Quality - {table.displayName or table.name.__root__}",
                description=Markdown(
                    f"Healthcare-specific data quality tests for {table.name.__root__} FHIR resource"
                ),
                executable=True,
                executableEntityReference=TestSuiteExecutableEntityReference(
                    id=table.id, type="table"
                ),
            )

            self._test_suite = self.metadata.create_or_update(request)
            logger.info(f"Created test suite for {table_fqn}")
            return self._test_suite.fullyQualifiedName.__root__

        except Exception as e:
            logger.error(f"Error creating test suite: {e}")
            return None

    def create_test_cases(self, table_fqn: str, resource_type: str) -> List[str]:
        """
        Create test cases for a specific FHIR resource type
        """
        created_cases = []

        if not self._test_suite:
            logger.warning("Test suite not created")
            return created_cases

        try:
            # Map resource types to applicable tests
            test_mapping = {
                "Patient": [
                    "patientIdentifierValidation",
                    "dateConsistencyCheck",
                    "referenceIntegrityCheck",
                    "phiCompletenessCheck",
                ],
                "Encounter": [
                    "dateConsistencyCheck",
                    "referenceIntegrityCheck",
                    "encounterStatusTransitionCheck",
                ],
                "Observation": [
                    "codingStandardsValidation",
                    "referenceIntegrityCheck",
                    "observationValueRangeCheck",
                ],
                "Condition": [
                    "codingStandardsValidation",
                    "referenceIntegrityCheck",
                    "dateConsistencyCheck",
                ],
                "Procedure": [
                    "codingStandardsValidation",
                    "referenceIntegrityCheck",
                    "dateConsistencyCheck",
                ],
                "Medication": ["codingStandardsValidation", "referenceIntegrityCheck"],
                "Practitioner": ["phiCompletenessCheck", "referenceIntegrityCheck"],
                "Organization": ["phiCompletenessCheck", "referenceIntegrityCheck"],
            }

            applicable_tests = test_mapping.get(
                resource_type, ["referenceIntegrityCheck"]
            )

            for test_name in applicable_tests:
                if test_name not in self._test_definitions:
                    continue

                # Create test case
                parameters = []
                if test_name in ["dateConsistencyCheck", "phiCompletenessCheck"]:
                    parameters.append(
                        TestCaseParameterValue(name="resourceType", value=resource_type)
                    )
                elif test_name == "codingStandardsValidation":
                    # Determine field name based on resource type
                    field_map = {
                        "Observation": "code",
                        "Condition": "code",
                        "Procedure": "code",
                        "Medication": "code",
                    }
                    field_name = field_map.get(resource_type, "code")
                    parameters.append(
                        TestCaseParameterValue(name="fieldName", value=field_name)
                    )

                request = CreateTestCaseRequest(
                    name=EntityName(f"{resource_type}_{test_name}"),
                    displayName=f"{test_name} for {resource_type}",
                    description=Markdown(
                        f"Validates {test_name} for {resource_type} FHIR resources"
                    ),
                    testDefinition=FullyQualifiedEntityName(
                        self._test_definitions[test_name].fullyQualifiedName.__root__
                    ),
                    entityLink=f"<#E::table::{table_fqn}>",
                    testSuite=FullyQualifiedEntityName(
                        self._test_suite.fullyQualifiedName.__root__
                    ),
                    parameterValues=parameters,
                )

                test_case = self.metadata.create_or_update(request)
                created_cases.append(test_case.fullyQualifiedName.__root__)
                logger.info(f"Created test case: {resource_type}_{test_name}")

        except Exception as e:
            logger.error(f"Error creating test cases: {e}")

        return created_cases

    def run_quality_tests(
        self, table_fqn: str, resource_type: str
    ) -> Dict[str, TestCaseResult]:
        """
        Run quality tests for a specific resource type
        """
        results = {}

        try:
            # Fetch sample data from FHIR server
            sample_data = self.client.search_resources(
                resource_type, params={"_count": 10}
            )

            resources = sample_data.get("entry", [])
            if not resources:
                logger.warning(f"No {resource_type} resources found for testing")
                return results

            # Run applicable tests based on resource type
            if resource_type == "Patient":
                for entry in resources[:3]:  # Test first 3 resources
                    resource = entry.get("resource", {})
                    results[
                        f"patient_identifier_{resource.get('id')}"
                    ] = self.quality_tests.validate_patient_identifier(resource)
                    results[
                        f"date_consistency_{resource.get('id')}"
                    ] = self.quality_tests.validate_date_consistency(
                        resource, resource_type
                    )
                    results[
                        f"phi_completeness_{resource.get('id')}"
                    ] = self.quality_tests.validate_phi_completeness(
                        resource, resource_type
                    )
                    results[
                        f"reference_integrity_{resource.get('id')}"
                    ] = self.quality_tests.validate_reference_integrity(resource)

            elif resource_type == "Encounter":
                # Collect all encounters for status transition check
                all_encounters = [entry.get("resource", {}) for entry in resources]
                results[
                    "encounter_status_transitions"
                ] = self.quality_tests.validate_encounter_status_transitions(
                    all_encounters
                )

                # Individual encounter tests
                for entry in resources[:3]:
                    resource = entry.get("resource", {})
                    results[
                        f"date_consistency_{resource.get('id')}"
                    ] = self.quality_tests.validate_date_consistency(
                        resource, resource_type
                    )
                    results[
                        f"reference_integrity_{resource.get('id')}"
                    ] = self.quality_tests.validate_reference_integrity(resource)

            elif resource_type == "Observation":
                for entry in resources[:3]:
                    resource = entry.get("resource", {})
                    results[
                        f"coding_standards_{resource.get('id')}"
                    ] = self.quality_tests.validate_coding_standards(resource, "code")
                    results[
                        f"value_range_{resource.get('id')}"
                    ] = self.quality_tests.validate_observation_values(resource)
                    results[
                        f"reference_integrity_{resource.get('id')}"
                    ] = self.quality_tests.validate_reference_integrity(resource)

            else:
                # Default tests for other resource types
                for entry in resources[:3]:
                    resource = entry.get("resource", {})
                    results[
                        f"reference_integrity_{resource.get('id')}"
                    ] = self.quality_tests.validate_reference_integrity(resource)

        except Exception as e:
            logger.error(f"Error running quality tests: {e}")

        return results

    def generate_quality_report(self, results: Dict[str, TestCaseResult]) -> str:
        """
        Generate a summary report of quality test results
        """
        total_tests = len(results)
        passed = sum(
            1 for r in results.values() if r.testCaseStatus == TestCaseStatus.Success
        )
        failed = sum(
            1 for r in results.values() if r.testCaseStatus == TestCaseStatus.Failed
        )
        warnings = sum(
            1 for r in results.values() if r.testCaseStatus == TestCaseStatus.Warning
        )
        aborted = sum(
            1 for r in results.values() if r.testCaseStatus == TestCaseStatus.Aborted
        )

        report = f"""
Healthcare Data Quality Report
==============================
Total Tests: {total_tests}
Passed: {passed} ({passed/total_tests*100:.1f}%)
Failed: {failed} ({failed/total_tests*100:.1f}%)
Warnings: {warnings} ({warnings/total_tests*100:.1f}%)
Aborted: {aborted} ({aborted/total_tests*100:.1f}%)

Detailed Results:
"""

        for test_name, result in results.items():
            status_emoji = {
                TestCaseStatus.Success: "✅",
                TestCaseStatus.Failed: "❌",
                TestCaseStatus.Warning: "⚠️",
                TestCaseStatus.Aborted: "🚫",
            }.get(result.testCaseStatus, "❓")

            report += f"\n{status_emoji} {test_name}: {result.result}"

        return report


def yield_data_quality_results(
    metadata: OpenMetadata,
    service_connection: EpicConnection,
    client: EpicFHIRClient,
    table_fqn: str,
    resource_type: str,
) -> Iterable[Either[Dict]]:
    """
    Yield data quality test results for ingestion
    """
    try:
        runner = HealthcareDataQualityRunner(metadata, service_connection, client)

        # Create test definitions if needed
        runner.create_test_definitions()

        # Create test suite
        suite_fqn = runner.create_test_suite(table_fqn)
        if not suite_fqn:
            yield Either(
                left={
                    "error": f"Failed to create test suite for {table_fqn}",
                    "stackTrace": "",
                }
            )
            return

        # Create test cases
        test_cases = runner.create_test_cases(table_fqn, resource_type)

        # Run quality tests
        results = runner.run_quality_tests(table_fqn, resource_type)

        # Generate report
        report = runner.generate_quality_report(results)

        yield Either(
            right={
                "table_fqn": table_fqn,
                "resource_type": resource_type,
                "test_suite": suite_fqn,
                "test_cases": test_cases,
                "results": {k: v.model_dump() for k, v in results.items()},
                "report": report,
            }
        )

    except Exception as e:
        yield Either(
            left={
                "error": f"Error running data quality tests: {str(e)}",
                "stackTrace": traceback.format_exc(),
            }
        )
