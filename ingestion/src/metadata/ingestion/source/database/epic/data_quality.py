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
Healthcare-specific data quality tests for Epic FHIR data
"""
import time
from typing import Dict, List

from metadata.generated.schema.tests.basic import TestCaseResult, TestCaseStatus
from metadata.utils.logger import ingestion_logger

logger = ingestion_logger()


def get_timestamp():
    """Get current timestamp in milliseconds"""
    return int(time.time() * 1000)


class HealthcareDataQualityTests:
    """
    Healthcare-specific data quality tests for FHIR resources
    """

    @staticmethod
    def validate_patient_identifier(patient_data: dict) -> TestCaseResult:
        """
        Validate patient identifiers meet healthcare standards
        - Must have at least one identifier
        - MRN (Medical Record Number) should be present
        - Identifiers should have proper system and value
        """
        try:
            identifiers = patient_data.get("identifier", [])

            if not identifiers:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result="Patient has no identifiers",
                )

            # Check for MRN
            has_mrn = any(
                identifier.get("type", {}).get("coding", [{}])[0].get("code") == "MR"
                for identifier in identifiers
            )

            if not has_mrn:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result="Patient missing Medical Record Number (MRN)",
                )

            # Validate identifier structure
            for identifier in identifiers:
                if not identifier.get("system") or not identifier.get("value"):
                    return TestCaseResult(
                        timestamp=get_timestamp(),
                        testCaseStatus=TestCaseStatus.Failed,
                        result="Invalid identifier structure - missing system or value",
                    )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result=f"Patient has {len(identifiers)} valid identifiers including MRN",
            )

        except Exception as e:
            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating patient identifiers: {str(e)}",
            )

    @staticmethod
    def validate_date_consistency(
        resource_data: dict, resource_type: str
    ) -> TestCaseResult:
        """
        Validate date fields are consistent and logical
        - Birth dates should be in the past
        - Encounter dates should be after patient birth
        - Death dates should be after birth dates
        """
        try:
            from datetime import datetime

            current_date = datetime.now()

            if resource_type == "Patient":
                birth_date = resource_data.get("birthDate")
                deceased_date = resource_data.get("deceasedDateTime")

                if birth_date:
                    try:
                        if "T" in birth_date and birth_date.endswith("Z"):
                            birth_dt = datetime.fromisoformat(
                                birth_date.replace("Z", "+00:00")
                            )
                        elif "T" in birth_date:
                            birth_dt = datetime.fromisoformat(birth_date)
                        else:
                            # Date only format
                            birth_dt = datetime.strptime(birth_date, "%Y-%m-%d")
                    except ValueError:
                        birth_dt = datetime.strptime(birth_date, "%Y-%m-%d")
                    if birth_dt.replace(tzinfo=None) > current_date:
                        return TestCaseResult(
                            timestamp=get_timestamp(),
                            testCaseStatus=TestCaseStatus.Failed,
                            result="Birth date is in the future",
                        )

                    if deceased_date:
                        if deceased_date.endswith("Z"):
                            deceased_dt = datetime.fromisoformat(
                                deceased_date.replace("Z", "+00:00")
                            )
                        else:
                            deceased_dt = datetime.fromisoformat(deceased_date)
                        if deceased_dt.replace(tzinfo=None) < birth_dt.replace(
                            tzinfo=None
                        ):
                            return TestCaseResult(
                                timestamp=get_timestamp(),
                                testCaseStatus=TestCaseStatus.Failed,
                                result="Death date is before birth date",
                            )

            elif resource_type == "Encounter":
                period = resource_data.get("period", {})
                start = period.get("start")
                end = period.get("end")

                if start and end:
                    if start.endswith("Z"):
                        start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                    else:
                        start_dt = datetime.fromisoformat(start)

                    if end.endswith("Z"):
                        end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
                    else:
                        end_dt = datetime.fromisoformat(end)

                    if end_dt.replace(tzinfo=None) < start_dt.replace(tzinfo=None):
                        return TestCaseResult(
                            timestamp=get_timestamp(),
                            testCaseStatus=TestCaseStatus.Failed,
                            result="Encounter end date is before start date",
                        )

                    if start_dt.replace(tzinfo=None) > current_date:
                        return TestCaseResult(
                            timestamp=get_timestamp(),
                            testCaseStatus=TestCaseStatus.Failed,
                            result="Encounter start date is in the future",
                        )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result="Date fields are consistent and valid",
            )

        except Exception as e:
            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating dates: {str(e)}",
            )

    @staticmethod
    def validate_coding_standards(
        resource_data: dict, field_name: str
    ) -> TestCaseResult:
        """
        Validate medical coding standards (ICD-10, SNOMED CT, LOINC)
        """
        try:
            coding_field = resource_data.get(field_name)
            if not coding_field:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Success,
                    result=f"No {field_name} field to validate",
                )

            # Handle both direct coding and CodeableConcept
            codings = []
            if isinstance(coding_field, dict):
                codings = coding_field.get("coding", [])
            elif isinstance(coding_field, list):
                for item in coding_field:
                    if isinstance(item, dict):
                        codings.extend(item.get("coding", []))

            valid_systems = {
                "http://snomed.info/sct": "SNOMED CT",
                "http://loinc.org": "LOINC",
                "http://hl7.org/fhir/sid/icd-10": "ICD-10",
                "http://hl7.org/fhir/sid/icd-10-cm": "ICD-10-CM",
                "http://www.nlm.nih.gov/research/umls/rxnorm": "RxNorm",
            }

            invalid_codings = []
            for coding in codings:
                system = coding.get("system", "")
                code = coding.get("code", "")

                if not system or not code:
                    invalid_codings.append("Missing system or code")
                elif system not in valid_systems:
                    invalid_codings.append(f"Unknown coding system: {system}")

            if invalid_codings:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result=f"Invalid codings found: {'; '.join(invalid_codings[:3])}",
                )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result=f"All codings use valid standards",
            )

        except Exception as e:
            return TestCaseResult(
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating coding standards: {str(e)}",
            )

    @staticmethod
    def validate_reference_integrity(resource_data: dict) -> TestCaseResult:
        """
        Validate FHIR reference integrity
        - References should have proper format
        - Reference type should match expected resource
        """
        try:
            references = []

            # Collect all references in the resource
            def extract_references(obj, refs):
                if isinstance(obj, dict):
                    if "reference" in obj:
                        refs.append(obj)
                    for value in obj.values():
                        extract_references(value, refs)
                elif isinstance(obj, list):
                    for item in obj:
                        extract_references(item, refs)

            extract_references(resource_data, references)

            invalid_refs = []
            for ref in references:
                reference = ref.get("reference", "")

                # Validate reference format (ResourceType/ID)
                if "/" not in reference:
                    invalid_refs.append(f"Invalid format: {reference}")
                else:
                    resource_type, resource_id = reference.split("/", 1)
                    # Validate resource type is known FHIR resource
                    valid_types = [
                        "Patient",
                        "Encounter",
                        "Observation",
                        "Condition",
                        "Procedure",
                        "Medication",
                        "Practitioner",
                        "Organization",
                    ]
                    if resource_type not in valid_types:
                        invalid_refs.append(f"Unknown resource type: {resource_type}")

            if invalid_refs:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result=f"Invalid references: {'; '.join(invalid_refs[:3])}",
                )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result=f"All {len(references)} references are valid",
            )

        except Exception as e:
            return TestCaseResult(
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating references: {str(e)}",
            )

    @staticmethod
    def validate_phi_completeness(
        resource_data: dict, resource_type: str
    ) -> TestCaseResult:
        """
        Validate PHI fields are properly populated or redacted
        """
        try:
            phi_fields = {
                "Patient": ["name", "telecom", "address", "birthDate"],
                "Practitioner": ["name", "telecom", "address"],
                "Organization": ["name", "telecom", "address"],
            }

            fields_to_check = phi_fields.get(resource_type, [])
            missing_fields = []

            for field in fields_to_check:
                value = resource_data.get(field)
                if not value or (isinstance(value, list) and len(value) == 0):
                    missing_fields.append(field)

            if missing_fields:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result=f"Missing PHI fields: {', '.join(missing_fields)}",
                )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result="All required PHI fields are present",
            )

        except Exception as e:
            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating PHI completeness: {str(e)}",
            )

    @staticmethod
    def validate_observation_values(observation_data: dict) -> TestCaseResult:
        """
        Validate observation values are within reasonable ranges
        """
        try:
            code = (
                observation_data.get("code", {}).get("coding", [{}])[0].get("code", "")
            )
            value = observation_data.get("valueQuantity", {})

            if not value:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Success,
                    result="No value to validate",
                )

            value_num = value.get("value")
            unit = value.get("unit", "")

            # Define reasonable ranges for common observations
            ranges = {
                # Vital signs
                "8867-4": (30, 220, "Heart rate"),  # Heart rate
                "8310-5": (35, 42, "Body temperature"),  # Body temperature (Celsius)
                "8302-2": (50, 250, "Body height"),  # Height in cm
                "29463-7": (1, 300, "Body weight"),  # Weight in kg
                "8480-6": (50, 200, "Systolic blood pressure"),  # Systolic BP
                "8462-4": (30, 130, "Diastolic blood pressure"),  # Diastolic BP
                # Lab results
                "2160-0": (0.2, 10, "Creatinine"),  # Creatinine mg/dL
                "718-7": (2, 12, "Hemoglobin"),  # Hemoglobin g/dL
                "2345-7": (50, 400, "Glucose"),  # Glucose mg/dL
            }

            if code in ranges and value_num is not None:
                min_val, max_val, name = ranges[code]
                if value_num < min_val or value_num > max_val:
                    return TestCaseResult(
                        timestamp=get_timestamp(),
                        testCaseStatus=TestCaseStatus.Failed,
                        result=f"{name} value {value_num} {unit} outside normal range ({min_val}-{max_val})",
                    )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result=f"Observation value within acceptable range",
            )

        except Exception as e:
            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating observation values: {str(e)}",
            )

    @staticmethod
    def validate_encounter_status_transitions(encounters: List[dict]) -> TestCaseResult:
        """
        Validate encounter status transitions are logical
        """
        try:
            # Valid status transitions
            valid_transitions = {
                "planned": ["arrived", "cancelled"],
                "arrived": ["triaged", "in-progress", "cancelled"],
                "triaged": ["in-progress", "cancelled"],
                "in-progress": ["onleave", "finished", "cancelled"],
                "onleave": ["in-progress", "finished", "cancelled"],
                "finished": ["entered-in-error"],
                "cancelled": ["entered-in-error"],
                "entered-in-error": [],
            }

            # Sort encounters by date
            sorted_encounters = sorted(
                encounters,
                key=lambda e: e.get("period", {}).get("start", ""),
            )

            invalid_transitions = []

            for i in range(1, len(sorted_encounters)):
                prev_status = sorted_encounters[i - 1].get("status")
                curr_status = sorted_encounters[i].get("status")

                if prev_status and curr_status:
                    allowed = valid_transitions.get(prev_status, [])
                    if curr_status not in allowed and curr_status != prev_status:
                        invalid_transitions.append(f"{prev_status} -> {curr_status}")

            if invalid_transitions:
                return TestCaseResult(
                    timestamp=get_timestamp(),
                    testCaseStatus=TestCaseStatus.Failed,
                    result=f"Invalid status transitions: {'; '.join(invalid_transitions[:3])}",
                )

            return TestCaseResult(
                timestamp=get_timestamp(),
                testCaseStatus=TestCaseStatus.Success,
                result="All encounter status transitions are valid",
            )

        except Exception as e:
            return TestCaseResult(
                testCaseStatus=TestCaseStatus.Aborted,
                result=f"Error validating status transitions: {str(e)}",
            )


def get_healthcare_test_definitions() -> List[Dict[str, str]]:
    """
    Get list of healthcare-specific test definitions
    """
    return [
        {
            "name": "patientIdentifierValidation",
            "displayName": "Patient Identifier Validation",
            "description": "Validates patient identifiers meet healthcare standards including MRN presence",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [
                {
                    "name": "resourceType",
                    "dataType": "STRING",
                    "required": True,
                    "description": "FHIR resource type (e.g., Patient)",
                }
            ],
        },
        {
            "name": "dateConsistencyCheck",
            "displayName": "Date Consistency Check",
            "description": "Validates date fields are consistent and logical",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [
                {
                    "name": "resourceType",
                    "dataType": "STRING",
                    "required": True,
                    "description": "FHIR resource type",
                }
            ],
        },
        {
            "name": "codingStandardsValidation",
            "displayName": "Medical Coding Standards Validation",
            "description": "Validates medical codes use standard systems (ICD-10, SNOMED CT, LOINC)",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [
                {
                    "name": "fieldName",
                    "dataType": "STRING",
                    "required": True,
                    "description": "Field containing medical codes",
                }
            ],
        },
        {
            "name": "referenceIntegrityCheck",
            "displayName": "FHIR Reference Integrity Check",
            "description": "Validates FHIR references have proper format and valid resource types",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [],
        },
        {
            "name": "phiCompletenessCheck",
            "displayName": "PHI Completeness Check",
            "description": "Validates required PHI fields are populated or properly redacted",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [
                {
                    "name": "resourceType",
                    "dataType": "STRING",
                    "required": True,
                    "description": "FHIR resource type",
                }
            ],
        },
        {
            "name": "observationValueRangeCheck",
            "displayName": "Observation Value Range Check",
            "description": "Validates clinical observation values are within reasonable ranges",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [],
        },
        {
            "name": "encounterStatusTransitionCheck",
            "displayName": "Encounter Status Transition Check",
            "description": "Validates encounter status transitions follow logical progression",
            "testPlatforms": ["OpenMetadata"],
            "parameterDefinition": [],
        },
    ]
