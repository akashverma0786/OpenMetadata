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
Test Epic FHIR data quality checks
"""
import unittest
from datetime import datetime, timedelta

from metadata.generated.schema.tests.basic import TestCaseStatus
from metadata.ingestion.source.database.epic.data_quality import (
    HealthcareDataQualityTests,
)


class TestHealthcareDataQuality(unittest.TestCase):
    """Test healthcare data quality validations"""

    def test_patient_identifier_validation_success(self):
        """Test successful patient identifier validation"""
        patient_data = {
            "identifier": [
                {
                    "system": "http://hospital.org/mrn",
                    "value": "123456",
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "MR",
                            }
                        ]
                    },
                },
                {"system": "http://hl7.org/fhir/sid/us-ssn", "value": "999-99-9999"},
            ]
        }

        result = HealthcareDataQualityTests.validate_patient_identifier(patient_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)
        self.assertIn("2 valid identifiers", result.result)

    def test_patient_identifier_validation_no_mrn(self):
        """Test patient identifier validation without MRN"""
        patient_data = {
            "identifier": [
                {"system": "http://hl7.org/fhir/sid/us-ssn", "value": "999-99-9999"}
            ]
        }

        result = HealthcareDataQualityTests.validate_patient_identifier(patient_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("missing Medical Record Number", result.result)

    def test_patient_identifier_validation_no_identifiers(self):
        """Test patient identifier validation with no identifiers"""
        patient_data = {"identifier": []}

        result = HealthcareDataQualityTests.validate_patient_identifier(patient_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("no identifiers", result.result)

    def test_date_consistency_patient_success(self):
        """Test successful date consistency for patient"""
        patient_data = {
            "birthDate": "1980-01-15",
            "deceasedDateTime": "2020-12-31T10:00:00Z",
        }

        result = HealthcareDataQualityTests.validate_date_consistency(
            patient_data, "Patient"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_date_consistency_patient_future_birth(self):
        """Test date consistency with future birth date"""
        future_date = (datetime.now() + timedelta(days=30)).isoformat()
        patient_data = {"birthDate": future_date}

        result = HealthcareDataQualityTests.validate_date_consistency(
            patient_data, "Patient"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("future", result.result)

    def test_date_consistency_patient_death_before_birth(self):
        """Test date consistency with death before birth"""
        patient_data = {
            "birthDate": "1980-01-15",
            "deceasedDateTime": "1970-12-31T10:00:00Z",
        }

        result = HealthcareDataQualityTests.validate_date_consistency(
            patient_data, "Patient"
        )
        print(f"Result status: {result.testCaseStatus}, message: {result.result}")
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("before birth", result.result)

    def test_date_consistency_encounter_success(self):
        """Test successful date consistency for encounter"""
        encounter_data = {
            "period": {"start": "2023-01-15T10:00:00Z", "end": "2023-01-15T14:00:00Z"}
        }

        result = HealthcareDataQualityTests.validate_date_consistency(
            encounter_data, "Encounter"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_date_consistency_encounter_end_before_start(self):
        """Test encounter with end before start"""
        encounter_data = {
            "period": {"start": "2023-01-15T14:00:00Z", "end": "2023-01-15T10:00:00Z"}
        }

        result = HealthcareDataQualityTests.validate_date_consistency(
            encounter_data, "Encounter"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("before start", result.result)

    def test_coding_standards_validation_success(self):
        """Test successful coding standards validation"""
        resource_data = {
            "code": {
                "coding": [
                    {
                        "system": "http://snomed.info/sct",
                        "code": "386661006",
                        "display": "Fever",
                    },
                    {
                        "system": "http://loinc.org",
                        "code": "8310-5",
                        "display": "Body temperature",
                    },
                ]
            }
        }

        result = HealthcareDataQualityTests.validate_coding_standards(
            resource_data, "code"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_coding_standards_validation_invalid_system(self):
        """Test coding standards with invalid system"""
        resource_data = {
            "code": {
                "coding": [{"system": "http://invalid-system.com", "code": "12345"}]
            }
        }

        result = HealthcareDataQualityTests.validate_coding_standards(
            resource_data, "code"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("Unknown coding system", result.result)

    def test_coding_standards_validation_missing_code(self):
        """Test coding standards with missing code"""
        resource_data = {
            "code": {"coding": [{"system": "http://snomed.info/sct", "code": ""}]}
        }

        result = HealthcareDataQualityTests.validate_coding_standards(
            resource_data, "code"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("Missing system or code", result.result)

    def test_reference_integrity_success(self):
        """Test successful reference integrity validation"""
        resource_data = {
            "subject": {"reference": "Patient/123", "display": "John Doe"},
            "encounter": {"reference": "Encounter/456"},
            "performer": [{"reference": "Practitioner/789"}],
        }

        result = HealthcareDataQualityTests.validate_reference_integrity(resource_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)
        self.assertIn("3 references are valid", result.result)

    def test_reference_integrity_invalid_format(self):
        """Test reference integrity with invalid format"""
        resource_data = {"subject": {"reference": "InvalidReference"}}

        result = HealthcareDataQualityTests.validate_reference_integrity(resource_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("Invalid format", result.result)

    def test_reference_integrity_unknown_type(self):
        """Test reference integrity with unknown resource type"""
        resource_data = {"subject": {"reference": "UnknownType/123"}}

        result = HealthcareDataQualityTests.validate_reference_integrity(resource_data)
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("Unknown resource type", result.result)

    def test_phi_completeness_success(self):
        """Test successful PHI completeness validation"""
        patient_data = {
            "name": [{"family": "Doe", "given": ["John"]}],
            "telecom": [{"system": "phone", "value": "555-1234"}],
            "address": [{"city": "Boston", "state": "MA"}],
            "birthDate": "1980-01-15",
        }

        result = HealthcareDataQualityTests.validate_phi_completeness(
            patient_data, "Patient"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_phi_completeness_missing_fields(self):
        """Test PHI completeness with missing fields"""
        patient_data = {
            "name": [{"family": "Doe", "given": ["John"]}],
            "birthDate": "1980-01-15",
        }

        result = HealthcareDataQualityTests.validate_phi_completeness(
            patient_data, "Patient"
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("telecom", result.result)
        self.assertIn("address", result.result)

    def test_observation_values_success(self):
        """Test successful observation value validation"""
        observation_data = {
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "8867-4",
                        "display": "Heart rate",
                    }
                ]
            },
            "valueQuantity": {"value": 72, "unit": "beats/minute"},
        }

        result = HealthcareDataQualityTests.validate_observation_values(
            observation_data
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_observation_values_out_of_range(self):
        """Test observation value out of range"""
        observation_data = {
            "code": {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "8867-4",
                        "display": "Heart rate",
                    }
                ]
            },
            "valueQuantity": {"value": 250, "unit": "beats/minute"},
        }

        result = HealthcareDataQualityTests.validate_observation_values(
            observation_data
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("outside normal range", result.result)

    def test_encounter_status_transitions_success(self):
        """Test successful encounter status transitions"""
        encounters = [
            {"status": "planned", "period": {"start": "2023-01-15T09:00:00Z"}},
            {"status": "arrived", "period": {"start": "2023-01-15T10:00:00Z"}},
            {"status": "in-progress", "period": {"start": "2023-01-15T10:30:00Z"}},
            {"status": "finished", "period": {"start": "2023-01-15T14:00:00Z"}},
        ]

        result = HealthcareDataQualityTests.validate_encounter_status_transitions(
            encounters
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Success)

    def test_encounter_status_transitions_invalid(self):
        """Test invalid encounter status transitions"""
        encounters = [
            {"status": "planned", "period": {"start": "2023-01-15T09:00:00Z"}},
            {
                "status": "finished",
                "period": {"start": "2023-01-15T10:00:00Z"},
            },  # Invalid transition
            {
                "status": "in-progress",
                "period": {"start": "2023-01-15T11:00:00Z"},
            },  # Invalid transition
        ]

        result = HealthcareDataQualityTests.validate_encounter_status_transitions(
            encounters
        )
        self.assertEqual(result.testCaseStatus, TestCaseStatus.Failed)
        self.assertIn("Invalid status transitions", result.result)
