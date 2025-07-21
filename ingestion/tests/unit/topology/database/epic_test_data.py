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
Sample FHIR data for Epic connector tests
"""

# Complete Patient resource example
SAMPLE_PATIENT = {
    "resourceType": "Patient",
    "id": "example",
    "meta": {"versionId": "1", "lastUpdated": "2024-01-15T10:30:00Z"},
    "identifier": [
        {
            "use": "usual",
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                        "code": "MR",
                    }
                ]
            },
            "system": "http://hospital.org/mrn",
            "value": "123456789",
        }
    ],
    "active": True,
    "name": [
        {
            "use": "official",
            "family": "Smith",
            "given": ["John", "Robert"],
            "prefix": ["Mr."],
        }
    ],
    "telecom": [
        {"system": "phone", "value": "(555) 123-4567", "use": "home"},
        {"system": "email", "value": "john.smith@example.com"},
    ],
    "gender": "male",
    "birthDate": "1980-05-15",
    "deceasedBoolean": False,
    "address": [
        {
            "use": "home",
            "type": "both",
            "text": "123 Main St, Apt 4B, Springfield, IL 62701",
            "line": ["123 Main St", "Apt 4B"],
            "city": "Springfield",
            "district": "Sangamon",
            "state": "IL",
            "postalCode": "62701",
            "country": "USA",
        }
    ],
    "maritalStatus": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus",
                "code": "M",
                "display": "Married",
            }
        ]
    },
    "contact": [
        {
            "relationship": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v2-0131",
                            "code": "C",
                            "display": "Emergency Contact",
                        }
                    ]
                }
            ],
            "name": {"family": "Smith", "given": ["Jane"]},
            "telecom": [{"system": "phone", "value": "(555) 987-6543"}],
        }
    ],
    "generalPractitioner": [
        {"reference": "Practitioner/example-gp", "display": "Dr. Sarah Johnson"}
    ],
}

# Complete Encounter resource example
SAMPLE_ENCOUNTER = {
    "resourceType": "Encounter",
    "id": "example",
    "identifier": [{"system": "http://hospital.org/visits", "value": "V-20240115-001"}],
    "status": "finished",
    "class": {
        "system": "http://terminology.hl7.org/CodeSystem/v3-ActCode",
        "code": "AMB",
        "display": "ambulatory",
    },
    "type": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "185345009",
                    "display": "Encounter for symptom",
                }
            ]
        }
    ],
    "priority": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/v3-ActPriority",
                "code": "R",
                "display": "routine",
            }
        ]
    },
    "subject": {"reference": "Patient/example", "display": "John Smith"},
    "participant": [
        {
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                            "code": "PPRF",
                            "display": "primary performer",
                        }
                    ]
                }
            ],
            "individual": {
                "reference": "Practitioner/example-gp",
                "display": "Dr. Sarah Johnson",
            },
        }
    ],
    "period": {"start": "2024-01-15T09:00:00Z", "end": "2024-01-15T09:30:00Z"},
    "reasonCode": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "25064002",
                    "display": "Headache",
                }
            ]
        }
    ],
    "diagnosis": [
        {
            "condition": {
                "reference": "Condition/example",
                "display": "Tension headache",
            },
            "use": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/diagnosis-role",
                        "code": "DD",
                        "display": "Discharge diagnosis",
                    }
                ]
            },
            "rank": 1,
        }
    ],
}

# Complete Observation resource example (lab result)
SAMPLE_OBSERVATION = {
    "resourceType": "Observation",
    "id": "example-lab",
    "identifier": [{"system": "http://hospital.org/labs", "value": "LAB-2024-00123"}],
    "status": "final",
    "category": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                    "code": "laboratory",
                    "display": "Laboratory",
                }
            ]
        }
    ],
    "code": {
        "coding": [
            {
                "system": "http://loinc.org",
                "code": "2947-0",
                "display": "Sodium [Moles/volume] in Blood",
            }
        ]
    },
    "subject": {"reference": "Patient/example", "display": "John Smith"},
    "encounter": {"reference": "Encounter/example"},
    "effectiveDateTime": "2024-01-15T08:30:00Z",
    "issued": "2024-01-15T10:45:00Z",
    "performer": [{"reference": "Organization/lab", "display": "Clinical Laboratory"}],
    "valueQuantity": {
        "value": 140,
        "unit": "mmol/L",
        "system": "http://unitsofmeasure.org",
        "code": "mmol/L",
    },
    "interpretation": [
        {
            "coding": [
                {
                    "system": "http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation",
                    "code": "N",
                    "display": "Normal",
                }
            ]
        }
    ],
    "referenceRange": [
        {
            "low": {
                "value": 136,
                "unit": "mmol/L",
                "system": "http://unitsofmeasure.org",
                "code": "mmol/L",
            },
            "high": {
                "value": 145,
                "unit": "mmol/L",
                "system": "http://unitsofmeasure.org",
                "code": "mmol/L",
            },
        }
    ],
}

# Structure Definition for Patient resource (simplified)
PATIENT_STRUCTURE_DEFINITION = {
    "resourceType": "StructureDefinition",
    "id": "Patient",
    "url": "http://hl7.org/fhir/StructureDefinition/Patient",
    "name": "Patient",
    "status": "active",
    "kind": "resource",
    "abstract": False,
    "type": "Patient",
    "baseDefinition": "http://hl7.org/fhir/StructureDefinition/DomainResource",
    "derivation": "specialization",
    "snapshot": {
        "element": [
            {
                "id": "Patient",
                "path": "Patient",
                "short": "Information about an individual or animal receiving health care services",
                "min": 0,
                "max": "*",
            },
            {
                "id": "Patient.id",
                "path": "Patient.id",
                "short": "Logical id of this artifact",
                "min": 0,
                "max": "1",
                "type": [{"code": "id"}],
            },
            {
                "id": "Patient.identifier",
                "path": "Patient.identifier",
                "short": "An identifier for this patient",
                "min": 0,
                "max": "*",
                "type": [{"code": "Identifier"}],
            },
            {
                "id": "Patient.active",
                "path": "Patient.active",
                "short": "Whether this patient's record is in active use",
                "min": 0,
                "max": "1",
                "type": [{"code": "boolean"}],
            },
            {
                "id": "Patient.name",
                "path": "Patient.name",
                "short": "A name associated with the patient",
                "min": 0,
                "max": "*",
                "type": [{"code": "HumanName"}],
            },
            {
                "id": "Patient.telecom",
                "path": "Patient.telecom",
                "short": "A contact detail for the individual",
                "min": 0,
                "max": "*",
                "type": [{"code": "ContactPoint"}],
            },
            {
                "id": "Patient.gender",
                "path": "Patient.gender",
                "short": "male | female | other | unknown",
                "min": 0,
                "max": "1",
                "type": [{"code": "code"}],
            },
            {
                "id": "Patient.birthDate",
                "path": "Patient.birthDate",
                "short": "The date of birth for the individual",
                "min": 0,
                "max": "1",
                "type": [{"code": "date"}],
            },
            {
                "id": "Patient.deceased[x]",
                "path": "Patient.deceased[x]",
                "short": "Indicates if the individual is deceased or not",
                "min": 0,
                "max": "1",
                "type": [{"code": "boolean"}, {"code": "dateTime"}],
            },
            {
                "id": "Patient.address",
                "path": "Patient.address",
                "short": "An address for the individual",
                "min": 0,
                "max": "*",
                "type": [{"code": "Address"}],
            },
            {
                "id": "Patient.maritalStatus",
                "path": "Patient.maritalStatus",
                "short": "Marital (civil) status of a patient",
                "min": 0,
                "max": "1",
                "type": [{"code": "CodeableConcept"}],
            },
        ]
    },
}

# Bundle response for Patient search
PATIENT_SEARCH_BUNDLE = {
    "resourceType": "Bundle",
    "id": "searchset",
    "type": "searchset",
    "total": 1523,
    "link": [
        {"relation": "self", "url": "https://epicserver.org/fhir/Patient?_count=10"},
        {
            "relation": "next",
            "url": "https://epicserver.org/fhir/Patient?_count=10&_offset=10",
        },
    ],
    "entry": [
        {
            "fullUrl": "https://epicserver.org/fhir/Patient/example",
            "resource": SAMPLE_PATIENT,
            "search": {"mode": "match"},
        }
    ],
}

# OAuth token response
OAUTH_TOKEN_RESPONSE = {
    "access_token": "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "Bearer",
    "expires_in": 3600,
    "scope": "system/*.read",
}

# Error responses
ERROR_UNAUTHORIZED = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "error",
            "code": "security",
            "details": {"text": "Authentication failed"},
        }
    ],
}

ERROR_NOT_FOUND = {
    "resourceType": "OperationOutcome",
    "issue": [
        {
            "severity": "error",
            "code": "not-found",
            "details": {"text": "Resource not found"},
        }
    ],
}
