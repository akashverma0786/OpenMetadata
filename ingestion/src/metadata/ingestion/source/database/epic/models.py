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
Epic FHIR data models
"""
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class FHIRElement(BaseModel):
    """Base FHIR element structure"""

    name: str
    type: str
    cardinality: str = "0..1"
    description: Optional[str] = None
    is_modifier: bool = False
    is_summary: bool = False


class FHIRResource(BaseModel):
    """FHIR Resource metadata"""

    resource_type: str
    url: str
    version: Optional[str] = None
    name: str
    status: str = "active"
    experimental: bool = False
    description: Optional[str] = None
    elements: List[FHIRElement] = Field(default_factory=list)
    search_params: List[str] = Field(default_factory=list)


class FHIRCapabilityStatement(BaseModel):
    """FHIR Server Capability Statement"""

    fhir_version: str
    formats: List[str] = Field(default_factory=list)
    rest: List[Dict] = Field(default_factory=list)


class ResourceCategory:
    """FHIR Resource Categories for schema organization"""

    CLINICAL = "Clinical"
    ADMINISTRATIVE = "Administrative"
    FINANCIAL = "Financial"
    WORKFLOW = "Workflow"
    INFRASTRUCTURE = "Infrastructure"


RESOURCE_CATEGORY_MAPPING = {
    # Clinical Resources
    "Patient": ResourceCategory.CLINICAL,
    "Encounter": ResourceCategory.CLINICAL,
    "Observation": ResourceCategory.CLINICAL,
    "Condition": ResourceCategory.CLINICAL,
    "Procedure": ResourceCategory.CLINICAL,
    "MedicationRequest": ResourceCategory.CLINICAL,
    "MedicationAdministration": ResourceCategory.CLINICAL,
    "Immunization": ResourceCategory.CLINICAL,
    "AllergyIntolerance": ResourceCategory.CLINICAL,
    "DiagnosticReport": ResourceCategory.CLINICAL,
    "CarePlan": ResourceCategory.CLINICAL,
    "CareTeam": ResourceCategory.CLINICAL,
    # Administrative Resources
    "Practitioner": ResourceCategory.ADMINISTRATIVE,
    "PractitionerRole": ResourceCategory.ADMINISTRATIVE,
    "Organization": ResourceCategory.ADMINISTRATIVE,
    "Location": ResourceCategory.ADMINISTRATIVE,
    "HealthcareService": ResourceCategory.ADMINISTRATIVE,
    "Endpoint": ResourceCategory.ADMINISTRATIVE,
    "Patient": ResourceCategory.ADMINISTRATIVE,  # Can be in multiple categories
    # Financial Resources
    "Coverage": ResourceCategory.FINANCIAL,
    "Claim": ResourceCategory.FINANCIAL,
    "ClaimResponse": ResourceCategory.FINANCIAL,
    "ExplanationOfBenefit": ResourceCategory.FINANCIAL,
    "PaymentNotice": ResourceCategory.FINANCIAL,
    "PaymentReconciliation": ResourceCategory.FINANCIAL,
    # Workflow Resources
    "Appointment": ResourceCategory.WORKFLOW,
    "AppointmentResponse": ResourceCategory.WORKFLOW,
    "Schedule": ResourceCategory.WORKFLOW,
    "Task": ResourceCategory.WORKFLOW,
    "ServiceRequest": ResourceCategory.WORKFLOW,
    "CommunicationRequest": ResourceCategory.WORKFLOW,
}


# PHI field mapping for automatic tagging
PHI_FIELD_MAPPING = {
    "PHI.Identifiable": [
        "identifier",
        "name",
        "telecom",
        "address",
        "photo",
        "contact",
        "generalPractitioner",
        "managingOrganization",
    ],
    "PHI.Demographic": [
        "birthDate",
        "gender",
        "race",
        "ethnicity",
        "maritalStatus",
        "multipleBirth",
        "deceased",
    ],
    "PHI.Clinical": [
        "code",
        "value",
        "interpretation",
        "referenceRange",
        "component",
        "diagnosis",
        "procedure",
        "medication",
    ],
    "PHI.Financial": [
        "insurance",
        "coverage",
        "beneficiary",
        "payor",
        "billablePeriod",
        "total",
        "payment",
    ],
}
