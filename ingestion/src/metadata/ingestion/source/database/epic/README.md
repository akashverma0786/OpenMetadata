# Epic FHIR Connector for OpenMetadata

This connector enables ingestion of healthcare data from Epic's FHIR APIs into OpenMetadata, providing comprehensive metadata management for healthcare organizations.

## Features

### Core Capabilities
- **FHIR Resource Ingestion**: Supports all major FHIR resource types (Patient, Encounter, Observation, etc.)
- **Multiple Authentication Methods**: OAuth2, Basic Auth, and SMART on FHIR
- **Dynamic Entity Mapping**: Maps FHIR resources to OpenMetadata's database/schema/table structure
- **Healthcare-Specific Metadata**: Captures medical coding systems, resource relationships, and clinical categories

### Healthcare-Specific Features
1. **PHI Classification**: Automatic tagging of Protected Health Information fields
2. **Clinical Categorization**: Organizes resources into Clinical, Administrative, Financial, and Workflow categories
3. **Data Quality Tests**: Healthcare-specific validation including:
   - Patient identifier validation (MRN presence)
   - Date consistency checks
   - Medical coding standards validation (ICD-10, SNOMED CT, LOINC)
   - FHIR reference integrity
   - Observation value range checks
   - Encounter status transition validation

4. **Custom Classifications**: Support for organization-specific tags and classifications
5. **Data Masking**: Built-in support for PHI data masking

## Configuration

### Basic Configuration

```yaml
source:
  type: epic
  serviceName: epic_hospital
  serviceConnection:
    config:
      type: Epic
      fhirServerUrl: https://epicserver.hospital.org/fhir
      authType: OAuth2
      clientId: ${EPIC_CLIENT_ID}
      clientSecret: ${EPIC_CLIENT_SECRET}
      tokenUrl: https://epicserver.hospital.org/oauth2/token
      scope: "system/*.read"
```

### Authentication Options

#### OAuth2 (Recommended)
```yaml
authType: OAuth2
clientId: ${EPIC_CLIENT_ID}
clientSecret: ${EPIC_CLIENT_SECRET}
tokenUrl: https://epicserver.hospital.org/oauth2/token
scope: "system/*.read"
```

#### Basic Authentication
```yaml
authType: BasicAuth
username: ${EPIC_USERNAME}
password: ${EPIC_PASSWORD}
```

#### SMART on FHIR
```yaml
authType: SmartOnFhir
clientId: ${EPIC_CLIENT_ID}
clientSecret: ${EPIC_CLIENT_SECRET}
tokenUrl: https://epicserver.hospital.org/oauth2/token
authorizeUrl: https://epicserver.hospital.org/oauth2/authorize
scope: "patient/*.read launch/patient"
```

### Advanced Configuration

#### Resource Type Selection
```yaml
resourceTypes:
  - Patient
  - Encounter
  - Observation
  - Condition
  - Procedure
  - MedicationRequest
  - Practitioner
  - Organization
```

#### Tag Configuration
```yaml
tagConfiguration:
  enablePHITags: true  # Automatic PHI classification
  enableClinicalTags: true  # Clinical category tags
  enableDataQualityTests: true  # Healthcare data quality validation
  customClassifications:
    - name: "Sensitive"
      description: "Highly sensitive medical data"
      tags:
        - name: "Mental Health"
          description: "Mental health related data"
        - name: "Substance Use"
          description: "Substance use disorder data"
```

#### Filtering
```yaml
sourceConfig:
  config:
    schemaFilterPattern:
      includes:
        - "Clinical"
        - "Administrative"
      excludes:
        - "Archived"
    tableFilterPattern:
      includes:
        - "Patient"
        - "Encounter"
      excludes:
        - ".*Test.*"
```

## Entity Mapping

The connector maps FHIR resources to OpenMetadata entities as follows:

- **Database**: Epic Organization (e.g., "epic_fhir")
- **Schema**: FHIR Resource Categories
  - Clinical (Patient, Observation, Condition, etc.)
  - Administrative (Practitioner, Organization, Location)
  - Financial (Coverage, Claim)
  - Workflow (Appointment, Schedule)
- **Table**: Individual FHIR Resources (Patient, Encounter, etc.)
- **Column**: FHIR Resource Attributes with appropriate data types

## Data Quality Tests

When `enableDataQualityTests` is enabled, the connector runs healthcare-specific validations:

1. **Patient Identifier Validation**: Ensures patients have valid identifiers including MRN
2. **Date Consistency**: Validates temporal relationships (e.g., death after birth)
3. **Coding Standards**: Verifies use of standard medical coding systems
4. **Reference Integrity**: Validates FHIR resource references
5. **PHI Completeness**: Checks for required PHI fields
6. **Value Range Validation**: Ensures clinical observations are within reasonable ranges

## Running the Connector

### Command Line
```bash
metadata ingest -c epic_workflow.yaml
```

### Programmatically
```python
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.workflow_output_handler import print_status

config = """
source:
  type: epic
  serviceName: epic_hospital
  serviceConnection:
    config:
      type: Epic
      fhirServerUrl: https://epicserver.hospital.org/fhir
      authType: OAuth2
      # ... rest of config
"""

workflow = MetadataWorkflow.create(config)
workflow.execute()
workflow.raise_from_status()
print_status(workflow)
workflow.stop()
```

## Security Considerations

1. **Authentication**: Always use OAuth2 or SMART on FHIR in production
2. **SSL/TLS**: Enable SSL verification for secure communication
3. **Data Masking**: Enable `enableDataMasking` to protect PHI
4. **Access Control**: Configure appropriate scopes for least privilege access
5. **Audit Logging**: Monitor connector activity for compliance

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify client credentials
   - Check token URL and scopes
   - Ensure Epic app is properly configured

2. **Resource Not Found**
   - Verify FHIR server URL includes `/fhir` suffix
   - Check resource types are supported by Epic instance
   - Validate organization ID if filtering is enabled

3. **Performance Issues**
   - Adjust `pageSize` (default: 100)
   - Increase `requestTimeout` for slow connections
   - Filter resource types to reduce scope

### Debug Mode
Enable debug logging in workflow configuration:
```yaml
workflowConfig:
  loggerLevel: DEBUG
```

## Compliance Notes

- The connector is designed to handle PHI in compliance with HIPAA
- Enable data masking and encryption for production use
- Implement appropriate access controls in OpenMetadata
- Regular audit of data access and modifications is recommended

## Support

For issues or questions:
1. Check Epic's FHIR API documentation
2. Review OpenMetadata logs
3. Contact OpenMetadata support with debug logs