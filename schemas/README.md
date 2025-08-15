# JSON Schemas

This directory contains JSON schema definitions for validating webhook payloads and other data structures.

## Files

### `bugsnag_webhook_schema.json`
JSON Schema for validating Bugsnag webhook payloads. This schema defines the structure and validation rules for incoming Bugsnag error reports.

**Key Features:**
- Validates required fields: `account`, `project`, `trigger`, `error`, `event`
- Enforces data types and formats (URIs, email addresses, date-times)
- Defines nested object structures for complex data
- Supports optional fields like `createdIssue`, `user`, `app`, `device`

**Usage:**
```python
from jsonschema import validate
import json

# Load schema
with open('schemas/bugsnag_webhook_schema.json', 'r') as f:
    schema = json.load(f)

# Validate payload
validate(instance=payload, schema=schema)
```

## Testing

Run the validation test:
```bash
uv run python test_json_schema.py
```

This will validate the sample payload in `samples/bugsnag_webhook_sample.json` against the schema.
