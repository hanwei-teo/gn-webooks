# Sample Data

This directory contains sample JSON payloads and test data for various webhook integrations.

## Files

### `bugsnag_webhook_sample.json`
A sample Bugsnag webhook payload that conforms to the AVRO schema defined in `bugsnag_avro_schema.json`.

**Structure:**
- `account`: Company/account information
- `project`: Project details
- `trigger`: What triggered the webhook
- `error`: Error information and metadata
- `event`: Detailed event data including user, app, device, and exception details

**Usage:**
```python
import json

# Load sample payload
with open('samples/bugsnag_webhook_sample.json', 'r') as f:
    sample_payload = json.load(f)

# Use for testing or as a template for AVRO validation
```

## Testing

The sample payload is used by `test_avro_schema_validation.py` to validate that the AVRO schema is working correctly.

Run the test:
```bash
uv run python test_avro_schema_validation.py
```
