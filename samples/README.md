# Sample Data

This directory contains sample JSON payloads and test data for various webhook integrations.

## Files

### `bugsnag_webhook_sample.json`
A sample Bugsnag webhook payload that conforms to the JSON schema defined in `schemas/bugsnag_webhook_schema.json`.

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

# Use for testing or as a template
```

## Testing

The sample payload is used by `test_json_schema.py` to validate that the JSON schema is working correctly.

Run the test:
```bash
uv run python test_json_schema.py
```
