import json
import os
from jsonschema import validate, ValidationError

def load_json_file(file_path):
    """Load and parse a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path}: {e}")
        return None

def main():
    # Load schema and sample from separate files
    schema_path = "schemas/bugsnag_webhook_schema.json"
    sample_path = "samples/bugsnag_webhook_sample.json"
    
    # Check if files exist
    if not os.path.exists(schema_path):
        print(f"Schema file not found: {schema_path}")
        return
    
    if not os.path.exists(sample_path):
        print(f"Sample file not found: {sample_path}")
        return
    
    # Load the schema and sample
    schema = load_json_file(schema_path)
    sample_payload = load_json_file(sample_path)
    
    if schema is None or sample_payload is None:
        print("Failed to load schema or sample payload")
        return
    
    # Perform the validation
    try:
        validate(instance=sample_payload, schema=schema)
        print("✅ Validation successful: The sample payload conforms to the JSON schema.")
    except ValidationError as e:
        print("❌ Validation failed:")
        print(f"  Path: {' -> '.join(str(p) for p in e.path)}")
        print(f"  Message: {e.message}")
        print(f"  Schema: {e.schema_path}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()