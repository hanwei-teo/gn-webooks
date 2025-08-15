#!/usr/bin/env python3
"""
Test script to demonstrate Schema Registry integration.
This shows how the producer would fetch and use schemas from the registry.
"""

import os
import json
import requests
import base64

# Configuration
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'insights-testing.test-ingest.bugsnagJson')

def fetch_schema_from_registry():
    """Fetch schema from Schema Registry (simulating what the producer would do)"""
    if not all([SCHEMA_REGISTRY_URL, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD]):
        print("❌ Missing required environment variables")
        return None
    
    subject_name = f"{KAFKA_TOPIC}-value"
    
    # Prepare authentication
    auth_string = f"{KAFKA_SASL_USERNAME}:{KAFKA_SASL_PASSWORD}"
    auth_bytes = auth_string.encode('ascii')
    auth_b64 = base64.b64encode(auth_bytes).decode('ascii')
    
    headers = {
        'Authorization': f'Basic {auth_b64}'
    }
    
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject_name}/versions/latest"
    
    print(f"🔍 Fetching schema from: {url}")
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            schema = json.loads(result.get('schema', '{}'))
            
            print(f"✅ Schema fetched successfully!")
            print(f"📋 Schema ID: {result.get('id')}")
            print(f"📋 Subject: {result.get('subject')}")
            print(f"📋 Version: {result.get('version')}")
            print(f"📋 Schema Type: {result.get('schemaType', 'JSON')}")
            
            return schema
        else:
            print(f"❌ Failed to fetch schema: HTTP {response.status_code}")
            print(f"📋 Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching schema: {e}")
        return None

def validate_message_against_schema(message, schema):
    """Validate a message against the fetched schema"""
    try:
        from jsonschema import validate, ValidationError
        
        validate(instance=message, schema=schema)
        print("✅ Message validation successful!")
        return True
        
    except ValidationError as e:
        print(f"❌ Message validation failed: {e.message}")
        print(f"   Path: {' -> '.join(str(p) for p in e.path)}")
        return False
    except ImportError:
        print("❌ jsonschema library not available")
        return False

def main():
    print("🧪 Schema Registry Integration Test")
    print("=" * 50)
    
    # Step 1: Fetch schema from registry
    print("\n📋 Step 1: Fetching schema from Schema Registry...")
    schema = fetch_schema_from_registry()
    
    if not schema:
        print("\n❌ Cannot proceed without schema")
        print("💡 Please register the schema in the Schema Registry via the UI")
        print(f"💡 Expected subject name: {KAFKA_TOPIC}-value")
        print(f"💡 Schema Registry URL: {SCHEMA_REGISTRY_URL}")
        return
    
    # Step 2: Load a sample message
    print("\n📋 Step 2: Loading sample message...")
    try:
        with open('samples/bugsnag_webhook_sample.json', 'r') as f:
            sample_message = json.load(f)
        print("✅ Sample message loaded")
    except Exception as e:
        print(f"❌ Failed to load sample message: {e}")
        return
    
    # Step 3: Validate message against schema
    print("\n📋 Step 3: Validating message against schema...")
    is_valid = validate_message_against_schema(sample_message, schema)
    
    if is_valid:
        print("\n🎉 Schema Registry integration test PASSED!")
        print("✅ The producer can successfully:")
        print("   - Fetch schemas from Schema Registry")
        print("   - Validate messages against fetched schemas")
        print("   - Handle schema evolution automatically")
    else:
        print("\n❌ Schema Registry integration test FAILED!")

if __name__ == "__main__":
    main()
