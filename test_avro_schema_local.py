#!/usr/bin/env python3
"""
Local test to validate AVRO schema against sample data.
"""

import json
import requests
from datetime import datetime
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Configuration
SCHEMA_REGISTRY_URL = "https://pc-28a787d5.aws-usw2-production-bbgdi.aws.snio.cloud/kafka"
KAFKA_SASL_USERNAME = "user"
KAFKA_SASL_PASSWORD = "eyJhbGciOiJSUzI1NiIsImtpZCI6IjllNjQzNzgxLTZiMmItNWNkNC1iMDMzLTMwYjQ4ZTAzNGUzMCIsInR5cCI6IkpXVCJ9.eyJhdWQiOlsidXJuOnNuOnB1bHNhcjpvLWo4am5pOmluc2lnaHRzLXVyc2EiXSwiZXhwIjoxNzYyNzcyNDMxLCJodHRwczovL3N0cmVhbW5hdGl2ZS5pby9zY29wZSI6WyJhY2Nlc3MiXSwiaHR0cHM6Ly9zdHJlYW1uYXRpdmUuaW8vdXNlcm5hbWUiOiJsei1pbnNpZ2h0c0BvLWo4am5pLmF1dGguc3RyZWFtbmF0aXZlLmNsb3VkIiwiaWF0IjoxNzU0OTkyODMxLCJpc3MiOiJodHRwczovL3BjLTI4YTc4N2Q1LmF3cy11c3cyLXByb2R1Y3Rpb24tYmJnZGkuYXdzLnNuaW8uY2xvdWQvYXBpa2V5cy8iLCJqdGkiOiI3ZmUyNzMxMWM4NTM0YmU5OGEyZTYzZWY0NmUzODcwMiIsInBlcm1pc3Npb25zIjpbXSwic3ViIjoiNloyWE4xbmdpVjQxbWRMUEk2RnJrajM0cTRrcjJmR1JAY2xpZW50cyJ9.sCu-I8G5KIYqh-iUReM9Q16TlzMCwxmPYAiS1hejBfm6jBZWXFYkQN3j1atKdGtysqtUgvoWHB1SiBkiUJ84xwNkvkc03UvTlDqP989_xto7vRSZx5TzBrL_N4xWh4okPvg3dQCPNxJ2f5F8S16mckTjRJpRH4ZfPI_scfFswJ-zK2XVKlJG_usVVTRVnW0Kh7pbUYfbCpgs1cYsh-E_l1LlNDdjwDJ9MzYdmyvz_bqRBgnckVsZMBh4t1HR3KPT-79tPsQB3-bwOVWwM916JO66vw0_Ncf9HEz5jVpWnNsuB9HqdM34stfgb6f6uiC_QlayexCVOnHpEbnKXLcDPw"
KAFKA_TOPIC = "insights-testing.test-ingest.bugsnag7"

def convert_iso_to_timestamp_millis(iso_string):
    """Convert ISO 8601 string to timestamp in milliseconds"""
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"Warning: Could not convert {iso_string} to timestamp: {e}")
        return int(datetime.now().timestamp() * 1000)  # Fallback to current time

def prepare_data_for_avro(message_data):
    """Prepare data for AVRO serialization"""
    # Create a copy to avoid modifying the original
    prepared_data = json.loads(json.dumps(message_data))
    
    # Convert datetime strings to timestamp-millis for AVRO logical types
    if 'error' in prepared_data and 'firstReceived' in prepared_data['error']:
        prepared_data['error']['firstReceived'] = convert_iso_to_timestamp_millis(
            prepared_data['error']['firstReceived']
        )
    
    if 'event' in prepared_data and 'received' in prepared_data['event']:
        prepared_data['event']['received'] = convert_iso_to_timestamp_millis(
            prepared_data['event']['received']
        )
    
    return prepared_data

def test_avro_schema():
    """Test AVRO schema validation"""
    print("🔍 Testing AVRO schema validation...")
    
    # Initialize Schema Registry Client
    schema_registry_client = SchemaRegistryClient({
        'url': SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': f'{KAFKA_SASL_USERNAME}:{KAFKA_SASL_PASSWORD}'
    })
    
    # Fetch AVRO schema for the topic
    subject_name = f"{KAFKA_TOPIC}-value"
    print(f"📋 Fetching schema for subject: {subject_name}")
    
    try:
        latest_schema = schema_registry_client.get_latest_version(subject_name)
        schema_str = latest_schema.schema.schema_str
        schema_id = latest_schema.schema_id
        print(f"✅ Fetched AVRO Schema (version {latest_schema.version}, ID: {schema_id})")
        print(f"📄 Schema: {schema_str[:200]}...")
        
        # Initialize AVRO Serializer
        print("🔧 Initializing AVRO serializer...")
        avro_serializer = AvroSerializer(
            schema_registry_client,
            schema_str
        )
        print("✅ AVRO serializer initialized")
        
        # Load sample data
        with open('samples/bugsnag_webhook_sample.json', 'r') as f:
            sample_data = json.load(f)
        
        print("📊 Sample data loaded")
        
        # Prepare data for AVRO
        prepared_data = prepare_data_for_avro(sample_data)
        print("🔧 Data prepared for AVRO")
        print(f"📄 Prepared data: {json.dumps(prepared_data, indent=2)[:500]}...")
        
        # Test serialization
        print("🔄 Testing AVRO serialization...")
        serialized_data = avro_serializer(prepared_data, None)
        
        if serialized_data is None:
            print("❌ AVRO serialization returned None")
            return False
        
        print(f"✅ AVRO serialization successful: {len(serialized_data)} bytes")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_avro_schema()
    if success:
        print("🎉 AVRO schema test PASSED!")
    else:
        print("💥 AVRO schema test FAILED!")
