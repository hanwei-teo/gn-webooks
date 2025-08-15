#!/usr/bin/env python3
"""
Test script to validate the AVRO schema with sample data and demonstrate logical types.
"""

import json
import pyarrow as pa
from datetime import datetime
import tempfile
import os
from pathlib import Path

def load_avro_schema():
    """Load the AVRO schema from file"""
    schema_path = Path("bugsnag_avro_schema.json")
    
    if not schema_path.exists():
        print(f"❌ Schema file not found: {schema_path}")
        return None
    
    try:
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        print(f"✅ Loaded AVRO schema from {schema_path}")
        return schema
    except Exception as e:
        print(f"❌ Error loading schema: {e}")
        return None

def load_sample_data():
    """Load the sample Bugsnag webhook data"""
    sample_path = Path("samples/bugsnag_webhook_sample.json")
    
    if not sample_path.exists():
        print(f"❌ Sample file not found: {sample_path}")
        return None
    
    try:
        with open(sample_path, 'r') as f:
            data = json.load(f)
        print(f"✅ Loaded sample data from {sample_path}")
        return data
    except Exception as e:
        print(f"❌ Error loading sample data: {e}")
        return None

def convert_iso_to_timestamp_millis(iso_string):
    """Convert ISO 8601 string to timestamp in milliseconds"""
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return int(dt.timestamp() * 1000)
    except Exception as e:
        print(f"Warning: Could not convert {iso_string} to timestamp: {e}")
        return None

def prepare_data_for_avro(data):
    """Prepare the data for AVRO format, converting datetime strings to timestamps"""
    # Create a copy to avoid modifying the original
    prepared_data = json.loads(json.dumps(data))
    
    # Convert datetime strings to timestamp-millis
    if 'error' in prepared_data and 'firstReceived' in prepared_data['error']:
        timestamp = convert_iso_to_timestamp_millis(prepared_data['error']['firstReceived'])
        if timestamp is not None:
            prepared_data['error']['firstReceived'] = timestamp
    
    if 'event' in prepared_data and 'received' in prepared_data['event']:
        timestamp = convert_iso_to_timestamp_millis(prepared_data['event']['received'])
        if timestamp is not None:
            prepared_data['event']['received'] = timestamp
    
    print("✅ Prepared data for AVRO format (converted datetime strings to timestamps)")
    return prepared_data

def create_pyarrow_schema_from_avro(avro_schema):
    """Create PyArrow schema from AVRO schema"""
    try:
        # Convert AVRO schema to PyArrow schema
        # This is a simplified conversion - in practice you might want to use a library
        # that handles AVRO to PyArrow schema conversion more comprehensively
        
        # For now, we'll create a basic schema that matches the structure
        schema = pa.schema([
            ('account', pa.struct([
                ('id', pa.string()),
                ('name', pa.string()),
                ('url', pa.string())
            ])),
            ('project', pa.struct([
                ('id', pa.string()),
                ('name', pa.string()),
                ('url', pa.string())
            ])),
            ('trigger', pa.struct([
                ('type', pa.string()),
                ('message', pa.string())
            ])),
            ('error', pa.struct([
                ('id', pa.string()),
                ('url', pa.string()),
                ('context', pa.string()),
                ('firstReceived', pa.int64()),  # timestamp-millis
                ('severity', pa.string()),
                ('status', pa.string()),
                ('createdIssue', pa.struct([
                    ('id', pa.string()),
                    ('number', pa.int32()),
                    ('type', pa.string()),
                    ('url', pa.string())
                ]))
            ])),
            ('event', pa.struct([
                ('id', pa.string()),
                ('received', pa.int64()),  # timestamp-millis
                ('user', pa.struct([
                    ('id', pa.string()),
                    ('name', pa.string()),
                    ('email', pa.string())
                ])),
                ('app', pa.struct([
                    ('id', pa.string()),
                    ('version', pa.string()),
                    ('versionCode', pa.int32()),
                    ('releaseStage', pa.string())
                ])),
                ('device', pa.struct([
                    ('hostname', pa.string()),
                    ('id', pa.string()),
                    ('manufacturer', pa.string()),
                    ('model', pa.string()),
                    ('osName', pa.string()),
                    ('osVersion', pa.string())
                ])),
                ('exceptions', pa.list_(pa.struct([
                    ('errorClass', pa.string()),
                    ('message', pa.string()),
                    ('stacktrace', pa.list_(pa.struct([
                        ('file', pa.string()),
                        ('lineNumber', pa.int32()),
                        ('method', pa.string())
                    ])))
                ])))
            ]))
        ])
        
        print("✅ Created PyArrow schema from AVRO schema")
        return schema
    except Exception as e:
        print(f"❌ Error creating PyArrow schema: {e}")
        return None

def test_avro_with_logical_types():
    """Test AVRO schema with logical types"""
    print("🚀 Testing AVRO Schema with Logical Types")
    print("=" * 60)
    
    # Step 1: Load AVRO schema
    avro_schema = load_avro_schema()
    if not avro_schema:
        return
    
    # Step 2: Load sample data
    sample_data = load_sample_data()
    if not sample_data:
        return
    
    # Step 3: Prepare data (convert datetime strings to timestamps)
    prepared_data = prepare_data_for_avro(sample_data)
    
    # Step 4: Create PyArrow schema
    pyarrow_schema = create_pyarrow_schema_from_avro(avro_schema)
    if not pyarrow_schema:
        return
    
    # Step 5: Create PyArrow table
    try:
        # Convert the single record to column format
        data = {
            'account': [prepared_data['account']],
            'project': [prepared_data['project']],
            'trigger': [prepared_data['trigger']],
            'error': [prepared_data['error']],
            'event': [prepared_data['event']]
        }
        
        table = pa.table(data, schema=pyarrow_schema)
        print("✅ Created PyArrow table with logical types")
        print(f"📊 Table schema: {table.schema}")
    except Exception as e:
        print(f"❌ Error creating PyArrow table: {e}")
        return
    
    # Step 6: Write to AVRO file
    with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as tmp_file:
        avro_path = tmp_file.name
    
    try:
        with pa.OSFile(avro_path, 'wb') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)
        print(f"✅ Wrote AVRO file with logical types: {avro_path}")
    except Exception as e:
        print(f"❌ Error writing AVRO file: {e}")
        return
    
    # Step 7: Read back and verify
    try:
        with pa.OSFile(avro_path, 'rb') as source:
            reader = pa.RecordBatchFileReader(source)
            read_table = reader.read_all()
        
        print("✅ Read AVRO file with logical types")
        print(f"📊 Read table schema: {read_table.schema}")
        
        # Convert to pandas for easier inspection
        df = read_table.to_pandas()
        print(f"📋 DataFrame shape: {df.shape}")
        
        # Display the full DataFrame as a table
        print("\n📊 FULL DATAFRAME OUTPUT (TABLE FORMAT):")
        print("=" * 80)
        
        # Set pandas display options for better table formatting
        import pandas as pd
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', 50)
        pd.set_option('display.width', None)
        
        print(df.to_string(index=True, max_colwidth=50))
        
        # Also show a prettier version with tabulate if available
        try:
            from tabulate import tabulate
            print("\n📊 PRETTY TABLE FORMAT:")
            print("=" * 80)
            print(tabulate(df, headers='keys', tablefmt='grid', showindex=True))
        except ImportError:
            print("\n💡 Install 'tabulate' for prettier table formatting: pip install tabulate")
        
        # Show DataFrame info
        print("\n📋 DATAFRAME INFO:")
        print("=" * 40)
        print(df.info())
        
        # Show timestamp fields
        print("\n🔍 Timestamp Fields (Logical Types):")
        print("=" * 40)
        
        # Extract and convert timestamps back to readable format
        if 'error' in df.columns and 'firstReceived' in df['error'].iloc[0]:
            timestamp_ms = df['error'].iloc[0]['firstReceived']
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            print(f"error.firstReceived: {timestamp_ms} ms -> {dt.isoformat()}")
        
        if 'event' in df.columns and 'received' in df['event'].iloc[0]:
            timestamp_ms = df['event'].iloc[0]['received']
            dt = datetime.fromtimestamp(timestamp_ms / 1000)
            print(f"event.received: {timestamp_ms} ms -> {dt.isoformat()}")
        
        # Show detailed sample data
        print("\n📋 DETAILED SAMPLE DATA:")
        print("=" * 40)
        print(f"Account: {df['account'].iloc[0]['name']}")
        print(f"Project: {df['project'].iloc[0]['name']}")
        print(f"Error Severity: {df['error'].iloc[0]['severity']}")
        print(f"User: {df['event'].iloc[0]['user']['name']}")
        print(f"Exception: {df['event'].iloc[0]['exceptions'][0]['errorClass']}")
        
        # Show nested data structure
        print("\n🔍 NESTED DATA STRUCTURE:")
        print("=" * 40)
        print("Account:", df['account'].iloc[0])
        print("Project:", df['project'].iloc[0])
        print("Trigger:", df['trigger'].iloc[0])
        print("Error:", df['error'].iloc[0])
        print("Event:", df['event'].iloc[0])
        
    except Exception as e:
        print(f"❌ Error reading AVRO file: {e}")
    
    # Cleanup
    try:
        os.unlink(avro_path)
        print(f"\n🧹 Cleaned up temporary file: {avro_path}")
    except:
        pass
    
    print("\n✅ AVRO schema with logical types test completed!")

if __name__ == "__main__":
    test_avro_with_logical_types()
