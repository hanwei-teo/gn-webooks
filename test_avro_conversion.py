#!/usr/bin/env python3
"""
Test script to convert JSON to AVRO and read with pyarrow.
Reads sample JSON input, converts to AVRO format using pyarrow, then reads back.
"""

import json
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import tempfile
import os

def read_sample_json():
    """Read the sample Bugsnag webhook JSON"""
    sample_path = Path("samples/bugsnag_webhook_sample.json")
    
    if not sample_path.exists():
        print(f"❌ Sample file not found: {sample_path}")
        return None
    
    try:
        with open(sample_path, 'r') as f:
            data = json.load(f)
        print(f"✅ Read sample JSON from {sample_path}")
        return data
    except Exception as e:
        print(f"❌ Error reading sample JSON: {e}")
        return None

def convert_json_to_avro_with_pyarrow(json_data, output_path):
    """Convert JSON data to AVRO format using pyarrow"""
    try:
        # Convert JSON to pyarrow table
        # Flatten the nested structure for easier handling
        flattened_data = flatten_json(json_data)
        
        # Create schema from flattened data
        schema_fields = []
        for key, value in flattened_data.items():
            if isinstance(value, str):
                schema_fields.append((key, pa.string()))
            elif isinstance(value, int):
                schema_fields.append((key, pa.int64()))
            elif isinstance(value, float):
                schema_fields.append((key, pa.float64()))
            elif isinstance(value, bool):
                schema_fields.append((key, pa.bool_()))
            else:
                schema_fields.append((key, pa.string()))  # Default to string
        
        schema = pa.schema(schema_fields)
        
        # Create pyarrow table with schema
        # Convert flattened data to column format
        data = {}
        for key, value in flattened_data.items():
            data[key] = [value]
        
        table = pa.table(data, schema=schema)
        
        # Write to AVRO format
        with pa.OSFile(output_path, 'wb') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)
        
        print(f"✅ Converted JSON to AVRO using pyarrow: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Error converting to AVRO: {e}")
        return False

def flatten_json(data, prefix=''):
    """Flatten nested JSON structure for easier table creation"""
    flattened = {}
    
    for key, value in data.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            flattened.update(flatten_json(value, new_key))
        elif isinstance(value, list):
            # Handle arrays by taking the first element or converting to string
            if value and isinstance(value[0], dict):
                # For complex arrays, flatten the first element
                flattened.update(flatten_json(value[0], f"{new_key}[0]"))
            else:
                flattened[new_key] = str(value)
        else:
            flattened[new_key] = value
    
    return flattened

def read_avro_with_pyarrow(avro_path):
    """Read AVRO file with pyarrow and return as dataframe"""
    try:
        # Read AVRO file with pyarrow using RecordBatchFileReader
        with pa.OSFile(avro_path, 'rb') as source:
            reader = pa.RecordBatchFileReader(source)
            table = reader.read_all()
        
        # Convert to pandas dataframe
        df = table.to_pandas()
        
        print(f"✅ Read AVRO with pyarrow: {avro_path}")
        print(f"📊 DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        print(f"❌ Error reading AVRO with pyarrow: {e}")
        return None

def create_simple_avro_test():
    """Create a simpler AVRO test with basic data structure"""
    try:
        # Create simple test data with flattened structure
        test_data = [
            {
                "id": "123",
                "name": "Test Error",
                "severity": "error",
                "timestamp": "2025-08-15T10:00:00Z",
                "user_id": "user123",
                "user_name": "John Doe",
                "user_email": "john@example.com"
            }
        ]
        
        # Convert to pyarrow table with explicit schema
        schema = pa.schema([
            ('id', pa.string()),
            ('name', pa.string()),
            ('severity', pa.string()),
            ('timestamp', pa.string()),
            ('user_id', pa.string()),
            ('user_name', pa.string()),
            ('user_email', pa.string())
        ])
        
        # Extract data as lists for each column
        data = {
            'id': [test_data[0]['id']],
            'name': [test_data[0]['name']],
            'severity': [test_data[0]['severity']],
            'timestamp': [test_data[0]['timestamp']],
            'user_id': [test_data[0]['user_id']],
            'user_name': [test_data[0]['user_name']],
            'user_email': [test_data[0]['user_email']]
        }
        
        table = pa.table(data, schema=schema)
        
        # Write to AVRO
        with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as tmp_file:
            avro_path = tmp_file.name
        
        with pa.OSFile(avro_path, 'wb') as sink:
            with pa.RecordBatchFileWriter(sink, table.schema) as writer:
                writer.write_table(table)
        
        print(f"✅ Created simple AVRO test file: {avro_path}")
        return avro_path
    except Exception as e:
        print(f"❌ Error creating simple AVRO test: {e}")
        return None

def main():
    """Main test function"""
    print("🚀 AVRO Conversion Test with PyArrow")
    print("=" * 50)
    
    # Step 1: Read sample JSON
    json_data = read_sample_json()
    if not json_data:
        return
    
    # Step 2: Create simple AVRO test first
    print("\n📝 Creating simple AVRO test...")
    simple_avro_path = create_simple_avro_test()
    if simple_avro_path:
        # Step 3: Read simple AVRO with pyarrow
        df_simple = read_avro_with_pyarrow(simple_avro_path)
        if df_simple is not None:
            print("\n📋 Simple DataFrame Preview:")
            print("=" * 50)
            print(df_simple.head())
            print(f"\n📊 Simple DataFrame Info:")
            print(df_simple.info())
        
        # Cleanup simple test
        try:
            os.unlink(simple_avro_path)
            print(f"\n🧹 Cleaned up simple test file: {simple_avro_path}")
        except:
            pass
    
    # Step 4: Try complex JSON to AVRO conversion
    print("\n📝 Converting complex JSON to AVRO...")
    with tempfile.NamedTemporaryFile(suffix='.avro', delete=False) as tmp_file:
        complex_avro_path = tmp_file.name
    
    if convert_json_to_avro_with_pyarrow(json_data, complex_avro_path):
        # Step 5: Read complex AVRO with pyarrow
        df_complex = read_avro_with_pyarrow(complex_avro_path)
        if df_complex is not None:
            print("\n📋 Complex DataFrame Preview:")
            print("=" * 50)
            print(df_complex.head())
            
            print("\n📊 Complex DataFrame Info:")
            print("=" * 50)
            print(df_complex.info())
            
            print("\n🔍 Sample Data:")
            print("=" * 50)
            # Display column names
            print("Columns:", list(df_complex.columns))
            
            # Display first row
            if len(df_complex) > 0:
                print("\nFirst row data:")
                for col in df_complex.columns:
                    print(f"  {col}: {df_complex[col].iloc[0]}")
        
        # Cleanup complex test
        try:
            os.unlink(complex_avro_path)
            print(f"\n🧹 Cleaned up complex test file: {complex_avro_path}")
        except:
            pass
    
    print("\n✅ Test completed!")

if __name__ == "__main__":
    main()
