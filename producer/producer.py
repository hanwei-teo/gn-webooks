#!/usr/bin/env python3
"""
AVRO Kafka Producer for Bugsnag webhook data.
Uses AVRO schema with logical types for datetime support.
"""

import os
import redis
import json
import time
import uuid
import sys
import io
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import fastavro

# Add debug logging
def debug_log(message):
    print(f"[DEBUG] {datetime.now().isoformat()}: {message}", flush=True)

# Configuration from Environment Variables
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'insights-testing.test-ingest.bugsnag7')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
REDIS_QUEUE = 'webhook_queue'

# Kafka Producer Configuration
KAFKA_BATCH_SIZE = int(os.environ.get('KAFKA_BATCH_SIZE', '131072'))  # 128KB
KAFKA_LINGER_MS = int(os.environ.get('KAFKA_LINGER_MS', '5'))
KAFKA_COMPRESSION_TYPE = os.environ.get('KAFKA_COMPRESSION_TYPE', 'lz4')
KAFKA_ACKS = os.environ.get('KAFKA_ACKS', '1')
KAFKA_RETRIES = int(os.environ.get('KAFKA_RETRIES', '3'))
KAFKA_MAX_IN_FLIGHT = int(os.environ.get('KAFKA_MAX_IN_FLIGHT', '10'))
KAFKA_QUEUE_MAX_MESSAGES = int(os.environ.get('KAFKA_QUEUE_MAX_MESSAGES', '10000'))
KAFKA_QUEUE_MAX_KBYTES = int(os.environ.get('KAFKA_QUEUE_MAX_KBYTES', '32768'))
KAFKA_SOCKET_SEND_BUFFER = int(os.environ.get('KAFKA_SOCKET_SEND_BUFFER', '262144'))
KAFKA_SOCKET_RECEIVE_BUFFER = int(os.environ.get('KAFKA_SOCKET_RECEIVE_BUFFER', '131072'))
KAFKA_BATCH_NUM_MESSAGES = int(os.environ.get('KAFKA_BATCH_NUM_MESSAGES', '100'))
KAFKA_ENABLE_IDEMPOTENCE = os.environ.get('KAFKA_ENABLE_IDEMPOTENCE', 'false').lower() == 'true'

class AVROKafkaProducer:
    """
    Kafka producer with AVRO serialization using Schema Registry.
    """
    def __init__(self):
        debug_log("Starting AVRO producer initialization...")
        
        # Check environment variables
        debug_log(f"Checking environment variables...")
        debug_log(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
        debug_log(f"KAFKA_SASL_USERNAME: {KAFKA_SASL_USERNAME}")
        debug_log(f"KAFKA_SASL_PASSWORD: {'*' * len(KAFKA_SASL_PASSWORD) if KAFKA_SASL_PASSWORD else 'None'}")
        debug_log(f"SCHEMA_REGISTRY_URL: {SCHEMA_REGISTRY_URL}")
        
        if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL]):
            debug_log("Missing required environment variables")
            raise ValueError("Required environment variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL")
        
        print("🚀 Starting AVRO Kafka Producer...")
        print(f"✓ Kafka Topic: {KAFKA_TOPIC}")
        print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"✓ Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
        
        # Initialize Redis
        debug_log("Initializing Redis connection...")
        try:
            self.redis = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            debug_log("Redis connection initialized successfully")
        except Exception as e:
            debug_log(f"Failed to initialize Redis: {e}")
            raise
        
        # Initialize Schema Registry Client
        debug_log("Initializing Schema Registry client...")
        try:
            self.schema_registry_client = SchemaRegistryClient({
                'url': SCHEMA_REGISTRY_URL,
                'basic.auth.user.info': f'{KAFKA_SASL_USERNAME}:{KAFKA_SASL_PASSWORD}'
            })
            debug_log("Schema Registry client initialized successfully")
        except Exception as e:
            debug_log(f"Failed to initialize Schema Registry client: {e}")
            raise
        
        # Fetch AVRO schema for the topic
        debug_log("Fetching AVRO schema from Schema Registry...")
        try:
            subject_name = f"{KAFKA_TOPIC}-value"
            debug_log(f"Looking for subject: {subject_name}")
            latest_schema = self.schema_registry_client.get_latest_version(subject_name)
            self.schema_str = latest_schema.schema.schema_str
            self.schema_id = latest_schema.schema_id
            debug_log("AVRO schema fetched and parsed successfully")
            print(f"📋 Fetched AVRO Schema for topic '{KAFKA_TOPIC}' (version {latest_schema.version})")
            print(f"📋 Schema ID: {self.schema_id}")
            
        except Exception as e:
            debug_log(f"Failed to fetch AVRO schema: {e}")
            print(f"❌ Failed to fetch AVRO schema for topic '{KAFKA_TOPIC}': {e}")
            print("💡 The AVRO schema needs to be registered in the Schema Registry")
            print(f"💡 Expected subject name: {subject_name}")
            print(f"💡 Schema Registry URL: {SCHEMA_REGISTRY_URL}")
            raise
        
        # Parse AVRO schema
        debug_log("Parsing AVRO schema...")
        try:
            self.avro_schema = json.loads(self.schema_str)
            debug_log("AVRO schema parsed successfully")
        except Exception as e:
            debug_log(f"Failed to parse AVRO schema: {e}")
            raise
        
                # Initialize Kafka Producer
        debug_log("Initializing Kafka producer...")
        try:
            producer_config = {
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'sasl.mechanism': KAFKA_SASL_MECHANISM,
                'security.protocol': 'SASL_SSL',
                'sasl.username': KAFKA_SASL_USERNAME,
                'sasl.password': f'token:{KAFKA_SASL_PASSWORD}',
                
                # Performance optimizations (configurable via environment variables)
                'batch.size': KAFKA_BATCH_SIZE,
                'linger.ms': KAFKA_LINGER_MS,
                'compression.type': KAFKA_COMPRESSION_TYPE,
                'acks': KAFKA_ACKS,
                'retries': KAFKA_RETRIES,
                'max.in.flight.requests.per.connection': KAFKA_MAX_IN_FLIGHT,
                'queue.buffering.max.messages': KAFKA_QUEUE_MAX_MESSAGES,
                'queue.buffering.max.kbytes': KAFKA_QUEUE_MAX_KBYTES,
                'socket.send.buffer.bytes': KAFKA_SOCKET_SEND_BUFFER,
                'socket.receive.buffer.bytes': KAFKA_SOCKET_RECEIVE_BUFFER,
                'batch.num.messages': KAFKA_BATCH_NUM_MESSAGES,
                'enable.idempotence': KAFKA_ENABLE_IDEMPOTENCE
            }
            
            debug_log(f"Kafka producer configuration:")
            debug_log(f"  batch.size: {KAFKA_BATCH_SIZE}")
            debug_log(f"  linger.ms: {KAFKA_LINGER_MS}")
            debug_log(f"  compression.type: {KAFKA_COMPRESSION_TYPE}")
            debug_log(f"  acks: {KAFKA_ACKS}")
            debug_log(f"  batch.num.messages: {KAFKA_BATCH_NUM_MESSAGES}")
            
            self.producer = Producer(producer_config)
            debug_log("Kafka producer initialized successfully")
        except Exception as e:
            debug_log(f"Failed to initialize Kafka producer: {e}")
            raise
        
        # Statistics
        self.message_count = 0
        self.error_count = 0
        self.dlq_count = 0
        self.start_time = time.time()
        
        print("✓ AVRO Producer initialized successfully")
        print("============================================================")

    def convert_iso_to_timestamp_millis(self, iso_string):
        """Convert ISO 8601 string to timestamp in milliseconds"""
        try:
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return int(dt.timestamp() * 1000)
        except Exception as e:
            print(f"Warning: Could not convert {iso_string} to timestamp: {e}")
            return int(time.time() * 1000)  # Fallback to current time

    def prepare_data_for_avro(self, message_data):
        """Prepare data for AVRO serialization"""
        # Create a copy to avoid modifying the original
        prepared_data = json.loads(json.dumps(message_data))
        
        # Convert datetime strings to timestamp-millis for AVRO logical types
        if 'error' in prepared_data and 'firstReceived' in prepared_data['error']:
            prepared_data['error']['firstReceived'] = self.convert_iso_to_timestamp_millis(
                prepared_data['error']['firstReceived']
            )
        
        if 'event' in prepared_data and 'received' in prepared_data['event']:
            prepared_data['event']['received'] = self.convert_iso_to_timestamp_millis(
                prepared_data['event']['received']
            )
        
        return prepared_data

    def serialize_for_kafka(self, data):
        """Serialize data for Kafka with Schema Registry format"""
        # Kafka Schema Registry format: [0] + [schema_id (4 bytes)] + [avro_data]
        buffer = io.BytesIO()
        
        # Write magic byte (0)
        buffer.write(b'\x00')
        
        # Write schema ID as 4-byte big-endian integer
        buffer.write(self.schema_id.to_bytes(4, byteorder='big'))
        
        # Write AVRO data
        fastavro.schemaless_writer(buffer, self.avro_schema, data)
        
        return buffer.getvalue()

    def send_message(self, message_data):
        """Send a message to Kafka with AVRO serialization"""
        try:
            # Prepare data for AVRO (convert timestamps)
            debug_log("Preparing data for AVRO serialization...")
            prepared_data = self.prepare_data_for_avro(message_data)
            debug_log("Data prepared successfully")
            
            # Serialize with AVRO using Kafka Schema Registry format
            debug_log("Serializing data with AVRO using Kafka Schema Registry format...")
            try:
                serialized_data = self.serialize_for_kafka(prepared_data)
                debug_log(f"Data serialized successfully: {len(serialized_data)} bytes")
            except Exception as avro_error:
                debug_log(f"AVRO serialization error: {avro_error}")
                debug_log(f"Data structure: {json.dumps(prepared_data, indent=2)[:500]}...")
                raise
            
            # Send to Kafka
            debug_log(f"Sending to Kafka topic: {KAFKA_TOPIC}")
            self.producer.produce(
                topic=KAFKA_TOPIC,
                value=serialized_data,
                callback=self.delivery_report
            )
            debug_log("Message queued for delivery")
            
            self.message_count += 1
            
            # Flush periodically
            if self.message_count % 10 == 0:  # Flush every 10 messages
                debug_log(f"Flushing producer after {self.message_count} messages")
                self.producer.flush()
                elapsed = time.time() - self.start_time
                rate = self.message_count / elapsed if elapsed > 0 else 0
                print(f"📊 Processed {self.message_count} messages ({rate:.1f} msg/s)")
            
            return True
            
        except Exception as e:
            self.error_count += 1
            debug_log(f"Error in send_message: {e}")
            print(f"❌ Error sending message: {e}")
            
            # Write to DLQ
            self.write_to_dlq(message_data, f"AVRO serialization error: {e}")
            return False

    def delivery_report(self, err, msg):
        """Delivery report callback"""
        if err is not None:
            self.error_count += 1
            debug_log(f"Message delivery failed: {err}")
            print(f"❌ Message delivery failed: {err}")
        elif msg is not None:
            # Only log delivery confirmations every 1000 messages to reduce noise
            if self.message_count % 1000 == 0:
                debug_log(f"Message delivered: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                print(f"🔍 Message sent: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
        else:
            # This shouldn't happen, but handle it gracefully
            debug_log("Delivery report called with both err and msg as None")

    def write_to_dlq(self, message_data, error_reason):
        """Write failed message to Dead Letter Queue"""
        try:
            dlq_entry = {
                "timestamp": datetime.now().isoformat(),
                "error_reason": error_reason,
                "message": message_data
            }
            
            filename = f"dlq_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
            filepath = f"/app/dlq/{filename}"
            
            with open(filepath, 'w') as f:
                json.dump(dlq_entry, f, indent=2)
            
            self.dlq_count += 1
            print(f"📝 Message written to DLQ: {filepath}")
            
        except Exception as e:
            print(f"❌ Failed to write to DLQ: {e}")

    def run(self):
        """Main processing loop"""
        print("🔄 Starting message processing loop...")
        
        try:
            while True:
                # Poll for messages from Redis
                message = self.redis.lpop(REDIS_QUEUE)
                
                if message:
                    debug_log(f"Processing message: {len(message)} bytes")
                    try:
                        # Parse JSON message
                        message_data = json.loads(message)
                        debug_log("JSON parsed successfully")
                        
                        # Send to Kafka
                        self.send_message(message_data)
                        
                    except json.JSONDecodeError as e:
                        self.error_count += 1
                        debug_log(f"Invalid JSON in message: {e}")
                        print(f"❌ Invalid JSON in message: {e}")
                        self.write_to_dlq(message, f"JSON decode error: {e}")
                    
                    except Exception as e:
                        self.error_count += 1
                        debug_log(f"Error processing message: {e}")
                        print(f"❌ Error processing message: {e}")
                        self.write_to_dlq(message, f"Processing error: {e}")
                
                else:
                    # No messages, wait a bit
                    time.sleep(0.1)
                    
        except KeyboardInterrupt:
            print("\n🛑 Shutting down AVRO producer...")
            self.producer.flush()
            
            # Print final statistics
            elapsed = time.time() - self.start_time
            print(f"\n📊 Final Statistics:")
            print(f"Total messages processed: {self.message_count}")
            print(f"Errors: {self.error_count}")
            print(f"DLQ entries: {self.dlq_count}")
            print(f"Runtime: {elapsed:.1f} seconds")
            if elapsed > 0:
                print(f"Average rate: {self.message_count / elapsed:.1f} msg/s")

if __name__ == "__main__":
    try:
        debug_log("Starting AVRO producer main execution...")
        producer = AVROKafkaProducer()
        debug_log("AVRO producer initialized, starting main loop...")
        producer.run()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down producer...")
    except Exception as e:
        debug_log(f"Fatal error in main execution: {e}")
        print(f"❌ Fatal error: {e}")
        exit(1)
