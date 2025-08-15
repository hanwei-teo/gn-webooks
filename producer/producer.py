import os
import redis
import json
import time
import uuid
import sys
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from jsonschema import validate, ValidationError

# Add debug logging
def debug_log(message):
    print(f"[DEBUG] {datetime.now().isoformat()}: {message}", flush=True)

# Configuration from Environment Variables
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'insights-testing.test-ingest.bugsnagJson')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
REDIS_QUEUE = 'webhook_queue'

class JSONSchemaRegistryKafkaProducer:
    """
    Kafka producer with JSON Schema validation fetched from Schema Registry.
    """
    def __init__(self):
        debug_log("Starting producer initialization...")
        
        # Check environment variables
        debug_log(f"Checking environment variables...")
        debug_log(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")
        debug_log(f"KAFKA_SASL_USERNAME: {KAFKA_SASL_USERNAME}")
        debug_log(f"KAFKA_SASL_PASSWORD: {'*' * len(KAFKA_SASL_PASSWORD) if KAFKA_SASL_PASSWORD else 'None'}")
        debug_log(f"SCHEMA_REGISTRY_URL: {SCHEMA_REGISTRY_URL}")
        
        if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL]):
            debug_log("Missing required environment variables")
            raise ValueError("Required environment variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL")
        
        print("🚀 Starting JSON Schema Registry Kafka Producer...")
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
        
        # Fetch schema for the topic
        debug_log("Fetching schema from Schema Registry...")
        try:
            subject_name = f"{KAFKA_TOPIC}-value"
            debug_log(f"Looking for subject: {subject_name}")
            latest_schema = self.schema_registry_client.get_latest_version(subject_name)
            self.schema = json.loads(latest_schema.schema.schema_str)
            debug_log("Schema fetched and parsed successfully")
            print(f"📋 Fetched JSON Schema for topic '{KAFKA_TOPIC}' (version {latest_schema.version})")
            print(f"📋 Schema ID: {latest_schema.schema_id}")
            
        except Exception as e:
            debug_log(f"Failed to fetch schema: {e}")
            print(f"❌ Failed to fetch schema for topic '{KAFKA_TOPIC}': {e}")
            print("💡 The schema needs to be registered in the Schema Registry via the UI")
            print(f"💡 Expected subject name: {subject_name}")
            print(f"💡 Schema Registry URL: {SCHEMA_REGISTRY_URL}")
            print("💡 Once registered, restart the producer to fetch the schema")
            raise
        
        # Initialize Kafka Producer
        debug_log("Initializing Kafka producer...")
        try:
            self.producer = Producer({
                'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
                'sasl.mechanism': KAFKA_SASL_MECHANISM,
                'security.protocol': 'SASL_SSL',
                'sasl.username': KAFKA_SASL_USERNAME,
                'sasl.password': f'token:{KAFKA_SASL_PASSWORD}',
                
                # Performance optimizations
                'batch.size': 131072,          # 128KB batch size
                'linger.ms': 5,                # 5ms linger
                'compression.type': 'lz4',     # LZ4 compression
                'acks': 1,                     # Leader acknowledgment
                'retries': 3,
                'max.in.flight.requests.per.connection': 10,
                'queue.buffering.max.messages': 10000,     # Reduced from 2M
                'queue.buffering.max.kbytes': 32768,       # 32MB buffer
                'socket.send.buffer.bytes': 262144,
                'socket.receive.buffer.bytes': 131072,
                'batch.num.messages': 100,                 # Send batches of 100 messages
                'enable.idempotence': False
            })
            debug_log("Kafka producer initialized successfully")
        except Exception as e:
            debug_log(f"Failed to initialize Kafka producer: {e}")
            raise
        
        # Statistics
        self.message_count = 0
        self.error_count = 0
        self.dlq_count = 0
        self.start_time = time.time()
        self._error_count = 0
        
        print("✓ Producer initialized successfully")
        print("============================================================")

    def send_message(self, message_data):
        """Send a message to Kafka with JSON Schema validation"""
        try:
            # Validate message against JSON Schema from Registry
            validate(instance=message_data, schema=self.schema)
            
            # Serialize to JSON
            message_json = json.dumps(message_data)
            
            # Send to Kafka
            self.producer.produce(
                topic=KAFKA_TOPIC,
                value=message_json.encode('utf-8'),
                callback=self._delivery_report
            )
            
            self.message_count += 1
            
            # Log progress every 100 messages
            if self.message_count % 100 == 0:
                elapsed = time.time() - self.start_time
                rate = self.message_count / elapsed if elapsed > 0 else 0
                print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sent: {KAFKA_TOPIC} (Total: {self.message_count})")
                print(f"📊 Rate: {rate:.1f} msg/s | Errors: {self.error_count} | DLQ: {self.dlq_count}")
            
        except ValidationError as e:
            print(f"❌ JSON Schema validation failed: {e.message}")
            print(f"   Path: {' -> '.join(str(p) for p in e.path)}")
            self._write_to_dlq(message_data, f"JSON Schema validation failed: {e.message}")
            self.error_count += 1
            
        except Exception as e:
            print(f"❌ Error sending message: {e}")
            self._write_to_dlq(message_data, f"Send error: {str(e)}")
            self.error_count += 1

    def _delivery_report(self, err, msg):
        """Callback for message delivery reports"""
        if err is not None:
            print(f"❌ Message delivery failed: {err}")
            self.error_count += 1
        else:
            # Message delivered successfully
            pass

    def _write_to_dlq(self, message_data, error_reason):
        """Write failed message to Dead Letter Queue"""
        try:
            dlq_entry = {
                "timestamp": datetime.now().isoformat(),
                "error_reason": error_reason,
                "message": message_data
            }
            
            # Create DLQ directory if it doesn't exist
            os.makedirs("/app/dlq", exist_ok=True)
            
            # Write to DLQ file
            dlq_filename = f"/app/dlq/dlq_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
            with open(dlq_filename, 'w') as f:
                json.dump(dlq_entry, f, indent=2)
            
            self.dlq_count += 1
            print(f"📝 Message written to DLQ: {dlq_filename}")
            
        except Exception as e:
            print(f"❌ Failed to write to DLQ: {e}")

    def run(self):
        """Main processing loop"""
        print("🔄 Starting message processing loop...")
        print(f"📥 Consuming from Redis queue: {REDIS_QUEUE}")
        print("=" * 60)
        
        last_flush_time = time.time()
        
        while True:
            try:
                # Get messages from Redis
                messages = []
                for _ in range(10):  # Process up to 10 messages at a time
                    msg = self.redis.lpop(REDIS_QUEUE)
                    if msg:
                        messages.append(msg)
                    else:
                        break
                
                # Process messages
                for msg in messages:
                    try:
                        # Parse JSON message
                        if isinstance(msg, str):
                            message_data = json.loads(msg)
                        else:
                            message_data = msg
                            
                        self.send_message(message_data)
                        print(f"🔍 Message sent: {message_data}")
                            
                    except json.JSONDecodeError:
                        print(f"❌ Invalid JSON: {msg[:100]}...")
                        self.error_count += 1
                    except Exception as e:
                        print(f"❌ Error processing message: {e}")
                        self.error_count += 1
                
                # Poll for delivery reports more frequently
                # This helps process callbacks and free up buffer space
                if self.message_count % 10 == 0:
                    self.producer.poll(0.01)
                
                # Periodic flush
                current_time = time.time()
                if current_time - last_flush_time > 5.0:
                    # Flush with a longer timeout to ensure messages are sent
                    remaining = self.producer.flush(timeout=5.0)
                    if remaining > 0:
                        print(f"⏱️  Periodic flush: {remaining} messages still in queue after 5s flush")
                    else:
                        print(f"✅ Periodic flush completed successfully")
                    last_flush_time = current_time
                
                # Sleep if no messages
                if not messages:
                    time.sleep(0.01)
                    
            except redis.exceptions.ConnectionError as e:
                print(f"❌ Redis connection error: {e}")
                print("⏳ Retrying in 5 seconds...")
                time.sleep(5)
                # Reconnect to Redis
                self.redis = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
                continue

if __name__ == "__main__":
    try:
        debug_log("Starting main execution...")
        producer = JSONSchemaRegistryKafkaProducer()
        debug_log("Producer initialized, starting run loop...")
        producer.run()
    except KeyboardInterrupt:
        debug_log("Received keyboard interrupt")
        print("\n🛑 Shutting down producer...")
    except Exception as e:
        debug_log(f"Fatal error: {e}")
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
