import os
import redis
import json
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer
from jsonschema import validate, ValidationError

# Configuration from Environment Variables
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'insights-testing.test-ingest.bugsnagJson')
REDIS_QUEUE = 'webhook_queue'

# JSON Schema for Bugsnag webhook payload
BUGSNAG_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Bugsnag Webhook Payload",
    "description": "JSON Schema for the Bugsnag data forwarding webhook payload",
    "type": "object",
    "required": [
        "account",
        "project",
        "trigger",
        "error",
        "event"
    ],
    "properties": {
        "account": { "$ref": "#/definitions/Account" },
        "project": { "$ref": "#/definitions/Project" },
        "trigger": { "$ref": "#/definitions/Trigger" },
        "error": { "$ref": "#/definitions/Error" },
        "event": { "$ref": "#/definitions/Event" }
    },
    "definitions": {
        "Account": {
            "type": "object",
            "required": ["id", "name", "url"],
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "url": { "type": "string", "format": "uri" }
            }
        },
        "Project": {
            "type": "object",
            "required": ["id", "name", "url"],
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "url": { "type": "string", "format": "uri" }
            }
        },
        "Trigger": {
            "type": "object",
            "required": ["type"],
            "properties": {
                "type": { "type": "string" },
                "message": { "type": "string" }
            }
        },
        "Error": {
            "type": "object",
            "required": ["id", "url", "context", "firstReceived", "severity", "status"],
            "properties": {
                "id": { "type": "string" },
                "url": { "type": "string", "format": "uri" },
                "context": { "type": "string" },
                "firstReceived": { "type": "string", "format": "date-time" },
                "severity": { "type": "string", "enum": ["error", "warning", "info"] },
                "status": { "type": "string" },
                "createdIssue": {
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "number": { "type": "integer" },
                        "type": { "type": "string" },
                        "url": { "type": "string", "format": "uri" }
                    }
                }
            }
        },
        "Event": {
            "type": "object",
            "required": ["id", "received", "exceptions"],
            "properties": {
                "id": { "type": "string" },
                "received": { "type": "string", "format": "date-time" },
                "user": { "$ref": "#/definitions/User" },
                "app": { "$ref": "#/definitions/App" },
                "device": { "$ref": "#/definitions/Device" },
                "exceptions": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/Exception" }
                }
            }
        },
        "User": {
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "name": { "type": "string" },
                "email": { "type": "string", "format": "email" }
            }
        },
        "App": {
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "version": { "type": "string" },
                "versionCode": { "type": "integer" },
                "releaseStage": { "type": "string" }
            }
        },
        "Device": {
            "type": "object",
            "properties": {
                "hostname": { "type": "string" },
                "id": { "type": "string" },
                "manufacturer": { "type": "string" },
                "model": { "type": "string" },
                "osName": { "type": "string" },
                "osVersion": { "type": "string" }
            }
        },
        "Exception": {
            "type": "object",
            "required": ["errorClass", "stacktrace"],
            "properties": {
                "errorClass": { "type": "string" },
                "message": { "type": "string" },
                "stacktrace": {
                    "type": "array",
                    "items": { "$ref": "#/definitions/StackFrame" }
                }
            }
        },
        "StackFrame": {
            "type": "object",
            "required": ["file", "lineNumber", "method"],
            "properties": {
                "file": { "type": "string" },
                "lineNumber": { "type": "integer" },
                "method": { "type": "string" }
            }
        }
    }
}

class JSONSchemaKafkaProducer:
    """
    Kafka producer with JSON Schema validation and JSON serialization.
    """
    def __init__(self):
        if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD]):
            raise ValueError("Required environment variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD")
        
        print("🚀 Starting JSON Schema Kafka Producer...")
        print(f"✓ Kafka Topic: {KAFKA_TOPIC}")
        print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"✓ Using JSON Schema validation")
        
        # Initialize Redis
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        
        # Initialize Kafka Producer
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
            # Validate message against JSON Schema
            validate(instance=message_data, schema=BUGSNAG_SCHEMA)
            
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
        producer = JSONSchemaKafkaProducer()
        producer.run()
    except KeyboardInterrupt:
        print("\n🛑 Shutting down producer...")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        exit(1)
