import os
import redis
import json
import time
import uuid
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# Configuration from Environment Variables
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SASL_USERNAME = os.environ.get('KAFKA_SASL_USERNAME')
KAFKA_SASL_PASSWORD = os.environ.get('KAFKA_SASL_PASSWORD')
KAFKA_SASL_MECHANISM = os.environ.get('KAFKA_SASL_MECHANISM', 'PLAIN')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'webhooks')
SCHEMA_REGISTRY_URL = os.environ.get('SCHEMA_REGISTRY_URL')
REDIS_QUEUE = 'webhook_queue'

class AVROKafkaProducer:
    """
    Kafka producer with AVRO serialization and schema registry support.
    """
    def __init__(self):
        if not all([KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL]):
            raise ValueError("Required environment variables: KAFKA_BOOTSTRAP_SERVERS, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD, SCHEMA_REGISTRY_URL")
        
        print("🚀 Starting AVRO Kafka Producer...")
        print(f"✓ Kafka Topic: {KAFKA_TOPIC}")
        print(f"✓ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"✓ Connected to Schema Registry at {SCHEMA_REGISTRY_URL}")
        
        # Initialize Redis
        self.redis = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
        
        # Initialize Schema Registry Client
        self.schema_registry_client = SchemaRegistryClient({
            'url': SCHEMA_REGISTRY_URL,
            'basic.auth.user.info': f'{KAFKA_SASL_USERNAME}:{KAFKA_SASL_PASSWORD}'
        })
        
        # Fetch schema for the topic
        try:
            subject_name = f"{KAFKA_TOPIC}-value"
            latest_schema = self.schema_registry_client.get_latest_version(subject_name)
            print(f"📋 Fetched schema for topic '{KAFKA_TOPIC}' (version {latest_schema.version})")
            
            # Create serializer
            def webhook_to_dict(webhook_data, ctx):
                if isinstance(webhook_data, dict):
                    return webhook_data
                return json.loads(webhook_data)
            
            self.serializer = AvroSerializer(
                self.schema_registry_client,
                latest_schema.schema.schema_str,
                to_dict=webhook_to_dict
            )
            
        except Exception as e:
            print(f"❌ Failed to fetch schema for topic '{KAFKA_TOPIC}': {e}")
            raise
        
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
        """Send a message to Kafka"""
        try:
            # Create serialization context
            ctx = SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            
            # Serialize the message
            serialized_data = self.serializer(message_data, ctx)
            
            # Send to Kafka
            self.producer.produce(
                topic=KAFKA_TOPIC,
                value=serialized_data,
                on_delivery=None  # No callback for high throughput
            )
            
            self.message_count += 1
            
            # Log progress every 100 messages
            if self.message_count % 100 == 0:
                elapsed = time.time() - self.start_time
                throughput = self.message_count / elapsed if elapsed > 0 else 0
                print(f"✅ [{datetime.now().strftime('%H:%M:%S')}] Sent: {KAFKA_TOPIC} (Total: {self.message_count})")
                
                # Log throughput every 1000 messages
                if self.message_count % 1000 == 0:
                    print(f"📊 Throughput: {throughput:.2f} msg/sec (Total: {self.message_count} messages in {elapsed:.1f}s)")
            
        except Exception as e:
            self._error_count += 1
            # Log errors less frequently
            if self._error_count % 100 == 0:
                print(f"❌ Serialization error (seen {self._error_count} times): {e.__class__.__name__}")
            self.write_to_dlq(message_data, str(e))
            self.error_count += 1

    def write_to_dlq(self, message, error):
        """Write failed messages to Dead Letter Queue"""
        try:
            dlq_dir = "/app/dlq"
            os.makedirs(dlq_dir, exist_ok=True)
            
            dlq_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "topic": KAFKA_TOPIC,
                "error": str(error),
                "message": message
            }
            
            filename = f"{dlq_dir}/failed_{int(time.time())}_{uuid.uuid4().hex[:8]}.json"
            with open(filename, 'w') as f:
                json.dump(dlq_entry, f, indent=2)
            
            self.dlq_count += 1
            
        except Exception as e:
            print(f"❌ Failed to write to DLQ: {e}")

    def process_messages(self):
        """Process messages from Redis queue"""
        print(f"📨 Processing messages from '{REDIS_QUEUE}'...")
        
        batch_size = 500
        last_flush_time = time.time()
        
        try:
            while True:
                try:
                    # Get batch of messages (non-blocking)
                    messages = []
                    for _ in range(batch_size):
                        msg = self.redis.lpop(REDIS_QUEUE)
                        if msg:
                            messages.append(msg)
                            print(f"🔍 Message received: {msg}")
                        else:
                            break
                    
                    if messages:
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
        
        except KeyboardInterrupt:
            print("\n👋 Shutting down producer...")
        finally:
            # Final flush
            print("🔄 Performing final flush...")
            self.producer.flush(timeout=30)
            self.print_final_statistics()

    def print_final_statistics(self):
        """Print final statistics"""
        elapsed = time.time() - self.start_time
        throughput = self.message_count / elapsed if elapsed > 0 else 0
        
        print("\n============================================================")
        print("📊 FINAL STATISTICS")
        print("============================================================")
        print(f"Total Messages Sent: {self.message_count}")
        print(f"Total Errors: {self.error_count}")
        print(f"DLQ Messages: {self.dlq_count}")
        print(f"Average Throughput: {throughput:.2f} msg/sec")
        print(f"Running Time: {elapsed:.1f}s")
        print("============================================================")

    def get_dlq_stats(self):
        """Get DLQ statistics"""
        try:
            dlq_dir = "/app/dlq"
            if not os.path.exists(dlq_dir):
                return {"total": 0, "files": []}
            
            files = [f for f in os.listdir(dlq_dir) if f.endswith('.json')]
            return {
                "total": len(files),
                "files": files[-10:]  # Last 10 files
            }
        except Exception as e:
            return {"error": str(e)}


if __name__ == "__main__":
    producer = AVROKafkaProducer()
    producer.process_messages()
