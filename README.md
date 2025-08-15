# GN Webhooks Pipeline

A production-ready webhook processing pipeline that receives webhooks, queues them in Redis, and streams them to Kafka/StreamNative with AVRO Schema Registry integration. Features enterprise-grade error handling with Dead Letter Queue (DLQ) for failed messages.

✅ **Production-ready**: Successfully tested with 10,000+ messages and Iceberg integration
⚡ **High throughput**: Optimized for 50+ messages/second with AVRO serialization
🛡️ **Fault tolerant**: Automatic DLQ for serialization errors and delivery failures
📊 **Schema Registry**: Dynamic AVRO schema fetching and validation

## 🏗️ Architecture

```
🌐 Internet
    ↓
📦 nginx (Load Balancer)
    ↓
🚀 FastAPI Receiver (Webhook Endpoint)
    ↓
🔴 Redis (Message Queue)
    ↓
📤 AVRO Producer (Kafka Schema Registry)
    ↓
☁️ StreamNative Kafka Cluster
    ↓
🗄️ Iceberg Data Lake
```

## 🏗️ Architecture

```
🌐 Internet
    ↓
📦 nginx (Load Balancer)
    ↓
🚀 FastAPI Receiver (Webhook Endpoint)
    ↓
🔴 Redis (Message Queue)
    ↓
📤 Schema Registry Producer (Kafka)
    ↓
☁️ StreamNative Kafka Cluster
    ↓
💀 DLQ (Failed Message Storage)
```

## 🎯 Production Features

### ✅ Core Capabilities
- **AVRO Schema Registry Integration**: Dynamic schema fetching and validation
- **High-throughput webhook processing**: 50+ msg/sec with optimized AVRO serialization
- **Dead Letter Queue**: Comprehensive error capture with detailed metadata
- **StreamNative/Pulsar**: Full Kafka producer API compatibility with JWT auth
- **Redis queue**: Efficient message buffering between receiver and producer
- **Docker deployment**: Production-ready containers with Python 3.12
- **Iceberg Integration**: Properly formatted AVRO messages for data lake ingestion

### 🔧 Production Optimizations
- **AVRO Serialization**: Kafka Schema Registry format with schema ID embedding
- **Producer batching**: LZ4 compression and optimized batch sizes
- **Non-blocking Redis operations**: Efficient message queuing
- **Error handling**: Comprehensive DLQ with detailed error tracking
- **Schema validation**: Runtime AVRO schema validation and error reporting

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.12+ (for testing tools)
- `uv` package manager (optional, for local development)

### 1. Setup Credentials
```bash
# Environment variables are loaded from .env file
# Update with your StreamNative credentials:
cp .env.example .env
# Edit .env with your actual credentials
```

### 2. Build & Start Services
```bash
# Build all Docker images
docker-compose build

# Start the pipeline
docker-compose up
```

### 3. Test the Pipeline

```bash
# Check service status
docker-compose ps

# Monitor producer logs
docker-compose logs -f producer

# Quick test - 100 messages
uv run python test_avro_end_to_end.py --count 100

# Larger test - 1,000 messages
uv run python test_avro_end_to_end.py --count 1000

# Test webhook endpoint manually
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d @samples/bugsnag_webhook_sample.json

# Check DLQ for any errors
ls -la dlq/
```

## 📊 Services

### nginx (Port 8080)
- **Role**: Load balancer and reverse proxy
- **Health Check**: `curl http://localhost:8080/health`
- **Config**: `nginx/nginx.conf`

### FastAPI Receiver
- **Role**: Webhook endpoint that queues messages to Redis
- **Endpoint**: `POST http://localhost:8080/webhook`
- **Health Check**: Built into nginx upstream

### Redis Queue
- **Role**: Message buffer between receiver and producer
- **Port**: 6379 (internal)
- **Queue**: `webhook_queue`

### AVRO Producer (Production)
- **Role**: High-throughput message processor (Redis → AVRO → Kafka)
- **Core features**: 
  - Dynamic AVRO schema fetching from Schema Registry
  - Kafka Schema Registry format serialization
  - Comprehensive DLQ for failed messages
  - JWT authentication for StreamNative/Pulsar
- **Performance optimizations**:
  - **Batching**: 128KB batch size, 5ms linger, 100 msg/batch
  - **Compression**: LZ4 for reduced bandwidth
  - **Non-blocking**: Uses `lpop` instead of `blpop`
  - **AVRO serialization**: Proper Kafka Schema Registry format
  - **Throughput**: 50+ messages/second sustained

## 🧪 Development & Testing

### Local Testing

The repository includes an end-to-end test script for local development:

```bash
# Test with different message counts
uv run python test_avro_end_to_end.py --count 100    # Quick test
uv run python test_avro_end_to_end.py --count 1000   # Medium test
uv run python test_avro_end_to_end.py --count 10000  # Large test

# Monitor test progress
docker-compose logs -f producer
```

### Test Features

- **Realistic Data**: Generates Bugsnag-like webhook payloads
- **Progress Tracking**: Real-time progress updates
- **Error Detection**: Identifies failed webhook submissions
- **Performance Metrics**: Reports throughput and success rates

## 🔧 Configuration
```bash
# Redis Configuration
REDIS_HOST=redis

# Kafka Configuration  
KAFKA_BOOTSTRAP_SERVERS=your-kafka-cluster:9093
KAFKA_SASL_USERNAME=user
KAFKA_SASL_PASSWORD=your_jwt_token_here
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_TOPIC=your-topic-name

# Schema Registry Configuration
SCHEMA_REGISTRY_URL=https://your-schema-registry/kafka

# Kafka Producer Performance Configuration
KAFKA_BATCH_SIZE=131072              # 128KB batch size
KAFKA_LINGER_MS=5                    # 5ms linger time
KAFKA_COMPRESSION_TYPE=lz4           # Compression algorithm
KAFKA_BATCH_NUM_MESSAGES=100         # Messages per batch
KAFKA_ACKS=1                         # Acknowledgment level
KAFKA_RETRIES=3                      # Retry attempts
KAFKA_MAX_IN_FLIGHT=10               # Max in-flight requests
KAFKA_QUEUE_MAX_MESSAGES=10000       # Max queued messages
KAFKA_QUEUE_MAX_KBYTES=32768         # Max queue size in KB
KAFKA_SOCKET_SEND_BUFFER=262144      # Socket send buffer
KAFKA_SOCKET_RECEIVE_BUFFER=131072   # Socket receive buffer
KAFKA_ENABLE_IDEMPOTENCE=false       # Enable exactly-once delivery
```

### StreamNative Integration
The pipeline is configured for StreamNative (Pulsar KoP) with:
- **SASL_SSL** security protocol
- **JWT token authentication**
- **Schema registry** for AVRO schemas
- **Topic**: `insights-testing.test-ingest.bugsnag`

## 🐛 Dead Letter Queue (DLQ)

### Automatic Error Handling
Failed messages are automatically saved to local files:
- **Schema validation failures**
- **AVRO serialization errors** 
- **Kafka producer errors**

### DLQ File Structure
```json
{
  "timestamp": "2025-01-12T10:30:45.123456",
  "topic": "insights-testing.test-ingest.bugsnag", 
  "error_reason": "Serialization failed: AvroTypeException - Field missing",
  "original_message": { ... }
}
```

### DLQ Management
```bash
# View all DLQ files with analysis
./view_dlq.py

# View specific DLQ file
./view_dlq.py --file dlq/dlq_topic_timestamp.json

# Clear all DLQ files
./view_dlq.py --clear
```

### DLQ Statistics
The producer shows DLQ stats when closing:
```
📊 DLQ Statistics:
   Total failed messages: 5
   Error breakdown:
     - Serialization failed: 3
     - Failed to create serializer: 2
```

## 🧪 Testing

### Bugsnag Webhook Simulator
Generates schema-compliant Bugsnag webhook events:
- **Error types**: TypeError, ReferenceError, ValidationError, NetworkError, etc.
- **Schema format**: Flat structure with JSON-encoded complex fields
- **Invalid messages**: Configurable percentage for DLQ testing
- **Performance**: Capable of generating 1000+ events/second

### Test Commands
```bash
# Basic test suite (10 events)
./run_webhook_test.sh

# Custom test parameters
python3 test_webhook.py --count 50 --delay 0.1

# Single event with full JSON output
python3 test_webhook.py --single

# Custom webhook endpoint
python3 test_webhook.py --url http://production.example.com/webhook
```

### Test Output
```
🐛 Starting Bugsnag webhook test suite
📡 Target URL: http://localhost:8080/webhook
📊 Error events to send: 10
⏱️  Delay between events: 1.0s
============================================================

🐛 [1/10] Sending TypeError error...
   Error: Cannot read property 'data' of undefined...
   Context: /dashboard/users
   ✅ Success! (200) - 0.045s
```

## 📁 Project Structure

```
gn-webooks/
├── docker-compose.yml           # Multi-service orchestration
├── pyproject.toml              # Python dependencies (uv native)
├── .env                         # Environment configuration
├── .env.example                 # Configuration template
├── nginx/
│   ├── Dockerfile
│   └── nginx.conf              # Load balancer config
├── receiver/
│   ├── Dockerfile
│   └── main.py                 # FastAPI webhook receiver
├── producer/
│   ├── Dockerfile
│   └── producer.py             # AVRO Kafka producer with DLQ
├── dlq/                        # Dead Letter Queue files
├── test_webhook.py             # Bugsnag webhook simulator
├── run_webhook_test.sh         # Test runner script
├── view_dlq.py                 # DLQ analysis tool
└── README.md
```

## 🔍 Monitoring & Debugging

### Service Logs
```bash
# View all service logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f producer
docker-compose logs -f receiver
```

### Health Checks
```bash
# Check nginx/receiver health
curl http://localhost:8080/health

# Check Redis connection
docker-compose exec redis redis-cli ping

# Check producer DLQ stats
docker-compose logs producer | grep "DLQ Statistics"
```

### Message Monitoring in StreamNative

⚠️ **Note**: Kafka consumers are NOT compatible with StreamNative/Pulsar due to protocol incompatibilities.
StreamNative's Kafka protocol handler doesn't fully support the Kafka consumer API.

For monitoring messages in StreamNative/Pulsar, use:
1. Pulsar's native client libraries
2. StreamNative's web console or CLI tools
3. Producer-side metrics and logs (as shown in this project)

### DLQ Monitoring
```bash
# Watch DLQ in real-time
watch -n 2 './view_dlq.py'

# Monitor DLQ directory
ls -la dlq/

# Count failed messages
ls dlq/dlq_*.json 2>/dev/null | wc -l
```

## 🚀 Production Considerations

### Performance Tuning
- **nginx**: Configured for high-throughput webhook processing
- **Redis**: Single instance suitable for moderate loads
- **Producer**: Optimized with batching, compression, and connection pooling
- **AVRO**: Schema caching for improved serialization performance

### Scaling
- **Horizontal**: Add more receiver replicas
- **Vertical**: Increase container resources
- **Redis**: Consider Redis Cluster for high availability
- **Producer**: Multiple producer instances for higher throughput

### Security
- **Environment variables**: Sensitive data in `.env` (not committed)
- **JWT authentication**: Secure Kafka connection
- **Network isolation**: Services communicate via Docker network

## 📝 Development

### Adding New Event Types
1. Update webhook generator in `test_webhook.py`
2. Ensure AVRO schema exists in registry
3. Test with DLQ to catch validation issues

### Schema Evolution
- AVRO schemas support backward/forward compatibility
- Producer automatically fetches latest schemas
- Failed messages preserved in DLQ for reprocessing

### Dependency Management
Dependencies are managed using uv's native `pyproject.toml` format with dependency groups:

```toml
[dependency-groups]
producer = ["confluent-kafka==2.3.0", "redis==4.3.4", ...]
receiver = ["fastapi==0.104.1", "uvicorn==0.24.0", ...]
test = ["requests==2.31.0", "faker==20.1.0"]
```

### Local Development
```bash
# Run tests with automatic dependency management
uv run --group test test_webhook.py

# Install and run dependencies for development
uv run --group dev python3 script.py

# Run individual services for debugging
docker-compose up redis
uv run --group receiver python3 receiver/main.py
uv run --group producer python3 producer/producer.py
```

## 🎯 Use Cases

- **Error Monitoring**: Bugsnag, Sentry webhook processing
- **Analytics Events**: User behavior, product analytics
- **System Integration**: Microservice event streaming
- **Data Pipeline**: ETL with AVRO schema validation
- **Audit Logging**: Compliance and monitoring webhooks

## 🔀 Multi-Topic Support

The pipeline supports routing webhooks to different Kafka topics based on various criteria.
See [Multi-Topic Guide](docs/MULTI_TOPIC_GUIDE.md) for:
- Path-based routing (`/webhook/bugsnag` → `production.insights.bugsnag`)
- Header-based routing (`X-Webhook-Source: sentry`)
- Smart content-based routing
- Configuration examples
- Migration strategies

---

**Built with ❤️ using FastAPI, Redis, Kafka, and AVRO**
