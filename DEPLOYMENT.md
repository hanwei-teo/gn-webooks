# Production Deployment Guide

## 🚀 Production Deployment

This guide covers deploying the GN Webhooks Pipeline to production environments.

## 📋 Prerequisites

- Docker & Docker Compose
- StreamNative/Pulsar cluster access
- Schema Registry access
- Network access to Kafka cluster

## 🔧 Environment Configuration

### 1. Update Environment Variables

Edit `.env` with your production credentials:

```bash
# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=your-production-kafka:9093
KAFKA_SASL_USERNAME=your-username
KAFKA_SASL_PASSWORD=your-production-jwt-token
KAFKA_TOPIC=your-production-topic

# Schema Registry
SCHEMA_REGISTRY_URL=https://your-schema-registry/kafka

# Kafka Producer Performance Configuration (Optional)
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

### 2. Schema Registration

Ensure your AVRO schema is registered in the Schema Registry:

```bash
# The schema should be registered with subject name:
# {KAFKA_TOPIC}-value
# Example: insights-testing.test-ingest.bugsnag9-value
```

## 🐳 Docker Deployment

### 1. Build Production Images

```bash
docker-compose build
```

### 2. Start Services

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Verify Deployment

```bash
# Check service health
curl http://localhost:8080/health

# Test webhook endpoint
curl -X POST http://localhost:8080/webhook \
  -H "Content-Type: application/json" \
  -d @samples/bugsnag_webhook_sample.json

# Run end-to-end test (for development environments)
uv run python test_bugsnag_end_to_end.py --count 100

# Monitor logs
docker-compose logs -f producer
```

## 📊 Monitoring

### Key Metrics to Monitor

1. **Producer Throughput**: `docker-compose logs producer | grep "📊 Processed"`
2. **Error Rate**: Check DLQ directory for failed messages
3. **Service Health**: `docker-compose ps`
4. **Queue Depth**: Monitor Redis queue length

### Log Monitoring

```bash
# Producer logs
docker-compose logs -f producer

# Receiver logs  
docker-compose logs -f receiver

# All services
docker-compose logs -f
```

## 🔍 Troubleshooting

### Common Issues

1. **Schema Registry Connection**
   - Verify `SCHEMA_REGISTRY_URL` is accessible
   - Check JWT token validity

2. **Kafka Connection**
   - Verify `KAFKA_BOOTSTRAP_SERVERS` is reachable
   - Check SASL credentials

3. **Message Processing**
   - Check DLQ for failed messages
   - Monitor producer logs for errors

### Debug Commands

```bash
# Check service status
docker-compose ps

# View recent logs
docker-compose logs --tail=50 producer

# Check DLQ
ls -la dlq/

# Test connectivity
docker-compose exec producer python -c "
import redis
r = redis.Redis(host='redis', port=6379, db=0)
print('Redis connected:', r.ping())
"
```

## 🔒 Security Considerations

1. **Environment Variables**: Keep `.env` secure and never commit to version control
2. **Network Security**: Ensure proper firewall rules for Kafka cluster access
3. **JWT Tokens**: Rotate tokens regularly and use least-privilege access
4. **Container Security**: Run containers with non-root users where possible

## 📈 Scaling

### Horizontal Scaling

For higher throughput, consider:

1. **Multiple Producer Instances**: Scale producer service
2. **Load Balancer**: Add nginx instances
3. **Redis Cluster**: For high-availability message queuing

### Performance Tuning

Adjust producer settings via environment variables in `.env`:

```bash
# For higher throughput
KAFKA_BATCH_SIZE=262144              # 256KB batch size
KAFKA_LINGER_MS=10                   # 10ms linger time
KAFKA_BATCH_NUM_MESSAGES=500         # More messages per batch

# For lower latency
KAFKA_LINGER_MS=1                    # 1ms linger time
KAFKA_BATCH_NUM_MESSAGES=50          # Fewer messages per batch

# For reliability
KAFKA_ACKS=all                       # All replicas acknowledgment
KAFKA_RETRIES=5                      # More retry attempts
KAFKA_ENABLE_IDEMPOTENCE=true        # Exactly-once delivery
```

## 🆘 Support

For production issues:

1. Check logs: `docker-compose logs -f`
2. Verify configuration: Review `.env` settings
3. Test connectivity: Use debug commands above
4. Check DLQ: `ls -la dlq/` for failed messages
