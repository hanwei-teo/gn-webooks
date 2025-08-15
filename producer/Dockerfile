FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libsasl2-2 \
    libsasl2-modules \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN pip install uv

WORKDIR /app

# Install Python dependencies for AVRO producer
RUN uv pip install --system confluent-kafka==2.5.0 redis==5.0.1 requests==2.31.0 fastavro==1.9.4

# Copy the producer
COPY producer.py producer.py

# Create DLQ directory
RUN mkdir -p /app/dlq

CMD ["python", "producer.py"]
