FROM python:3.12-slim
WORKDIR /app

# Install system dependencies for SASL, librdkafka and other packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libsasl2-dev \
    libsasl2-2 \
    libsasl2-modules \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv, the fast Python package installer
RUN pip install uv

# Install producer dependencies using uv
RUN uv pip install --system "confluent-kafka[avro,schema-registry]==2.5.0" redis==5.0.1

# Copy the rest of the application code
COPY . .

# Run the producer script
CMD ["python", "-u", "producer.py"]
