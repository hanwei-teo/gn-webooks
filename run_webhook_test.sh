#!/bin/bash
# Script to run Bugsnag webhook tests

echo "🐛 Bugsnag Webhook Test Runner"
echo "==============================="

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 is required but not installed"
    exit 1
fi

# Setup uv for dependency management
echo "📦 Setting up test environment..."
if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    python3 -m pip install uv --quiet --break-system-packages
fi

echo "✅ uv ready"
echo ""

# Check if services are running
echo "🔍 Checking if webhook service is running..."
if curl -s -f http://localhost:8080/health > /dev/null 2>&1; then
    echo "✅ Webhook service is running"
else
    echo "⚠️  Webhook service might not be running at http://localhost:8080"
    echo "   Start services with: docker-compose up"
    echo ""
fi

# Run the test using uv run (automatic dependency management)
echo "🚀 Starting Bugsnag webhook tests..."
echo ""
uv run --group test test_webhook.py "$@"
