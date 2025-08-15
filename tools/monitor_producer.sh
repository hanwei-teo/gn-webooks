#!/bin/bash

# Monitor producer progress in real-time

echo "🔍 Monitoring Producer Progress..."
echo "================================="
echo ""

# Function to get message count
get_message_count() {
    docker-compose logs producer 2>/dev/null | grep -oE "Total: [0-9]+" | tail -1 | cut -d' ' -f2
}

# Function to get DLQ count
get_dlq_count() {
    docker-compose exec producer find /app/dlq -name "*.json" 2>/dev/null | wc -l | tr -d ' '
}

# Function to get Redis queue length
get_redis_queue() {
    docker-compose exec redis redis-cli llen webhook_queue 2>/dev/null | tr -d '\r\n'
}

start_time=$(date +%s)
last_count=0

while true; do
    current_count=$(get_message_count)
    dlq_count=$(get_dlq_count)
    redis_queue=$(get_redis_queue)
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    if [ -n "$current_count" ] && [ "$current_count" != "$last_count" ]; then
        rate=$(echo "scale=1; ($current_count - $last_count) / 5" | bc 2>/dev/null || echo "0")
        avg_rate=$(echo "scale=1; $current_count / $elapsed" | bc 2>/dev/null || echo "0")
        
        clear
        echo "🔍 Producer Monitor - $(date '+%H:%M:%S')"
        echo "================================="
        echo "📊 Messages Processed: $current_count"
        echo "📨 Redis Queue Length: $redis_queue"
        echo "📁 DLQ Messages: $dlq_count"
        echo "⚡ Current Rate: $rate msg/sec"
        echo "📈 Average Rate: $avg_rate msg/sec"
        echo "⏱️  Elapsed Time: ${elapsed}s"
        echo ""
        echo "Press Ctrl+C to stop monitoring"
        
        last_count=$current_count
    fi
    
    sleep 5
done
