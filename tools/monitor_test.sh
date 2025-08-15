#!/bin/bash

# Enhanced monitoring for large-scale message tests
# Usage: ./monitor_test.sh [message_count]
# Default: 100000 messages

TOTAL_MESSAGES=${1:-100000}

echo "🔍 Monitoring ${TOTAL_MESSAGES} Message Test"
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

# Function to get last throughput
get_last_throughput() {
    docker-compose logs producer 2>/dev/null | grep "📊 Throughput:" | tail -1 | grep -oE "[0-9]+\.[0-9]+ msg/sec" || echo "0 msg/sec"
}

start_time=$(date +%s)
last_count=0

while true; do
    current_count=$(get_message_count)
    dlq_count=$(get_dlq_count)
    redis_queue=$(get_redis_queue)
    throughput=$(get_last_throughput)
    current_time=$(date +%s)
    elapsed=$((current_time - start_time))
    
    # Calculate progress percentage
    if [ -n "$current_count" ] && [ "$current_count" -gt 0 ]; then
        progress=$(echo "scale=2; $current_count * 100 / $TOTAL_MESSAGES" | bc 2>/dev/null || echo "0")
        rate=$(echo "scale=1; ($current_count - $last_count) / 2" | bc 2>/dev/null || echo "0")
        avg_rate=$(echo "scale=1; $current_count / $elapsed" | bc 2>/dev/null || echo "0")
        eta_seconds=$(echo "scale=0; ($TOTAL_MESSAGES - $current_count) / $avg_rate" | bc 2>/dev/null || echo "0")
        eta_minutes=$(echo "scale=1; $eta_seconds / 60" | bc 2>/dev/null || echo "0")
        
        clear
        echo "🔍 $(printf "%'d" $TOTAL_MESSAGES) MESSAGE TEST MONITOR - $(date '+%H:%M:%S')"
        echo "============================================"
        echo "📊 Messages Processed: $current_count / $(printf "%'d" $TOTAL_MESSAGES) ($progress%)"
        echo "📨 Redis Queue Length: $redis_queue"
        echo "📁 DLQ Messages: $dlq_count"
        echo "⚡ Last Throughput: $throughput"
        echo "📈 Average Rate: $avg_rate msg/sec"
        echo "⏱️  Elapsed Time: ${elapsed}s"
        echo "🎯 ETA: ~${eta_minutes} minutes"
        echo ""
        
        # Progress bar
        printf "Progress: ["
        filled=$(echo "scale=0; $progress / 2" | bc 2>/dev/null || echo "0")
        for ((i=0; i<$filled; i++)); do printf "█"; done
        for ((i=$filled; i<50; i++)); do printf "░"; done
        printf "] $progress%%\n"
        
        echo ""
        echo "Press Ctrl+C to stop monitoring"
        
        last_count=$current_count
    else
        echo "Waiting for messages to start processing..."
    fi
    
    sleep 2
done
