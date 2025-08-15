#!/usr/bin/env python3
"""
DLQ (Dead Letter Queue) viewer and analyzer for failed webhook messages.
"""

import json
import os
import argparse
from datetime import datetime
from collections import defaultdict

def analyze_dlq_directory(dlq_dir="./dlq"):
    """Analyze all DLQ files in the directory"""
    if not os.path.exists(dlq_dir):
        print(f"❌ DLQ directory not found: {dlq_dir}")
        return
    
    dlq_files = [f for f in os.listdir(dlq_dir) if f.startswith('dlq_') and f.endswith('.json')]
    
    if not dlq_files:
        print("✅ No failed messages found in DLQ")
        return
    
    print(f"📁 DLQ Directory: {dlq_dir}")
    print(f"💀 Found {len(dlq_files)} failed messages")
    print("=" * 60)
    
    error_stats = defaultdict(int)
    topic_stats = defaultdict(int)
    timeline = []
    
    for i, filename in enumerate(sorted(dlq_files), 1):
        filepath = os.path.join(dlq_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                dlq_entry = json.load(f)
            
            timestamp = dlq_entry.get('timestamp', 'Unknown')
            topic = dlq_entry.get('topic', 'Unknown')
            error_reason = dlq_entry.get('error_reason', 'Unknown')
            
            # Extract error type
            error_type = error_reason.split(':')[0] if ':' in error_reason else error_reason
            error_stats[error_type] += 1
            topic_stats[topic] += 1
            timeline.append((timestamp, error_type, topic))
            
            print(f"🐛 [{i}] {filename}")
            print(f"   📅 Time: {timestamp}")
            print(f"   📤 Topic: {topic}")
            print(f"   ❌ Error: {error_reason}")
            print()
            
        except Exception as e:
            print(f"⚠️  Error reading {filename}: {e}")
    
    # Print summary
    print("=" * 60)
    print("📊 SUMMARY")
    print("=" * 60)
    
    print(f"🎯 Error Types:")
    for error_type, count in sorted(error_stats.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(dlq_files)) * 100
        print(f"   - {error_type}: {count} ({percentage:.1f}%)")
    
    print(f"\n📤 Topics:")
    for topic, count in sorted(topic_stats.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(dlq_files)) * 100
        print(f"   - {topic}: {count} ({percentage:.1f}%)")

def view_dlq_file(filepath):
    """View a specific DLQ file in detail"""
    try:
        with open(filepath, 'r') as f:
            dlq_entry = json.load(f)
        
        print(f"📄 DLQ File: {os.path.basename(filepath)}")
        print("=" * 60)
        print(f"📅 Timestamp: {dlq_entry.get('timestamp', 'Unknown')}")
        print(f"📤 Topic: {dlq_entry.get('topic', 'Unknown')}")
        print(f"❌ Error: {dlq_entry.get('error_reason', 'Unknown')}")
        print()
        print("📝 Original Message:")
        print(json.dumps(dlq_entry.get('original_message', {}), indent=2))
        
    except Exception as e:
        print(f"❌ Error reading file: {e}")

def clear_dlq(dlq_dir="./dlq"):
    """Clear all DLQ files"""
    if not os.path.exists(dlq_dir):
        print(f"❌ DLQ directory not found: {dlq_dir}")
        return
    
    dlq_files = [f for f in os.listdir(dlq_dir) if f.startswith('dlq_') and f.endswith('.json')]
    
    if not dlq_files:
        print("✅ No DLQ files to clear")
        return
    
    print(f"🗑️  Clearing {len(dlq_files)} DLQ files...")
    
    for filename in dlq_files:
        filepath = os.path.join(dlq_dir, filename)
        try:
            os.remove(filepath)
            print(f"   ✅ Deleted {filename}")
        except Exception as e:
            print(f"   ❌ Failed to delete {filename}: {e}")
    
    print("✅ DLQ cleared")

def main():
    parser = argparse.ArgumentParser(description="View and analyze DLQ files")
    parser.add_argument(
        "--dir",
        default="./dlq",
        help="DLQ directory path (default: ./dlq)"
    )
    parser.add_argument(
        "--file",
        help="View specific DLQ file"
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear all DLQ files"
    )
    
    args = parser.parse_args()
    
    if args.clear:
        response = input("⚠️  Are you sure you want to clear all DLQ files? (y/N): ")
        if response.lower() in ['y', 'yes']:
            clear_dlq(args.dir)
        else:
            print("❌ Clear operation cancelled")
    elif args.file:
        view_dlq_file(args.file)
    else:
        analyze_dlq_directory(args.dir)

if __name__ == "__main__":
    main()
