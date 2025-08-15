#!/usr/bin/env python3
"""
Production test script for the cleaned-up JSON Schema webhook pipeline.
Tests with 10,000 messages to verify production readiness.
"""

import requests
import json
import time
import random
import uuid
from faker import Faker
from datetime import datetime
import argparse

class ProductionWebhookTester:
    """
    Production test for the JSON Schema webhook pipeline.
    """
    
    def __init__(self, webhook_url="http://localhost:8080/webhook"):
        self.webhook_url = webhook_url
        self.fake = Faker()
        
    def generate_bugsnag_event(self):
        """Generate Bugsnag webhook event data matching the JSON Schema structure"""
        # Generate timestamps
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        # Generate IDs
        account_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())
        error_id = str(uuid.uuid4())
        event_id = str(uuid.uuid4())
        
        # Generate error data
        error_classes = ["TypeError", "ReferenceError", "SyntaxError", "NetworkError", 
                        "ValidationError", "DatabaseError", "AuthenticationError", "TimeoutError"]
        error_class = random.choice(error_classes)
        
        # Build the webhook payload matching the JSON Schema structure
        event_data = {
            "account": {
                "id": account_id,
                "name": self.fake.company(),
                "url": f"https://app.bugsnag.com/accounts/{account_id}"
            },
            "project": {
                "id": project_id,
                "name": f"{self.fake.company()} {random.choice(['Web App', 'Mobile App', 'API', 'Backend Service'])}",
                "url": f"https://app.bugsnag.com/{account_id}/{project_id}"
            },
            "trigger": {
                "type": random.choice(["firstException", "newError", "reopenedError", "errorRateSpike"]),
                "message": f"A new error has occurred in your project {self.fake.company()}"
            },
            "error": {
                "id": error_id,
                "url": f"https://app.bugsnag.com/{account_id}/{project_id}/errors/{error_id}",
                "context": f"/{self.fake.uri_path()}",
                "firstReceived": timestamp,
                "severity": random.choice(["error", "warning", "info"]),
                "status": random.choice(["open", "ignored", "fixed", "snoozed"]),
                "createdIssue": {
                    "id": f"BUG{random.randint(100, 999)}",
                    "number": random.randint(1, 100),
                    "type": random.choice(["bugify", "jira", "github"]),
                    "url": f"https://demo.{random.choice(['bugify', 'jira', 'github'])}.com/issues/{random.randint(1, 100)}"
                }
            },
            "event": {
                "id": event_id,
                "received": timestamp,
                "user": {
                    "id": str(random.randint(1, 100000)),
                    "name": self.fake.name(),
                    "email": self.fake.email()
                },
                "app": {
                    "id": f"com.{self.fake.word()}.{self.fake.word()}.{random.choice(['debug', 'release'])}",
                    "version": f"{random.randint(1, 5)}.{random.randint(0, 99)}.{random.randint(0, 999)}",
                    "versionCode": random.randint(1, 100),
                    "releaseStage": random.choice(["production", "staging", "development"])
                },
                "device": {
                    "hostname": f"{self.fake.word()}.{self.fake.word()}.com",
                    "id": self.fake.sha256()[:20],
                    "manufacturer": random.choice(["LGE", "Samsung", "Apple", "Google", "OnePlus"]),
                    "model": random.choice(["Nexus 5", "iPhone 12", "Galaxy S21", "Pixel 6", "OnePlus 9"]),
                    "osName": random.choice(["android", "ios", "windows", "macos", "linux"]),
                    "osVersion": f"{random.randint(4, 15)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
                },
                "exceptions": [
                    {
                        "errorClass": error_class,
                        "message": f"Test error message for {error_class}",
                        "stacktrace": [
                            {
                                "file": f"/app/{self.fake.file_name()}",
                                "lineNumber": random.randint(1, 1000),
                                "method": self.fake.word() + "_handler"
                            }
                        ]
                    }
                ]
            }
        }
        
        return event_data
    
    def send_webhook(self, data, timeout=10):
        """Send Bugsnag webhook data to the endpoint"""
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Production-Tester/1.0"
            }
            
            response = requests.post(
                self.webhook_url,
                json=data,
                headers=headers,
                timeout=timeout
            )
            
            return response.status_code, response.elapsed.total_seconds()
            
        except requests.exceptions.Timeout:
            return None, timeout
        except requests.exceptions.ConnectionError:
            return None, 0
        except Exception as e:
            print(f"❌ Unexpected error sending webhook: {e}")
            return None, 0
    
    def run_test(self, count=10000, delay=0.01):
        """Run the production test with specified parameters"""
        print("🚀 Production Webhook Test Runner")
        print("=" * 50)
        print(f"📡 Target URL: {self.webhook_url}")
        print(f"📊 Messages to send: {count:,}")
        print(f"⏱️  Delay between messages: {delay}s")
        print(f"🎯 Expected duration: ~{count * delay / 60:.1f} minutes")
        print("=" * 50)
        print()
        
        successful = 0
        failed = 0
        start_time = time.time()
        
        for i in range(count):
            # Generate and send event
            event_data = self.generate_bugsnag_event()
            status_code, response_time = self.send_webhook(event_data)
            
            if status_code == 202:
                successful += 1
            else:
                failed += 1
            
            # Progress reporting
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                success_rate = (successful / (i + 1)) * 100
                print(f"📊 [{i+1:,}/{count:,}] Progress: {success_rate:.1f}% success rate, {rate:.1f} msg/s")
            
            # Add delay between requests
            if delay > 0 and i < count - 1:
                time.sleep(delay)
        
        # Final summary
        total_time = time.time() - start_time
        final_rate = count / total_time if total_time > 0 else 0
        final_success_rate = (successful / count) * 100
        
        print()
        print("=" * 50)
        print("📈 PRODUCTION TEST SUMMARY")
        print("=" * 50)
        print(f"Total messages sent: {count:,}")
        print(f"Successful: {successful:,}")
        print(f"Failed: {failed:,}")
        print(f"Success rate: {final_success_rate:.1f}%")
        print(f"Total time: {total_time:.1f}s")
        print(f"Average rate: {final_rate:.1f} msg/s")
        print(f"Expected vs Actual: {count * delay:.1f}s vs {total_time:.1f}s")
        
        if final_success_rate >= 99.0:
            print("✅ Production test PASSED - System is ready for production!")
        else:
            print("❌ Production test FAILED - Review logs and fix issues")

def main():
    parser = argparse.ArgumentParser(description="Production test for webhook pipeline")
    parser.add_argument("--count", type=int, default=10000, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay between messages in seconds")
    parser.add_argument("--url", default="http://localhost:8080/webhook", help="Webhook URL")
    
    args = parser.parse_args()
    
    # Create tester and run test
    tester = ProductionWebhookTester(args.url)
    tester.run_test(args.count, args.delay)

if __name__ == "__main__":
    main()
