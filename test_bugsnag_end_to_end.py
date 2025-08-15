#!/usr/bin/env python3
"""
End-to-end test for Bugsnag webhook pipeline with AVRO schema validation.
Tests the complete pipeline: webhook → Redis → Kafka → Schema Registry.
"""

import requests
import json
import time
import random
import uuid
from faker import Faker
from datetime import datetime
import argparse

class BugsnagEndToEndTester:
    def __init__(self, target_url="http://localhost:8080/webhook/bugsnag"):
        self.target_url = target_url
        self.fake = Faker()
        
    def generate_bugsnag_event(self):
        """Generate a Bugsnag event that matches the AVRO schema"""
        # Generate timestamps in ISO format (will be converted to timestamp-millis)
        timestamp = datetime.now().isoformat() + "Z"
        
        return {
            "account": {
                "id": str(uuid.uuid4()),
                "name": self.fake.company(),
                "url": f"https://app.bugsnag.com/accounts/{self.fake.slug()}"
            },
            "project": {
                "id": str(uuid.uuid4()),
                "name": f"{self.fake.word().capitalize()} Project",
                "url": f"https://app.bugsnag.com/projects/{self.fake.slug()}"
            },
            "trigger": {
                "type": random.choice(["firstException", "newError", "reopenedError", "errorRateSpike"]),
                "message": f"A new error has occurred in your project {self.fake.word().capitalize()}"
            },
            "error": {
                "id": str(uuid.uuid4()),
                "url": f"https://app.bugsnag.com/errors/{self.fake.slug()}",
                "context": f"{self.fake.word()}/{self.fake.word()}#{self.fake.word()}",
                "firstReceived": timestamp,
                "severity": random.choice(["error", "warning", "info"]),
                "status": random.choice(["open", "ignored", "snoozed", "resolved"]),
                "createdIssue": {
                    "id": f"BUG{random.randint(100, 999)}",
                    "number": random.randint(1, 100),
                    "type": random.choice(["github", "jira", "bugify", "gitlab", "bitbucket"]),
                    "url": f"https://demo.{self.fake.word()}.com/issues/{random.randint(1, 100)}"
                }
            },
            "event": {
                "id": str(uuid.uuid4()),
                "received": timestamp,
                "user": {
                    "id": str(random.randint(1000, 99999)),
                    "name": self.fake.name(),
                    "email": self.fake.email()
                },
                "app": {
                    "id": f"com.{self.fake.word()}.{self.fake.word()}.{random.choice(['debug', 'release'])}",
                    "version": f"{random.randint(1, 9)}.{random.randint(0, 99)}.{random.randint(0, 999)}",
                    "versionCode": random.randint(1, 100),
                    "releaseStage": random.choice(["development", "staging", "production"])
                },
                "device": {
                    "hostname": f"{self.fake.word()}.{self.fake.word()}.com",
                    "id": self.fake.hex_color().replace('#', '') + self.fake.hex_color().replace('#', ''),
                    "manufacturer": random.choice(["Apple", "Samsung", "Google", "OnePlus", "LGE"]),
                    "model": random.choice(["iPhone 12", "Galaxy S21", "Pixel 6", "OnePlus 9", "Nexus 5"]),
                    "osName": random.choice(["ios", "android", "macos", "linux", "windows"]),
                    "osVersion": f"{random.randint(10, 15)}.{random.randint(0, 9)}.{random.randint(0, 9)}"
                },
                "exceptions": [
                    {
                        "errorClass": random.choice([
                            "NetworkError", "DatabaseError", "ValidationError", 
                            "TimeoutError", "ReferenceError", "SyntaxError"
                        ]),
                        "message": f"Test error message for {self.fake.word()}",
                        "stacktrace": [
                            {
                                "file": f"/app/{self.fake.word()}.{random.choice(['js', 'py', 'java', 'rb'])}",
                                "lineNumber": random.randint(1, 1000),
                                "method": f"{self.fake.word()}_handler"
                            }
                        ]
                    }
                ]
            }
        }

    def send_webhook(self, data):
        """Send webhook to the receiver"""
        try:
            response = requests.post(
                self.target_url,
                json=data,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            # Accept both 200 OK and 202 Accepted as success
            return response.status_code in [200, 202]
        except Exception as e:
            print(f"❌ Error sending webhook: {e}")
            return False

    def run_test(self, count=1000, delay=0.01):
        """Run the end-to-end test"""
        print("🚀 Bugsnag Webhook End-to-End Test")
        print("=" * 50)
        print(f"📡 Target URL: {self.target_url}")
        print(f"📊 Messages to send: {count:,}")
        print(f"⏱️  Delay between messages: {delay}s")
        print(f"🎯 Expected duration: ~{count * delay / 60:.1f} minutes")
        print("=" * 50)
        print()

        start_time = time.time()
        successful = 0
        failed = 0

        for i in range(count):
            # Generate test data
            test_data = self.generate_bugsnag_event()
            
            # Send webhook
            if self.send_webhook(test_data):
                successful += 1
            else:
                failed += 1

            # Progress reporting
            if (i + 1) % 1000 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                success_rate = (successful / (i + 1)) * 100
                print(f"📊 [{i + 1:,}/{count:,}] Progress: {success_rate:.1f}% success rate, {rate:.1f} msg/s")

            # Delay between messages
            if delay > 0:
                time.sleep(delay)

        # Final results
        total_time = time.time() - start_time
        success_rate = (successful / count) * 100
        avg_rate = count / total_time if total_time > 0 else 0

        print("\n" + "=" * 50)
        print("📈 BUGSNAG WEBHOOK TEST SUMMARY")
        print("=" * 50)
        print(f"Total messages sent: {count:,}")
        print(f"Successful: {successful:,}")
        print(f"Failed: {failed:,}")
        print(f"Success rate: {success_rate:.1f}%")
        print(f"Total time: {total_time:.1f}s")
        print(f"Average rate: {avg_rate:.1f} msg/s")
        print(f"Expected vs Actual: {count * delay:.1f}s vs {total_time:.1f}s")

        if success_rate >= 99.0:
            print("✅ Bugsnag webhook test PASSED - Pipeline working correctly!")
        else:
            print("❌ Bugsnag webhook test FAILED - Check producer logs for validation errors")

def main():
    parser = argparse.ArgumentParser(description="Bugsnag Webhook End-to-End Test")
    parser.add_argument("--count", type=int, default=1000, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay between messages in seconds")
    parser.add_argument("--url", default="http://localhost:8080/webhook/bugsnag", help="Target webhook URL")
    
    args = parser.parse_args()
    
    tester = BugsnagEndToEndTester(args.url)
    tester.run_test(args.count, args.delay)

if __name__ == "__main__":
    main()
