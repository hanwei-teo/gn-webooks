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

    def generate_invalid_payload(self):
        """Generate an invalid payload that should trigger DLQ"""
        invalid_types = [
            "missing_required_fields",
            "wrong_data_types", 
            "malformed_json",
            "empty_payload",
            "extra_fields",
            "null_values"
        ]
        
        invalid_type = random.choice(invalid_types)
        
        if invalid_type == "missing_required_fields":
            # Missing required fields like account, project, error, event
            return {
                "trigger": {
                    "type": "firstException",
                    "message": "Missing required fields test"
                }
            }
            
        elif invalid_type == "wrong_data_types":
            # Wrong data types (strings where numbers expected, etc.)
            return {
                "account": {
                    "id": 12345,  # Should be string
                    "name": True,  # Should be string
                    "url": None    # Should be string
                },
                "project": {
                    "id": "test",
                    "name": "test",
                    "url": "test"
                },
                "error": {
                    "id": "test",
                    "url": "test",
                    "context": "test",
                    "firstReceived": "not-a-timestamp",  # Invalid timestamp
                    "severity": 123,  # Should be string
                    "status": []      # Should be string
                },
                "event": {
                    "id": "test",
                    "received": "invalid-timestamp",
                    "user": "not-an-object",  # Should be object
                    "app": None,              # Should be object
                    "device": 123,            # Should be object
                    "exceptions": "not-an-array"  # Should be array
                }
            }
            
        elif invalid_type == "malformed_json":
            # This will be handled by the receiver's JSON validation
            return "This is not valid JSON"
            
        elif invalid_type == "empty_payload":
            # Empty payload
            return {}
            
        elif invalid_type == "extra_fields":
            # Valid structure but with extra fields that don't match schema
            valid_payload = self.generate_bugsnag_event()
            valid_payload["extra_field_1"] = "should not be here"
            valid_payload["account"]["extra_field_2"] = 12345
            valid_payload["event"]["exceptions"][0]["extra_field_3"] = {"nested": "extra"}
            return valid_payload
            
        elif invalid_type == "null_values":
            # Null values in required fields
            return {
                "account": None,
                "project": {
                    "id": None,
                    "name": None,
                    "url": None
                },
                "error": {
                    "id": "test",
                    "url": "test",
                    "context": "test",
                    "firstReceived": None,
                    "severity": None,
                    "status": None
                },
                "event": {
                    "id": None,
                    "received": None,
                    "user": None,
                    "app": None,
                    "device": None,
                    "exceptions": None
                }
            }
        
        # Fallback to empty payload
        return {}

    def send_webhook(self, data):
        """Send webhook to the receiver"""
        try:
            # Handle malformed JSON case
            if isinstance(data, str):
                response = requests.post(
                    self.target_url,
                    data=data,  # Send as raw string, not JSON
                    headers={'Content-Type': 'application/json'},
                    timeout=10
                )
            else:
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

    def run_test(self, count=1000, delay=0.01, invalid_percentage=10):
        """Run the end-to-end test with invalid payloads"""
        print("🚀 Bugsnag Webhook End-to-End Test (with DLQ testing)")
        print("=" * 50)
        print(f"📡 Target URL: {self.target_url}")
        print(f"📊 Messages to send: {count:,}")
        print(f"🎯 Invalid payloads: {invalid_percentage}% (will trigger DLQ)")
        print(f"⏱️  Delay between messages: {delay}s")
        print(f"🎯 Expected duration: ~{count * delay / 60:.1f} minutes")
        print("=" * 50)
        print()

        start_time = time.time()
        successful = 0
        failed = 0
        valid_count = 0
        invalid_count = 0

        for i in range(count):
            # Determine if this should be an invalid payload
            should_be_invalid = random.random() < (invalid_percentage / 100.0)
            
            if should_be_invalid:
                # Generate invalid payload
                test_data = self.generate_invalid_payload()
                invalid_count += 1
            else:
                # Generate valid payload
                test_data = self.generate_bugsnag_event()
                valid_count += 1
            
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
                print(f"📊 [{i + 1:,}/{count:,}] Progress: {success_rate:.1f}% success rate, {rate:.1f} msg/s (Valid: {valid_count}, Invalid: {invalid_count})")

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
        print(f"Valid payloads: {valid_count:,}")
        print(f"Invalid payloads: {invalid_count:,}")
        print(f"Successful webhook submissions: {successful:,}")
        print(f"Failed webhook submissions: {failed:,}")
        print(f"Webhook success rate: {success_rate:.1f}%")
        print(f"Total time: {total_time:.1f}s")
        print(f"Average rate: {avg_rate:.1f} msg/s")
        print(f"Expected vs Actual: {count * delay:.1f}s vs {total_time:.1f}s")
        print("\n💡 Invalid payloads should appear in DLQ for review")

        if success_rate >= 99.0:
            print("✅ Bugsnag webhook test PASSED - Pipeline working correctly!")
        else:
            print("❌ Bugsnag webhook test FAILED - Check producer logs for validation errors")

def main():
    parser = argparse.ArgumentParser(description="Bugsnag Webhook End-to-End Test")
    parser.add_argument("--count", type=int, default=1000, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.0, help="Delay between messages in seconds")
    parser.add_argument("--url", default="http://localhost:8080/webhook/bugsnag", help="Target webhook URL")
    parser.add_argument("--invalid", type=int, default=0, help="Percentage of invalid payloads (0-100)")
    
    args = parser.parse_args()
    
    tester = BugsnagEndToEndTester(args.url)
    tester.run_test(args.count, args.delay, args.invalid)

if __name__ == "__main__":
    main()
