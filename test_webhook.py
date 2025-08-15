#!/usr/bin/env python3
"""
Bugsnag webhook test script that generates sample error data using Faker
and sends it to the webhook receiver to test the full pipeline.
"""

import requests
import json
import time
import random
import uuid
from faker import Faker
from datetime import datetime
import argparse

class BugsnagWebhookTester:
    """
    Generate and send mock Bugsnag webhook data to test the webhook pipeline.
    """
    
    def __init__(self, webhook_url="http://localhost:8080/webhook"):
        self.webhook_url = webhook_url
        self.fake = Faker()
        
    def generate_javascript_error(self):
        """Generate realistic JavaScript error scenarios"""
        error_scenarios = [
            {
                "errorClass": "TypeError",
                "errorMessage": f"Cannot read property '{self.fake.word()}' of undefined",
                "file": f"/static/js/{self.fake.file_name(extension='js')}",
                "context": f"/{self.fake.uri_path()}"
            },
            {
                "errorClass": "ReferenceError", 
                "errorMessage": f"{self.fake.word()} is not defined",
                "file": f"/src/components/{self.fake.word().title()}.jsx",
                "context": "/dashboard"
            },
            {
                "errorClass": "SyntaxError",
                "errorMessage": "Unexpected token ')'",
                "file": f"/assets/{self.fake.file_name(extension='js')}",
                "context": f"/{self.fake.uri_path()}"
            },
            {
                "errorClass": "NetworkError",
                "errorMessage": "Failed to fetch",
                "file": f"/api/{self.fake.word()}.js",
                "context": "/api/users"
            }
        ]
        return random.choice(error_scenarios)
    
    def generate_server_error(self):
        """Generate realistic server-side error scenarios"""
        error_scenarios = [
            {
                "errorClass": "ValidationError",
                "errorMessage": f"Required field '{self.fake.word()}' is missing",
                "file": f"/app/models/{self.fake.word()}.py",
                "context": "/api/v1/users"
            },
            {
                "errorClass": "DatabaseError",
                "errorMessage": "Connection to database failed",
                "file": "/app/database/connection.py",
                "context": "/api/v1/orders"
            },
            {
                "errorClass": "AuthenticationError",
                "errorMessage": "Invalid JWT token",
                "file": "/app/middleware/auth.py", 
                "context": "/api/protected"
            },
            {
                "errorClass": "TimeoutError",
                "errorMessage": "Request timeout after 30 seconds",
                "file": f"/app/services/{self.fake.word()}_service.py",
                "context": f"/api/{self.fake.word()}"
            }
        ]
        return random.choice(error_scenarios)
    
    def generate_bugsnag_event(self):
        """Generate Bugsnag webhook event data matching the new simplified AVRO schema"""
        # Generate timestamps
        now = datetime.now()
        server_time = int(now.timestamp() * 1000)  # milliseconds since epoch
        
        # Generate IDs
        error_id = str(uuid.uuid4())
        project_id = str(uuid.uuid4())
        
        # Choose between JavaScript and server-side error
        is_js_error = random.choice([True, False])
        if is_js_error:
            error_data = self.generate_javascript_error()
        else:
            error_data = self.generate_server_error()
        
        # Build the webhook payload matching the simplified AVRO schema
        event_data = {
            # Required fields
            "trigger_type": random.choice(["errorRateSpike", "newError", "reopenedError", "errorThreshold"]),
            "project_id": project_id,
            "project_name": f"{self.fake.company()} {random.choice(['Web App', 'Mobile App', 'API', 'Backend Service'])}",
            "server_time": server_time,
            "ingestion_timestamp": server_time,
            
            # Optional fields - set some to null randomly
            "trigger_message": f"Error rate increased to {random.randint(10, 100)}%" if random.choice([True, False]) else None,
            "trigger_rate": round(random.uniform(0.1, 99.9), 2) if random.choice([True, False]) else None,
            "project_url": f"https://app.bugsnag.com/projects/{project_id}",
            "error_id": error_id,
            "error_url": f"https://app.bugsnag.com/errors/{error_id}",
            "error_class": error_data["errorClass"],
            "error_message": error_data["errorMessage"],
            "error_context": error_data.get("context", "/"),
            "first_received": now.isoformat() + 'Z',
            "received_at": now.isoformat() + 'Z',
            "severity": random.choice(["error", "warning", "info"]),
            "status": random.choice(["open", "ignored", "fixed", "snoozed"]),
            "grouping_hash": self.fake.sha256()[:16],
            
            # Complex fields as JSON strings (schema expects strings for these)
            "stacktrace": json.dumps([{
                "file": error_data.get("file", f"/app/{self.fake.file_name()}"),
                "lineNumber": random.randint(1, 1000),
                "columnNumber": random.randint(1, 200),
                "method": self.fake.word() + "_handler",
                "inProject": True
            }]) if random.choice([True, False]) else None,
            
            "exceptions": json.dumps([{
                "errorClass": error_data["errorClass"],
                "message": error_data["errorMessage"],
                "stacktrace": [{
                    "file": error_data.get("file", f"/app/{self.fake.file_name()}"),
                    "lineNumber": random.randint(1, 1000)
                }]
            }]),
            
            "breadcrumbs": json.dumps([{
                "timestamp": (now.timestamp() - random.randint(1, 300)),
                "name": random.choice(["navigation", "request", "log", "user"]),
                "type": "manual",
                "metaData": {"message": self.fake.sentence()}
            }]) if random.choice([True, False]) else None,
            
            "request": json.dumps({
                "url": f"https://{self.fake.domain_name()}{error_data.get('context', '/')}",
                "httpMethod": random.choice(["GET", "POST", "PUT", "DELETE"]),
                "clientIp": self.fake.ipv4(),
                "referer": f"https://{self.fake.domain_name()}/"
            }) if random.choice([True, False]) else None,
            
            "device": json.dumps({
                "userAgent": self.fake.user_agent(),
                "locale": self.fake.locale(),
                "browserName": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "osName": random.choice(["Mac OS", "Windows", "Linux", "iOS", "Android"])
            }),
            
            "app": json.dumps({
                "version": f"{random.randint(1, 5)}.{random.randint(0, 99)}.{random.randint(0, 999)}",
                "releaseStage": random.choice(["production", "staging", "development"]),
                "type": "browser" if is_js_error else "server"
            }),
            
            "user": json.dumps({
                "id": str(random.randint(1, 100000)),
                "email": self.fake.email(),
                "name": self.fake.name()
            }) if random.choice([True, False]) else None,
            
            "metadata": json.dumps({
                "custom": {
                    "feature": self.fake.word(),
                    "action": self.fake.word(),
                    "timestamp": now.isoformat()
                }
            }) if random.choice([True, False]) else None,
            
            "release": f"v{random.randint(1, 5)}.{random.randint(0, 99)}.{random.randint(0, 999)}",
            "account": json.dumps({
                "id": str(random.randint(1, 1000)),
                "name": self.fake.company()
            }) if random.choice([True, False]) else None
        }
        
        return event_data
    

    
    def send_webhook(self, data, timeout=10):
        """Send Bugsnag webhook data to the endpoint"""
        try:
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Bugsnag-Webhook/1.0",
                "X-Bugsnag-Event": "error.created",
                "X-Test-Source": "bugsnag-webhook-tester"
            }
            
            response = requests.post(
                self.webhook_url,
                json=data,
                headers=headers,
                timeout=timeout
            )
            
            return {
                "success": response.status_code in [200, 202],  # Accept both 200 OK and 202 Accepted
                "status_code": response.status_code,
                "response_text": response.text,
                "response_time": response.elapsed.total_seconds()
            }
            
        except requests.exceptions.RequestException as e:
            return {
                "success": False,
                "error": str(e),
                "response_time": None
            }
    
    def generate_invalid_bugsnag_event(self, invalid_type="missing_required"):
        """Generate invalid Bugsnag events for DLQ testing"""
        # Start with a valid event and make it invalid
        event = self.generate_bugsnag_event()
        
        if invalid_type == "missing_required":
            # Remove required fields
            required_fields = ["trigger_type", "project_id", "project_name", "server_time", "ingestion_timestamp"]
            field_to_remove = random.choice(required_fields)
            del event[field_to_remove]
        
        elif invalid_type == "wrong_type":
            # Wrong data types
            wrong_type_scenarios = [
                ("server_time", "not_a_number"),  # Should be long
                ("trigger_rate", "not_a_double"),  # Should be double
                ("ingestion_timestamp", "not_a_long"),  # Should be long
            ]
            field, wrong_value = random.choice(wrong_type_scenarios)
            event[field] = wrong_value
        
        elif invalid_type == "invalid_nested":
            # Invalid JSON in string fields
            json_fields = ["stacktrace", "exceptions", "device", "app"]
            field_to_corrupt = random.choice(json_fields)
            if field_to_corrupt in event and event[field_to_corrupt]:
                event[field_to_corrupt] = "{ invalid json: true"  # Malformed JSON
        
        elif invalid_type == "missing_nested":
            # Set complex fields to empty/invalid values
            event["exceptions"] = json.dumps([])  # Empty exceptions array
            
        return event
    
    def run_bugsnag_test(self, num_events=10, delay=1.0, invalid_percentage=0):
        """Run Bugsnag webhook test suite"""
        print(f"🐛 Starting Bugsnag webhook test suite")
        print(f"📡 Target URL: {self.webhook_url}")
        print(f"📊 Error events to send: {num_events}")
        print(f"⏱️  Delay between events: {delay}s")
        if invalid_percentage > 0:
            print(f"⚠️  Invalid messages: {invalid_percentage}%")
        print("=" * 60)
        
        results = {
            "total_sent": 0,
            "successful": 0,
            "failed": 0,
            "errors": [],
            "error_types": {}
        }
        
        invalid_types = ["missing_required", "wrong_type", "invalid_nested", "missing_nested"]
        invalid_count = 0
        
        for i in range(num_events):
            # Determine if this should be an invalid message
            should_be_invalid = invalid_percentage > 0 and random.randint(1, 100) <= invalid_percentage
            
            if should_be_invalid:
                invalid_type = random.choice(invalid_types)
                event_data = self.generate_invalid_bugsnag_event(invalid_type)
                invalid_count += 1
                print(f"\n⚠️  [{i+1}/{num_events}] Sending INVALID event (type: {invalid_type})...")
                error_class = f"INVALID_{invalid_type}"
            else:
                # Generate valid Bugsnag event data
                event_data = self.generate_bugsnag_event()
                error_class = event_data["error_class"]
                print(f"\n🐛 [{i+1}/{num_events}] Sending {error_class} error...")
                print(f"   Error: {event_data['error_message'][:60]}...")
                print(f"   Context: {event_data['error_context']}")
            
            # Track error types
            if error_class not in results["error_types"]:
                results["error_types"][error_class] = 0
            results["error_types"][error_class] += 1
            
            # Send webhook
            result = self.send_webhook(event_data)
            results["total_sent"] += 1
            
            if result["success"]:
                results["successful"] += 1
                print(f"   ✅ Success! ({result['status_code']}) - {result['response_time']:.3f}s")
            else:
                results["failed"] += 1
                error_msg = result.get("error", f"HTTP {result.get('status_code', 'Unknown')}")
                results["errors"].append(f"{error_class}: {error_msg}")
                print(f"   ❌ Failed: {error_msg}")
            
            # Wait before next event (except for the last one)
            if i < num_events - 1:
                time.sleep(delay)
        
        # Print summary
        print("\n" + "=" * 60)
        print("📈 BUGSNAG TEST SUMMARY")
        print("=" * 60)
        print(f"Total error events sent: {results['total_sent']}")
        print(f"Successful: {results['successful']}")
        print(f"Failed: {results['failed']}")
        print(f"Success rate: {results['successful']/results['total_sent']*100:.1f}%")
        
        if invalid_count > 0:
            print(f"\n⚠️  Invalid messages sent: {invalid_count} (should appear in DLQ)")
        
        if results['error_types']:
            print("\n🎯 Error types generated:")
            # Separate valid and invalid types
            valid_types = {k: v for k, v in results['error_types'].items() if not k.startswith('INVALID_')}
            invalid_types = {k: v for k, v in results['error_types'].items() if k.startswith('INVALID_')}
            
            if valid_types:
                for error_type, count in sorted(valid_types.items()):
                    print(f"   - {error_type}: {count}")
            
            if invalid_types:
                print("\n⚠️  Invalid event types:")
                for error_type, count in sorted(invalid_types.items()):
                    print(f"   - {error_type}: {count}")
        
        if results["errors"]:
            print(f"\n❌ Errors encountered:")
            for error in results["errors"]:
                print(f"   - {error}")
        
        return results

def main():
    parser = argparse.ArgumentParser(description="Test Bugsnag webhook endpoint with realistic error data")
    parser.add_argument(
        "--url", 
        default="http://localhost:8080/webhook",
        help="Webhook endpoint URL (default: http://localhost:8080/webhook)"
    )
    parser.add_argument(
        "--count", 
        type=int, 
        default=10,
        help="Number of error events to send (default: 10)"
    )
    parser.add_argument(
        "--delay", 
        type=float, 
        default=1.0,
        help="Delay between events in seconds (default: 1.0)"
    )
    parser.add_argument(
        "--invalid", 
        type=int, 
        default=0,
        help="Percentage of invalid messages to send for DLQ testing (0-100, default: 0)"
    )
    parser.add_argument(
        "--single", 
        action="store_true",
        help="Send a single error event and show the JSON payload"
    )
    
    args = parser.parse_args()
    
    tester = BugsnagWebhookTester(args.url)
    
    if args.single:
        # Generate and display a single event
        print("🐛 Generating single Bugsnag error event:")
        print("=" * 60)
        
        event_data = tester.generate_bugsnag_event()
        print(json.dumps(event_data, indent=2))
        
        print("\n" + "=" * 60)
        print("📤 Sending to webhook...")
        
        result = tester.send_webhook(event_data)
        if result["success"]:
            print(f"✅ Success! ({result['status_code']}) - {result['response_time']:.3f}s")
            print(f"Response: {result['response_text']}")
        else:
            error_msg = result.get("error", f"HTTP {result.get('status_code', 'Unknown')}")
            print(f"❌ Failed: {error_msg}")
    else:
        # Run the full test suite
        tester.run_bugsnag_test(args.count, args.delay, args.invalid)

if __name__ == "__main__":
    main()
