#!/usr/bin/env python3
import sys
import json
from datetime import datetime
import paho.mqtt.client as mqtt

# Color codes for terminal output
RED = '\033[91m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"{GREEN}Connected to MQTT broker successfully{RESET}")
        # Subscribe to brake alerts topic
        client.subscribe("sncb/brake/alerts")
        print(f"{BLUE}Subscribed to topic: sncb/brake/alerts{RESET}")
    else:
        print(f"{RED}Failed to connect, return code {rc}{RESET}")

def on_message(client, userdata, msg):
    print(f"\n{YELLOW}{'='*60}{RESET}")
    print(f"{RED}BRAKE ALERT RECEIVED!{RESET}")
    print(f"Topic: {msg.topic}")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Try to parse as JSON if NebulaStream sends structured data
        payload = json.loads(msg.payload.decode())
        print(f"\nAlert Details:")
        for key, value in payload.items():
            print(f"  {key}: {value}")
    except:
        # If not JSON, just print raw payload
        print(f"Payload: {msg.payload.decode()}")
    
    print(f"{YELLOW}{'='*60}{RESET}")

def on_disconnect(client, userdata, rc):
    if rc != 0:
        print(f"{RED}Unexpected disconnection from broker{RESET}")

def main():
    broker_host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    broker_port = int(sys.argv[2]) if len(sys.argv) > 2 else 1883
    
    print(f"{BLUE}MQTT Brake Alert Monitor{RESET}")
    print(f"Connecting to broker at {broker_host}:{broker_port}")
    print("Press Ctrl+C to stop\n")
    
    # Create MQTT client
    client = mqtt.Client(client_id="brake_monitor_subscriber")
    
    # Set callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    try:
        # Connect to broker
        client.connect(broker_host, broker_port, 60)
        
        # Start loop
        client.loop_forever()
        
    except KeyboardInterrupt:
        print(f"\n{YELLOW}Stopping MQTT subscriber{RESET}")
        client.disconnect()
    except Exception as e:
        print(f"{RED}Error: {e}{RESET}")
        sys.exit(1)

if __name__ == "__main__":
    main()