# SNCB Brake Monitoring Test Setup

This guide helps you test the SNCB brake monitoring query with TCP source and MQTT sink.

## Architecture

```
TCP Generator → NebulaStream → MQTT Broker → MQTT Subscriber
(Docker)         (Docker)        (Docker)     (Mac Host)
```

## Prerequisites

- Docker and Docker Compose installed
- Python 3 with paho-mqtt: `pip install paho-mqtt`

## Quick Start

### 1. Start MQTT Broker in Docker

```bash
# Start only the MQTT broker
docker-compose up -d mosquitto
```

### 2. Start MQTT Subscriber on Mac

```bash
# From your Mac terminal
cd Tests
chmod +x mqtt_subscriber.py
./mqtt_subscriber.py localhost 1883
```

### 3. Start NebulaStream with Query

In another terminal:
```bash
# Start NebulaStream coordinator and worker
docker-compose up -d nes-coordinator nes-worker

# Deploy the brake monitoring query
# (You'll need to use NebulaStream's REST API or UI to deploy the query from Queries/sncb_brake_monitoring.yaml)
```

### 4. Generate Test Data

```bash
# From your Mac, send data to NebulaStream's TCP source
cd Tests
chmod +x tcp_data_generator.py

# Connect to NebulaStream worker's TCP port
./tcp_data_generator.py localhost 9000 3
```

## Scripts

### tcp_data_generator.py

Generates realistic brake sensor data:
- Simulates multiple train devices
- Normal operation with occasional high variance events (10% chance)
- Sends CSV formatted data over TCP

Usage:
```bash
./tcp_data_generator.py [host] [port] [device_count]
# Default: localhost 9000 3
```

### mqtt_subscriber.py

Monitors MQTT broker for brake alerts:
- Subscribes to `sncb/brake/alerts` topic
- Displays alerts with color coding
- Shows alert details and timestamps

Usage:
```bash
./mqtt_subscriber.py [broker_host] [broker_port]
# Default: localhost 1883
```

## Expected Behavior

1. **Normal Operation**: Most data points have low variance
2. **Alert Triggers**: When PCFA variance > 0.4 AND PCFF variance > 1.0
3. **Alert Frequency**: ~10% of 30-second windows should trigger alerts
4. **Alert Content**: Contains device_id, start/end timestamps, and variance values

## Troubleshooting

- **No TCP Connection**: Ensure NebulaStream worker is running and TCP source is configured on port 9000
- **No MQTT Messages**: Check that the query is deployed and MQTT sink is configured correctly
- **Connection Refused**: Verify Docker containers are running with `docker ps`

## Testing High Variance

The data generator has a 10% chance of generating high variance data. To force high variance for testing, modify line 17 in tcp_data_generator.py:
```python
if random.random() < 0.9:  # Change to 90% chance
```