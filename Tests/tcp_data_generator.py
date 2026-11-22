#!/usr/bin/env python3
import socket
import time
import random
import sys
from datetime import datetime

def generate_brake_data(device_id):
    """Generate realistic brake pressure data with occasional variance spikes"""
    # Base values
    pcfa_base = 1013.25  # mbar
    pcff_base = 1013.25  # mbar
    
    # Add some variance - occasionally trigger alert conditions
    if random.random() < 0.1:  # 10% chance of high variance
        pcfa = pcfa_base + random.gauss(0, 5)  # High variance
        pcff = pcff_base + random.gauss(0, 8)  # High variance
    else:
        pcfa = pcfa_base + random.gauss(0, 0.5)  # Normal variance
        pcff = pcff_base + random.gauss(0, 0.8)  # Normal variance
    
    # Generate other sensor data
    time_utc = int(time.time() * 1000)  # milliseconds
    vbat = 24.0 + random.uniform(-0.5, 0.5)
    pcf1 = 1013.25 + random.gauss(0, 1)
    pcf2 = 1013.25 + random.gauss(0, 1)
    t1 = 20.0 + random.uniform(-5, 5)
    t2 = 20.0 + random.uniform(-5, 5)
    code1 = 0.0
    code2 = 0.0
    gps_speed = 80.0 + random.uniform(-20, 20)
    gps_lat = 50.8503 + random.uniform(-0.01, 0.01)
    gps_lon = 4.3517 + random.uniform(-0.01, 0.01)
    
    # Format as CSV
    return f"{time_utc},{device_id},{vbat:.2f},{pcfa:.2f},{pcff:.2f},{pcf1:.2f},{pcf2:.2f},{t1:.2f},{t2:.2f},{code1:.1f},{code2:.1f},{gps_speed:.2f},{gps_lat:.6f},{gps_lon:.6f}\n"

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9000
    device_count = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    
    print(f"Connecting to {host}:{port}")
    print(f"Simulating {device_count} devices")
    print("Press Ctrl+C to stop")
    
    while True:
        try:
            # Create socket connection
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, port))
            print(f"Connected to {host}:{port}")
            
            device_ids = list(range(1000, 1000 + device_count))
            
            while True:
                for device_id in device_ids:
                    data = generate_brake_data(device_id)
                    sock.sendall(data.encode())
                    print(f"Sent: {data.strip()}")
                
                time.sleep(0.5)  # Send data every 500ms
                
        except socket.error as e:
            print(f"Socket error: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(5)
        except KeyboardInterrupt:
            print("\nStopping data generator")
            break
        finally:
            sock.close()

if __name__ == "__main__":
    main()