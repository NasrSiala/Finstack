import sys
import json
import subprocess
import os
from kafka_producer import FinceptKafkaProducer

def main():
    # 1. Capture arguments
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python market_data_kafka_bridge.py <script_name> <args...>"}))
        return

    script_to_run = sys.argv[1]
    script_args = sys.argv[2:]

    # 2. Run the actual data script
    try:
        # Construct path to the script (assume it's in the same directory)
        script_path = os.path.join(os.path.dirname(__file__), script_to_run)
        
        result = subprocess.run(
            [sys.executable, script_path] + script_args,
            capture_output=True,
            text=True,
            check=True
        )
        
        raw_output = result.stdout.strip()
        
        # 3. Print output immediately so terminal doesn't wait
        print(raw_output)
        
        # 4. Asynchronously (sort of) push to Kafka
        try:
            data = json.loads(raw_output)
            
            # Initialize producer
            producer = FinceptKafkaProducer()
            
            # Determine topic
            topic = "market-data-minute"
            
            # Extract data payload
            payload = None
            if isinstance(data, dict):
                # Handle standardized output
                if "data" in data and "success" in data:
                    payload = data["data"]
                    # Handle table format
                    if isinstance(payload, dict) and payload.get("type") == "table":
                        payload = payload.get("rows", [])
                else:
                    payload = data
            elif isinstance(data, list):
                payload = data
                
            if payload:
                producer.produce(topic, payload)
                producer.flush()
                
        except Exception:
            # Silently fail Kafka push to not break the terminal
            pass
            
    except subprocess.CalledProcessError as e:
        print(e.stdout)
        print(e.stderr, file=sys.stderr)
        sys.exit(e.returncode)
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)

if __name__ == "__main__":
    main()
