from kafka import KafkaConsumer
import json

print("Starting consumer...")

consumer = KafkaConsumer(
    'stock-quotes',
    bootstrap_servers=['localhost:9092'],
    group_id='my-test-group',              
    auto_offset_reset='earliest',          
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    key_deserializer=lambda m: m.decode('utf-8') if m else None,
    consumer_timeout_ms=30000     
)

print("Listening for messages... Press Ctrl+C to stop")

try:
    for message in consumer:
        print(f"Offset: {message.offset}, Key: {message.key}, Value: {message.value}")
except KeyboardInterrupt:
    print("Stopping...")
except Exception as e:
    print(f"Error: {e}")
finally:
    consumer.close()