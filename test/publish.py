import json
import os
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../src/pipeline', '.env'))
from google.cloud import pubsub_v1

# Initialize Pub/Sub client
project_id = os.environ.get('GCP_PROJECT_ID')
topic_id = os.environ.get('GCP_TOPIC_ID')
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

# Function to publish a message to Pub/Sub
def publish_to_pubsub(message):
    message_data = json.dumps(message).encode('utf-8')
    future = publisher.publish(topic_path, message_data)
    print(f'Published message ID: {future.result()}')

def read_data_from_file(file_path):
    with open(file_path, 'r') as file:
        return json.load(file)

def modify_and_publish(file_path):
    json_data = read_data_from_file(file_path)
    
    modify_from = int(len(json_data)/2)
    modified_data = json_data[modify_from:]  # Take the second half of the data
    original_data = json_data[:modify_from]  # Take the first half of the data

    for entry in original_data:
        # Publish the original entry to Pub/Sub
        publish_to_pubsub(entry)
        print(f"Published original entry: {entry['name']}")
    
    for entry in modified_data:
        entry["lang"] = entry["language"]
        del entry["language"]
        
        # Publish the modified entry to Pub/Sub
        publish_to_pubsub(entry)
        print(f"Published modified entry: {entry['name']}")

if __name__ == '__main__':
    # Provide the path to your JSON file containing the firewall rules
    file_path = 'data.json'
    
    # Replicate and publish the rules
    modify_and_publish(file_path)
