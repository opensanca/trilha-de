from google.cloud import storage

import base64
import json

gcs_client=storage.Client()
bucket = gcs_client.bucket('trilhadataeng')

def process_message(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    json_message = json.loads(pubsub_message)
    
    blob = bucket.blob(json_message['id']+'.json')

    blob.upload_from_string(pubsub_message, content_type="application/x-ndjson")
