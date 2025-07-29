import json
import base64
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('fraud-transactions')  

S3_BUCKET = 'raw-customer-transactions-dev'  

def lambda_handler(event, context):
    for record in event['Records']:
        payload = base64.b64decode(record["kinesis"]["data"]).decode('utf-8')
        txn = json.loads(payload)
        print("🔹 Incoming Transaction:", txn)

        # Save raw transaction to S3
        save_to_s3(txn)

        # Apply simple fraud logic
        amount = txn.get("amount", 0)
        if amount > 8000:
            txn["fraud_flag"] = True
            save_to_dynamodb(txn)

def save_to_s3(transaction):
    try:
        key = f"transactions/{datetime.utcnow().strftime('%Y/%m/%d')}/{uuid.uuid4()}.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(transaction),
            ContentType='application/json'
        )
        print(f"✅ Raw transaction saved to S3: {key}")
    except Exception as e:
        print(f"❌ Error saving to S3: {e}")

def save_to_dynamodb(transaction):
    try:
        table.put_item(Item=transaction)
        print("✅ Fraud transaction saved to DynamoDB")
    except Exception as e:
        print(f"❌ Error saving to DynamoDB: {e}")




