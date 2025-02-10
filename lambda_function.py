import json
import boto3
import pymongo
import os

def get_mongodb_collection():
    db_name = os.environ.get("databaseName")
    mongodb_uri = os.environ.get("mongoUri")
    collection_name = os.environ.get("collection")

    mongo_client = pymongo.MongoClient(mongodb_uri)
    db = mongo_client[db_name]
    collection = db[collection_name]

    return collection

def get_s3_client():
    s3_client = boto3.client('s3')
    return s3_client
    
def get_sqs_client():
    sqs_client = boto3.client('sqs')
    return sqs_client

def get_sqs_url():
    sqs_url = os.environ.get("sqsUrl")
    return sqs_url


def lambda_handler(event, context):

    for record in event['Records']:
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        s3_client = get_s3_client()

        metadata_response = s3_client.head_object(Bucket=bucket_name, Key=object_key)

        metadata = metadata_response.get("Metadata", {})
        arquivo_id = metadata.get("arquivo-id", None)

        collection = get_mongodb_collection()
        
        resultado = collection.find_one({"_id": arquivo_id})

        if resultado:
        
            message_body = {
                "id": arquivo_id,
                "keyVideo": object_key,
                "keyZip": "images/" + arquivo_id + ".zip",
                "intervalInSeconds": resultado["intervalo"]
            }
            
            sqs_client = get_sqs_client()
            sqs_url = get_sqs_url()

        
            response = sqs_client.send_message(
                QueueUrl=sqs_url,
                MessageBody=json.dumps(message_body)
            )

            print(f"Mensagem enviada para fila: {response['MessageId']}")

    return {
        "statusCode": 200,
        "body": "Processamento concluído!"
    }
