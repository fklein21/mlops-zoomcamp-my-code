import os
import json
import boto3
import base64

from sklearn.feature_extraction import DictVectorizer
from sklearn.ensemble import RandomForestRegressor

import mlflow


# kinesis_client = boto3.client(
#     'kinesis',
#     aws_access_key_id=os.getenv('ACCESS_KEY'),
#     aws_secret_access_key=os.getenv('SECRET_KEY'),)
kinesis_client = boto3.client('kinesis',)

PREDICTIONS_STREAM_NAME = os.getenv('PREDICTIONS_STREAM_NAME', 'ride_predictions')


RUN_ID = os.getenv('RUN_ID') # 8a740ec346824dc49fe2767cc66dfe64/

logged_model = f's3://mlflow-artifacts-remote-week04/1/{RUN_ID}/artifacts/model'
print(f"logged_model: {logged_model}")
model = mlflow.pyfunc.load_model(logged_model)


TEST_RUN = os.getenv('TEST_RUN', False) == 'True'


def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    pred = model.predict(features)
    return float(pred[0])


def lambda_handler(event, context):
    # ride = event['ride']
    # ride_id = event['ride_id']
    
    # features = prepare_features(ride)
    # prediction = predict(features)

    print(json.dumps(event))
    
    predictions_events = []
    
    for record in event['Records']:
        encoded_data = record['kinesis']['data']
        decoded_data = base64.b64decode(encoded_data).decode('utf-8')
        ride_event = json.loads(decoded_data)
        print(ride_event)
        
        ride = ride_event['ride']
        ride_id = ride_event['ride_id']
        
        features = prepare_features(ride)
        prediction = predict(features)
        
        prediction_event = {
            'model': 'ride_duration_prediction_model',
            'version': '123',
            'prediction': {
                'ride_duration': prediction,
                'ride_id': ride_id
            }
        }
        
        print(prediction_event)
        print(f"PREDICTIONS_STREAM_NAME: {PREDICTIONS_STREAM_NAME}") 
        
        print(f"TEST_RUN: {TEST_RUN}")
        if not TEST_RUN:
            kinesis_client.put_record(
                StreamName=PREDICTIONS_STREAM_NAME,
                Data=json.dumps(prediction_event),
                PartitionKey=str(ride_id)
            )
        
        # response = kinesis_client.put_record(
        #     StreamName=PREDICTIONS_STREAM_NAME,
        #     Data=json.dumps(prediction_event),
        #     PartitionKey=str(ride_id)
        # )
        # print(f"response: {response}")
        
        predictions_events.append(prediction_event)
        
    return {
        'predictions': predictions_events
    }



# Command in terminal:
# export PREDICTIONS_STREAM_NAME="ride_predictions"
# export RUN_ID="8a740ec346824dc49fe2767cc66dfe64"
# export TEST_RUN="True"

# python test.py
