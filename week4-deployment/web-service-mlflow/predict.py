from http import client
import pickle

import mlflow
from mlflow.tracking import MlflowClient

from flask import Flask, request, jsonify

MLFLOW_TRACKING_URI  = 'http://192.168.122.179:5000'
RUN_ID = '0085387cffc0432a8ee408dee53bfe38'

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

logged_model = f'runs://{RUN_ID}/model'
model = mlflow.pyfunc.load_model(logged_model)


def prepare_features(ride):
    features = {}
    features['PU_DO'] = '%s_%s' % (ride['PULocationID'], ride['DOLocationID'])
    features['trip_distance'] = ride['trip_distance']
    return features


def predict(features):
    pred = model.predict(features)
    return float(preds[0])


app = Flask('duration-prediction')


@app.route('/predict', methods=['POST'])
def predict_endpoint():
    ride = request.get_json()

    features = prepare_features(ride)
    pred = predict(features)

    result = {
        'duration': pred
    }
    print(result)

    return jsonify(result)


if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=9696)