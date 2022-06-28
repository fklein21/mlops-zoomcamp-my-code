#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
from datetime import datetime, date
from pathlib import Path
import pandas as pd



def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    categorical = ['PUlocationID', 'DOlocationID']
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df



def load_model(model_file: str):
    # dv, lr
    with open(model_file, 'rb') as f_in:
        dv, lr = pickle.load(f_in)
    return dv, lr


def save_results(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['prediction'] = y_pred

    df_result.to_parquet(
        output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )


def upload_results(df, y_pred, run_date, taxi_type):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['prediction'] = y_pred

    remote_output_file = f's3://mlflow-artifacts-remote-week04/taxi_type={taxi_type}/year={run_date.year:04d}/month={run_date.month:02d}/{taxi_type}_duration_prediction.parquet'

    print(f'Remote output file: {remote_output_file}')
    df_result.to_parquet(
        remote_output_file,
        engine='pyarrow',
        compression=None,
        index=False
    )


def apply_model(
        model_file: str,
        input_file: str,
        run_date: datetime,
        output_file: str):
    print(f'Loading the date from file {input_file} ...')
    df = read_data(input_file)

    df['ride_id'] = f'{run_date.year:04d}/{run_date.month:02d}_' + df.index.astype('str')

    print(f'Loading the model from file {model_file} ...')
    dv, lr = load_model(model_file)

    categorical = ['PUlocationID', 'DOlocationID']
    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)

    print(f'Applying the model ...')
    y_pred = lr.predict(X_val)

    y_pred_mean = y_pred.mean()
    print(f'The mean predicted duration is {y_pred_mean}.')

    print(f'Saving the results fo file {output_file} ...')
    save_results(df, y_pred, output_file)

    print(f'Uploading the results fo s3 ...')
    upload_results(df, y_pred, run_date, 'fhv')


def get_paths(run_date: datetime):
    input_file = f'https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_{run_date.year:04d}-{run_date.month:02d}.parquet'
    output_file = f'output/fhv_tripdata_{run_date.year:04d}-{run_date.month:02d}_prediction.parquet'
    Path(output_file).resolve().parent.mkdir(parents=True, exist_ok=True)

    return input_file, output_file


def ride_duration_prediction(
    run_date: datetime = None):
    if run_date is None:
        run_date = date.today()


def run():
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    run_date = datetime(year=year, month=month, day=1)

    input_file, output_file = get_paths(run_date)

    model_file = 'model.bin'

    apply_model(
        model_file=model_file,
        input_file=input_file,
        run_date=run_date,
        output_file=output_file
    )



if __name__ == '__main__':
    run()
    