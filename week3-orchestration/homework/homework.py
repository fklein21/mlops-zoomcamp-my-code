from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from pathlib import Path
from urllib.request import urlretrieve
import pickle

import pandas as pd

from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

from prefect import get_run_logger, flow, task
from prefect.task_runners import SequentialTaskRunner
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


@task
def read_data(path):
    df = pd.read_parquet(path)
    return df

@task
def download_file(url):
    logger = get_run_logger()
    p = Path("data/")
    p.mkdir(exist_ok=True)
    file_path = p.resolve().as_posix() + '/' + url.split('/')[-1]
    logger.info(f"Downloading {url} to directory {file_path.split('/')[:-1]}.")
    if not Path(file_path).is_file():
        urlretrieve(url, file_path)


def save_model(model, dv, date=None):
    if date is None:
        date = datetime.strftime('%Y-%m-%d')
    p = Path("models/")
    p.mkdir(exist_ok=True)
    model_file = p.resolve().as_posix() + '/' + 'model-' + date + '.bin'
    with open(model_file, 'wb') as f_out:
        pickle.dump(model, f_out)
    dv_file = p.resolve().as_posix() + '/' + 'dv-' + date + '.bin'
    with open(dv_file, 'wb') as f_out:
        pickle.dump(dv, f_out)


@task
def prepare_features(df, categorical, train=True):
    logger = get_run_logger()
    df['duration'] = df.dropOff_datetime - df.pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60
    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    mean_duration = df.duration.mean()
    if train:
        logger.info(f"The mean duration of training is {mean_duration}")
    else:
        logger.info(f"The mean duration of validation is {mean_duration}")
    
    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    return df


@task
def train_model(df, categorical):

    logger = get_run_logger()
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts) 
    y_train = df.duration.values

    logger.info(f"The shape of X_train is {X_train.shape}")
    logger.info(f"The DictVectorizer has {len(dv.feature_names_)} features")

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred = lr.predict(X_train)
    mse = mean_squared_error(y_train, y_pred, squared=False)
    logger.info(f"The MSE of training is: {mse}")
    return lr, dv


@task
def run_model(df, categorical, dv, lr):
    logger = get_run_logger()
    val_dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(val_dicts) 
    y_pred = lr.predict(X_val)
    y_val = df.duration.values

    mse = mean_squared_error(y_val, y_pred, squared=False)
    logger.info(f"The MSE of validation is: {mse}")
    return


@task
def get_paths(date: str) -> list[str]:
    logger = get_run_logger()
    if date is not None:
        date = datetime.strptime(date, '%Y-%m-%d')
    else:
        date = datetime.now().date()
    logger.info(f"date: {date.strftime('%Y-%m')}")
    logger.info(f"date 2 months earlier: {(date+relativedelta(months=-2)).strftime('%Y-%m')}")
    url_base = "https://s3.amazonaws.com/nyc-tlc/trip+data/fhv_tripdata_"
    train_path = url_base + (date+relativedelta(months=-2)).strftime('%Y-%m') + ".parquet"
    val_path = url_base + (date+relativedelta(months=-1)).strftime('%Y-%m') + ".parquet"
    return train_path, val_path


@flow(task_runner=SequentialTaskRunner)
def main(date: str=None):
        # train_path: str = './data/fhv_tripdata_2021-01.parquet', 
        #    val_path: str = './data/fhv_tripdata_2021-02.parquet'):
    
    logger = get_run_logger()
    train_path, val_path = get_paths(date).result()
    logger.info(f"train_path: {train_path}, val_path: {val_path}")

    download_file(train_path)
    download_file(val_path)

    categorical = ['PUlocationID', 'DOlocationID']

    df_train = read_data(train_path)
    df_train_processed = prepare_features(df_train, categorical)

    df_val = read_data(val_path)
    df_val_processed = prepare_features(df_val, categorical, False)

    # train the model
    lr, dv = train_model(df_train_processed, categorical).result()
    run_model(df_val_processed, categorical, dv, lr)

    # save the model and the DictVectorizer
    save_model(lr, dv, date)


# main(date="2021-08-15")

DeploymentSpec(
    flow=main,
    name='cron-schedule-fhv-train-mid-of-month',
    schedule=CronSchedule(
        cron="0 9 15 * *",
        timezone="America/New_York"
    ),
    flow_runner=SubprocessFlowRunner(),
    tags=['ml', 'fhv']
)


