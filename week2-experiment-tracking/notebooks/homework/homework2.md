# Homework for week 2 of the MLOps zoomcamp

Ferdinand Kleinschroth
(ferdinand.kleinschroth@gmx.de)

## Q1: Install MLflow

```console
> mlflow --version                                 
mlflow, version 1.26.1
```

## Q2: Download and preprocess the data

After running `preprocess_data.py` there are 4 files in the output folder.

## Q3: Train a model with autolog

There are 17 parameters automatically logged.

## Q4: Launch the tracking server locally

I used the following command:
```console
❯ mlflow ui --backend-store-uri sqlite:///mlflow.db --default-artifact-root artifacts
```

## Q5: Tune the hyperparameters of the model

After hyperparameter optimizing the best validation rmse was 6.626.

## Q6: Promote the best model to the model registry

The `test-rmse` of the best model is 6.547.