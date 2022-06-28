from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessFlowRunner


DeploymentSpec(
    flow_location="score.py",
    name="ride_duration_prediction",
    parameters={
        "taxi_type": "green",
        "run_id": "8a740ec346824dc49fe2767cc66dfe64",
    },
    flow_storage="00e5deaf-4082-49f2-bc16-468f546bea2f",
    schedule=CronSchedule(cron="0 3 2 * *"),
    flow_runner=SubprocessFlowRunner(),
    tags=['ml']
)
