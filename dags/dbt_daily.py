from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

#Docker image to run
IMAGE = "europe-north1-docker.pkg.dev/project-e075d482-cb78-4b68-a53/dbt/dbt-analytics:1.0.0"

#Define resource requirements
resources = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},      # Represents the guaranteed minimum, m stands for mill-cores(1000m = 1 full CPU core), 512Mi = 512 Mebibytes(~536 Megabytes)
    limits={"cpu": "1000m", "memory": "1Gi"},         # The Hard Ceiling, 1Gi = 1 Gibibyte(~1.07 Gigabytes), if dbt pod accumulates more data in memory than this limit, Kubernetes will kill it with an OOMKilled(Out of Memory) error
)

def dbt_task(task_id, args):
    return KubernetesPodOperator(
        task_id=task_id,  # The task's name in the Airflow UI
        name=task_id.replace("_", "-"), # The pod's name in Kubernetes, which cannot contain underscores, hence the replace
        namespace="composer-user-workloads", # The area of the cluster where the pod runs
        config_file="/home/airflow/composer_kube_config", # config_file and kubernetes_conn_id represent how airflow authenticates to cluster
        kubernetes_conn_id="kubernetes_default",
        image=IMAGE, # Which image to start
        arguments=args, # the dbt command argument, replacing CMD
        env_vars={"GCP_PROJECT": "project-e075d482-cb78-4b68-a53", "DBT_DATASET": "analytics"},
        container_resources=resources,
        get_logs=True,  #Copies the pod's output into the airflow log
        on_finish_action="delete_pod",  #delete_pod removes the pod when it finishes
        startup_timeout_seconds=300,  #How long to wait for the pod to start before failing the task
    )

    start_pipeline = EmptyOperator(task_id="start")
    end_pipeline = EmptyOperator(task_id="end")

with DAG(
    dag_id="dbt_daily", # The DAG's name in the UI
    start_date=datetime(2026, 1, 1), # The earliest date the schedule applies from, historical date is fine
    schedule=None, # Only manual run
    catchup=False, # Don't run all the missed days between start_date and today
):
    dbt_run = dbt_task("dbt_run", ["run"])
    start_pipeline = EmptyOperator(task_id="start")
    end_pipeline = EmptyOperator(task_id="end")
    start_pipeline >> dbt_run >> end_pipeline

