import json
import boto3
import requests
import logging
import airflow
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable, DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from libs.dataos import send_failure_email, send_success_email
from cumulus_libs.aws_support import aws_secrets_manager_get_secret
from datetime import datetime, timedelta
from airflow import settings
from sqlalchemy.orm.session import Session
from cumulus_libs.parquet_comparison_operator import get_parquet_compare_pod

dag_name = "parquet_comparison_cdm"

env_vars = Variable.get("cumulus-common-variables", deserialize_json=True)
env_vars.update(Variable.get("cumulus-parquet-comparison-cdm-variables", deserialize_json=True))
# --> note that job id comes from the variables here

image = f"{Variable.get('ecr_repository')}{env_vars.get('ecr_image')}"

args = {
    'owner': 'airflow pepto',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
    'depends_on_past': False
}

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    schedule_interval=env_vars.get("schedule"),
    params={"email_to": "ryan.schreiber@hp.com"},
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
    access_control = {
        'cumulus-operator': ['can_read','can_edit'],
        'cumulus-admin': ['can_read','can_edit'],
    },
)

comparison_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
data = json.loads(Variable.get("cumulus-parquet-comparison-cdm-sources"))
environment = Variable.get("environment")
notebook_params = {
    "startDate": comparison_date,
    "endDate": comparison_date,
    "switch": "Parquet",
    "pathA": None,
    "pathB": None,
    "joinColumns": None,
    "fromEmail": "databricks.itg@hp8.us",
    "emailTitle": None,
    "ToEmail": "ty.henningsen@hp.com,sujithkrishnan.pallath@hp.com,joshua.yim@hp.com,erica.cho@hp.com,krishna.vuyyuru@hp.com"
}

def gather_parquet_compare_tasks(data):
    command = "env && python parquet_validator.py"
    tasks = []
    for sourceId in data:
        if sourceId == "lfp-aerd":
            env_vars["DAG_ID"] = data[sourceId]['dag']
            env_vars["TASK_ID"] = data[sourceId]['task']

            notebook_params["pathA"] = "s3a://gbd-lf-data-lake-prod-cdm-intermediate/all-events-raw-data"
            notebook_params["pathB"] = "s3a://gbd-lf-data-lake-daily-cdm-intermediate/all-events-raw-data" if environment == "dev" else "s3a://gbd-lf-data-lake-stage-cdm-intermediate/all-events-raw-data"
            notebook_params["joinColumns"] = "account,project,event_id,receive_date"
            notebook_params['emailTitle'] = "LFP AERD Parquet Compare"
            notebook_params["whereClause"] = "receive_date = '%s'"%(comparison_date)
            env_vars["NOTEBOOK_PARAMS"] = json.dumps(notebook_params)
        elif sourceId == "sh":
            env_vars["DAG_ID"] = data[sourceId]['dag']
            env_vars["TASK_ID"] = data[sourceId]['task']

            notebook_params["pathA"] = "s3a://hp-bigdata-databricks-prod/supply_history/delta_lake/PROD_STACK/"
            notebook_params["pathB"] = "s3a://hp-bigdata-databricks/CDM/supplies_history/delta_lake/PROD_STACK/" if environment == "dev" else "s3a://hp-bigdata-databricks-itg/CDM/supplies_history/delta_lake/PROD_STACK/"
            notebook_params["joinColumns"] = "eventMeta.eventMetaDetail.eventId"
            notebook_params['emailTitle'] = "SH Parquet Compare"
            notebook_params["whereClause"] = "receive_date = '%s'"%(comparison_date)
            env_vars["NOTEBOOK_PARAMS"] = json.dumps(notebook_params)
            
        tasks.append(get_parquet_compare_pod(
            dag = dag,
            name = dag_name + "-" + sourceId.lower(),
            namespace = Variable.get("namespace"),
            image = image,
            command = command,
            env_vars = env_vars,
            service_account_name = env_vars.get("service_account_name"),
        ))
            
    return tasks


## chaining tasks, running all of these in series
tasks = gather_parquet_compare_tasks(data)
for i in range(len(tasks)):
  if i not in [0]: 
    tasks[i-1] >> tasks[i]
