
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
from datetime import datetime
from airflow import settings
from sqlalchemy.orm.session import Session
from cumulus_libs.parquet_comparison_operator import get_parquet_compare_pod


dag_name = "parquet_comparison_raw"

env_vars = Variable.get("cumulus-common-variables", deserialize_json=True)
env_vars.update(Variable.get("cumulus-parquet-comparison-raw-variables", deserialize_json=True))
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


data = json.loads(Variable.get("cumulus-parquet-comparison-raw-sources"))
notebook_params = {
    "sendEmail": "False", 
    "joinColumn": "payload_hash",
    "environment": Variable.get("environment"),
    "filterList": "source_directory,field_modification_log,consumable_type_enum,duplicate_check_hash,payload_id,payload,raw_lot_id",
    "countTestSwitch": None, # will be updated in the loop
    "sourceId": None, # will be updated in the loop
    "metricsBucket": None, # will be set below based on env,
    "metricsPath": None, # will be updated in the loop
    "toEmail": None # will be updated in the loop
}
if Variable.get("environment") == "DEV":
    notebook_params["metricsBucket"] = "hp-bigdata-databricks-metrics-testresults"
else:
    notebook_params["metricsBucket"] = f"hp-bigdata-databricks-{Variable.get('environment').lower()}-metrics-testresults"
    
    

countTestSwitchList = ["wja_ledm_dpp", "wja_mhit_dpp", "pe_laser_dpp", "pe_laser_printernet", "samsung_ampv", "hppk_ampv", "gotham_ledm"]
email_list = {
  "ink_sources" : "ty.henningsen@hp.com,sujithkrishnan.pallath@hp.com,joshua.yim@hp.com,krishna.vuyyuru@hp.com,mohan.gonnabathula1@hp.com,phani.kiran.maddukuri@hp.com,ravi.malhotra@hp.com,shelly.reasoner@hp.com,dataos.cumulus.rcb@external.groups.hp.com,cumulus_dev@groups.hp.com",
  "laser_sources" : "ty.henningsen@hp.com,sujithkrishnan.pallath@hp.com,krishna.vuyyuru@hp.com,mohan.gonnabathula1@hp.com,shelly.reasoner@hp.com,phani.kiran.maddukuri@hp.com,ravi.malhotra@hp.com,dataos.cumulus.rcb@external.groups.hp.com,cumulus_dev@groups.hp.com",
}


def gather_parquet_compare_tasks(data):
    command = "python parquet_validator.py"
    tasks = []
    for source in data:
        for sourceId in data[source]:

            env_vars["DAG_ID"] = data[source][sourceId]['dag']
            env_vars["TASK_ID"] = data[source][sourceId]['task']
            
            notebook_params["sourceId"] = sourceId
            notebook_params["metricsPath"] = data[source][sourceId]['metrics']
            notebook_params["countTestSwitch"] = str(sourceId in countTestSwitchList).lower()
            notebook_params["toEmail"] = email_list[source]
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
    
