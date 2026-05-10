
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


dag_name = "parquet_comparison_stdraw"

env_vars = Variable.get("cumulus-common-variables", deserialize_json=True)
env_vars.update(Variable.get("cumulus-parquet-comparison-stdraw-variables", deserialize_json=True))
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


data = json.loads(Variable.get("cumulus-parquet-comparison-stdraw-sources"))
notebook_params = {
    "environment": Variable.get("environment"),
    "newProductNumbers": None, # will be updated in the loop
    "testSwitch": None, # will be updated in the loop
    "sourceId": None, # will be updated in the loop
    "filterList": None, # will be updated in the loop
    "ToEmail": "ty.henningsen@hp.com,sujithkrishnan.pallath@hp.com,joshua.yim@hp.com,ken.tubbs@hp.com,krishna.vuyyuru@hp.com,shelly.reasoner@hp.com, dataos.cumulus.rcb@external.groups.hp.com, cumulus_dev@groups.hp.com"
}

laser_sources = ["jam_ledm_dpp", "jam_mhit_dpp", "wja_ledm_dpp", "wja_mhit_dpp", "pelaser_dpp", "pelaser_printernet", "hppk_ampv", "samsung_ampv", "printernet_mhit_dpp", "gotham_ledm_laser", "wppgen2_scheduled_laser"]

def gather_parquet_compare_tasks(data):
    command = "env && python parquet_validator.py"
    tasks = []
    for sourceId in data:

        env_vars["DAG_ID"] = data[sourceId]['dag']
        env_vars["TASK_ID"] = data[sourceId]['task']
        
        notebook_params["testSwitch"] = "ParquetCompare" if sourceId in laser_sources else "ParquetCompare,ProductRef"
        notebook_params["newProductNumbers"] = "" if sourceId in laser_sources else "28B49A,28B50A,28B54A,28B70A,28B71A,28B75A,28B97A,28B98A,28C02A,2DR21D,38W38A,4WF66A,28B55A,28B96A,19Z12A,28B99A,2H3N3A,6UU46A,6UU47A,6UU48A,8QT49A,CN596A,T8W88A"
        notebook_params["sourceId"] = sourceId
        notebook_params["filterList"] = "raw_lot_id,payload_id,rec_created_ts,stdraw_lot_id,stdraw_process_ts,record_id,sw_version,xml_surveysysinfo,xml_productusagedyn,xml_productconfigdyn,xml_consumableconfigdyn,xml_usagedatacollectiondyn,xml_usageservice,xml_complete,xml_deviceidentification,xml_deviceinformation,xml_deviceusageservice,xml_devicesuppliesservice,xml_fimservice,parser_results_innerxml,xml_printernet,eea_valve_flag,privacy_bases,privacy_detail_version,privacy_data_purposes,eea_strnec_flag" if sourceId in laser_sources else "raw_lot_id,payload_id,rec_created_ts,stdraw_lot_id,stdraw_process_ts,record_id,sw_version,xml_productusagedyn,xml_productconfigdyn,xml_consumableconfigdyn,xml_surveysysinfo,xml_usagedatacollectiondyn,geo2_ip_org,supply_eureka_detected_k_flag,supply_eureka_detected_c_flag,supply_eureka_detected_m_flag,supply_eureka_detected_y_flag,supply_first_failure_model_k,supply_first_failure_model_cmy,supply_first_failure_code_k,supply_first_failure_code_cmy,supply_second_failure_code_k,supply_second_failure_code_cmy,supply_t2_k_flag,supply_t2_c_flag,supply_t2_m_flag,supply_t2_y_flag,print_pages_5x5,fw_country_code,eea_valve_flag"
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
    
