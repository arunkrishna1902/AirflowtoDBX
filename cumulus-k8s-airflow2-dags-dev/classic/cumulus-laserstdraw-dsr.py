import json
from pathlib import Path
from datetime import datetime

import airflow
from airflow.models import DAG
from airflow.models import Variable

from libs.dataos import send_failure_email, send_success_email
from libs.dsr import dsr_factory

#DAG_DIR = Path(__file__).parent
#CONFIG_DIR = "dsr_configs"
#DAG_CONFIG_FILE_PATH = f'{DAG_DIR}/{CONFIG_DIR}'

#Load Team And Environment Specific DAG Variables
dsr_vars = json.loads(Variable.get('cumulus-laserstdraw-dsr'))
access_control = dsr_vars['default_access_control']

#DSR Image Configuration
image_tag = dsr_vars['dsr_image_tag']
image = Variable.get('ecr_repository') + f'dsr-delete-tools:{image_tag}'

# Input Vars simple
args = {
    'owner': 'Cumulus-DSR',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False
}

# Generate dag if based on environment
airflow_namespace=Variable.get("namespace")
if airflow_namespace=="airflow2-dev":
    envs=["dailyCI"] #,"DEV"
    env_for_deployment = "DAILY"
if airflow_namespace=="airflow2-itg":
    envs=["ITG"]
    env_for_deployment = "ITG"
if airflow_namespace=="airflow2-prod":
    envs=["PROD"]
    env_for_deployment = "PROD"

dag = DAG(
    dag_id= env_for_deployment+'_cumulus_dsr_laserstdraw',
    default_args=args,
    schedule_interval="30 13 2,16 * *",
    max_active_runs=1,
    concurrency=dsr_vars['dsr_configuration'].get('dsr_max_task_concurrency', 1),
    catchup=False,
    params={"email_to": "swathi.v.k@hp.com"},
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
    access_control=access_control,
    tags=['dsr', 'cumulus-dsr', 'east'],
    doc_md="""#DAG to DSR capabilities"""
)

sub_dag = dsr_factory.build_dsr_taskgroup(dag=dag,
                                          group_id='DSR-CleanRoom',
                                          image=image,
                                          dsr_vars=dsr_vars                                                                             
                                          )