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

# Development Only
# Load Custom Airflow variables from a json sting variable in Airflow
#dag_config_file_path = f'classic\cumulus-inkstdraw-test.json'
#f = open(dag_config_file_path, "r")
#airflow_trident_vars = json.loads(f.read())
## End Development Hooks

#Load Team And Environment Specific DAG Variables
dsr_vars = json.loads(Variable.get('cumulus-inkstdraw-test'))
access_control = dsr_vars['default_access_control']

#DSR Image Configuration
image_tag = dsr_vars['dsr_image_tag']
image = Variable.get('ecr_repository') + f'dsr-delete-tools:{image_tag}'

# Input Vars simple
args = {
    'owner': 'Cumulus-DSR-Test',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False
}

dag = DAG(
    dag_id='cumulus__dev_inkstdraw',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    concurrency=dsr_vars['dsr_configuration'].get('dsr_max_task_concurrency', 1),
    catchup=False,
    params={"email_to": "swathi.v.k@hp.com"},
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
    access_control=access_control,
    tags=['dsr', 'cumulus-dsr-test', 'east'],
    doc_md="""#DAG to DSR capabilities"""
)

sub_dag = dsr_factory.build_dsr_taskgroup(dag=dag,
                                          group_id='DSR',
                                          image=image,
                                          dsr_vars=dsr_vars,
                                          run_type='databricks',                                     
                                          )