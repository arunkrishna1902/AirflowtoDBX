import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_test import PeptoDAG,PeptoDatabricksTaskList

source_id = "PEINK-XREF"

# dag specific vars
vars = Variable.get("cumulus-common-variables", deserialize_json=True)
vars.update(Variable.get(f"cumulus-{source_id.lower()}", deserialize_json=True))
env_vars = vars

schedule = env_vars.get("schedule")
if not schedule:
    schedule = None

command = env_vars.get("run_command")

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
    
for env in envs:     
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag=dag_factory.assemble_dag()
    
    #INK
    env_vars_raw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PEINK_STDRAW_XREF_HARVEST_WEEKLY_{env_for_deployment.upper()}"}
    raw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_stdraw_xref_harvest",super_package="ink_stdraw_pepto",package="dps_stdraw_xref_harvest",env_vars=env_vars_raw)
    raw_task_dict=raw_task_list.get_ops()
    
