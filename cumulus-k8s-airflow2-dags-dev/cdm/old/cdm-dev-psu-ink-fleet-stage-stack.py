import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList

source_id="PSU-INK-FLEET-STAGE-STACK"

# dag specific vars
vars = Variable.get("cumulus-cdm-common-variables", deserialize_json=True)
vars.update(Variable.get("cumulus-cdm-individual-dag-variables", deserialize_json=True).get(f"cumulus-{source_id.lower()}",{}))
env_vars = vars

airflow_namespace=Variable.get("namespace")
if airflow_namespace=="airflow2-dev":
    envs=["DEV"] #,"DEV"
    env_for_deployment = "DEV"
if airflow_namespace=="airflow2-itg":
    envs=["ITG","PRODSMOKEITG"]
    env_for_deployment = "ITG"
if airflow_namespace=="airflow2-prod":
    envs=["PROD"]
    env_for_deployment = "PROD"

for env in envs:     
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag=dag_factory.assemble_dag()

    env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_INK_FLEET_STAGE_STACK_{env_for_deployment.upper()}"}
    stdraw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="stage_stack",env_vars=env_vars_stdraw)
    stdraw_task_dict=stdraw_task_list.get_ops()

    globals()[dag.dag_id]=dag