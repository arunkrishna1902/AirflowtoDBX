import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList

source_id="SH_DUNE"

# dag specific vars
vars = Variable.get("cumulus-cdm-common-variables", deserialize_json=True)
vars.update(Variable.get("cumulus-cdm-individual-dag-variables", deserialize_json=True).get(f"cumulus-{source_id.lower()}",{}))
env_vars = vars


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

    prod_env_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_SH_DUNE_PROD_STACK_{env_for_deployment.upper()}"}
    prod_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="prod_stack",env_vars=prod_env_vars)
    prod_task_dict=prod_task_list.get_ops()

    stg_env_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_SH_DUNE_STAGE_STACK_{env_for_deployment.upper()}"}
    stg_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="stage_stack",env_vars=stg_env_vars)
    stg_task_dict=stg_task_list.get_ops()

    # prod_task_dict["run_op"]>>stg_task_dict["branch_op"]
    # prod_task_dict["skipped_op"]>>stg_task_dict["branch_op"]

    globals()[dag.dag_id]=dag

