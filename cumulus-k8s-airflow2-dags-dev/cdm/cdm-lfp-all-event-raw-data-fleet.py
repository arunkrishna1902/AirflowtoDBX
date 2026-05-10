import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList,trigger_downstream_dag

source_id="LFP_ALL_EVENT_RAW_DATA_FLEET"

# dag specific vars
vars = Variable.get("cumulus-cdm-common-variables", deserialize_json=True)
vars.update(Variable.get("cumulus-cdm-individual-dag-variables", deserialize_json=True).get(f"cumulus-{source_id.lower()}",{}))
env_vars = vars
access = [
    "cumulus-operator",
    "cumulus-admin",
    "lf-operator",
    "lf-admin",
]

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

    
def branch_to_trigger(dag_id_to_check, task_id_true, task_id_false):
    return task_id_true if is_dag_paused(dag_id_to_check) else task_id_false

downstream_dags = ["cumulus-xcom-test"]

for env in envs:     
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars,access_control=access)
    dag=dag_factory.assemble_dag()

    env_vars_aerd = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_LFP_ALL_EVENTS_RAW_DATA_FLEET_{env_for_deployment.upper()}"}
    lfp_aerd_dict=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="prod_stack",env_vars=env_vars_aerd,team="lf",)
    lfp_aerd_dict=lfp_aerd_dict.get_ops()

    # trigger downstream tasks for lfp udm pipelines
#     for downstream_dag in downstream_dags:
#         trigger_downstream_dag(dag, env, lfp_aerd_dict["run_op"], downstream_dag)
        

    globals()[dag.dag_id]=dag
