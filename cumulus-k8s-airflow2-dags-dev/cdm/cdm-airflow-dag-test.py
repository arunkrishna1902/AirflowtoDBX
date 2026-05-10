import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_testing import PeptoDAG,PeptoDatabricksTaskList

source_id="Test_DAG_PROD_STACK"

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

    env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_Test_DAG_PROD_STACK_{env_for_deployment.upper()}"}
    stdraw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="prod_stack",env_vars=env_vars_stdraw)
    stdraw_task_dict=stdraw_task_list.get_ops()
    # raw_task_dict["run_op"]>>stdraw_task_dict["branch_op"]
    # raw_task_dict["skipped_op"]>>stdraw_task_dict["branch_op"]

    env_vars_lake = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_Test_DAG_PROD_STACK_INK_{env_for_deployment.upper()}"}
    lake_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="ink", previous_task_ids=[stdraw_task_dict["run_op"].task_id],env_vars=env_vars_lake)
    lake_task_dict=lake_task_list.get_ops()
    stdraw_task_dict["skipped_op"]>>lake_task_dict["branch_op"]
    stdraw_task_dict["run_op"]>>lake_task_dict["branch_op"]
    
    env_vars_rs = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_Test_DAG_PROD_STACK_LAAER{env_for_deployment.upper()}"}
    rs_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="laser",previous_task_ids=[stdraw_task_dict["run_op"].task_id],env_vars=env_vars_rs)
    rs_task_dict=rs_task_list.get_ops()
    stdraw_task_dict["skipped_op"]>>rs_task_dict["branch_op"]
    stdraw_task_dict["run_op"]>>rs_task_dict["branch_op"] 


    globals()[dag.dag_id]=dag