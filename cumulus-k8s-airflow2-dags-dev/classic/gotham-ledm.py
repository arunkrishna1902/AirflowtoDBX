import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_test import PeptoDAG,PeptoDatabricksTaskList

source_id="GOTHAM_LEDM"

# dag specific vars
vars = Variable.get("cumulus-common-variables", deserialize_json=True)
vars.update(Variable.get(f"cumulus-{source_id.lower()}", deserialize_json=True))
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
    
    #INK
    env_vars_fix_raw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_FIX_RAW_{env_for_deployment.upper()}"}
    raw_task_fix_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_raw_fix",env_vars=env_vars_fix_raw)
    raw_task_fix_dict=raw_task_fix_list.get_ops()
    
    env_vars_raw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_RAW_{env_for_deployment.upper()}"}
    raw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_raw",env_vars=env_vars_raw)
    raw_task_dict=raw_task_list.get_ops()
    
    env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_STDRAW_{env_for_deployment.upper()}"}
    stdraw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_stdraw",previous_task_ids=[raw_task_dict["run_op"].task_id],env_vars=env_vars_stdraw)
    stdraw_task_dict=stdraw_task_list.get_ops()
    [raw_task_dict["run_op"], raw_task_fix_dict["run_op"]]>>stdraw_task_dict["branch_op"]
    [raw_task_dict["skipped_op"], raw_task_fix_dict["skipped_op"]]>>stdraw_task_dict["branch_op"]

    env_vars_stdraw_old = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_STDRAW_{env_for_deployment.upper()}_OLD"}
    stdraw_task_list_old=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_stdraw_old",previous_task_ids=[raw_task_dict["run_op"].task_id],env_vars=env_vars_stdraw_old)
    stdraw_task_dict_old=stdraw_task_list_old.get_ops()
    [raw_task_dict["run_op"], raw_task_fix_dict["run_op"]]>>stdraw_task_dict_old["branch_op"]
    [raw_task_dict["skipped_op"], raw_task_fix_dict["skipped_op"]]>>stdraw_task_dict_old["branch_op"]

    env_vars_lake = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_STDRAW_DELTA_LAKE_{env_for_deployment.upper()}"}
    lake_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_stdraw_delta_lake",previous_task_ids=[stdraw_task_dict_old["run_op"].task_id],env_vars=env_vars_lake)
    lake_task_dict=lake_task_list.get_ops()
    stdraw_task_dict_old["run_op"]>>lake_task_dict["branch_op"]
    stdraw_task_dict_old["skipped_op"]>>lake_task_dict["branch_op"]
    
    env_vars_rs = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_REDSHIFT_{env_for_deployment.upper()}"}
    rs_task_list = PeptoDatabricksTaskList(dag=dag, env=env, source_id=source_id, process="dps_redshiftloader",previous_task_ids=[stdraw_task_dict_old["run_op"].task_id],env_vars=env_vars_rs)
    rs_task_dict=rs_task_list.get_ops()
    stdraw_task_dict_old["run_op"]>>rs_task_dict["branch_op"] 
    stdraw_task_dict_old["skipped_op"]>>rs_task_dict["branch_op"]

    #LASER
    laser_env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_LASER_STDRAW_{env_for_deployment.upper()}"}
    laser_stdraw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="laser_dps_stdraw",previous_task_ids=[raw_task_dict["run_op"].task_id],env_vars=laser_env_vars_stdraw)
    laser_stdraw_task_dict=laser_stdraw_task_list.get_ops()
    [raw_task_dict["run_op"], raw_task_fix_dict["run_op"]]>>laser_stdraw_task_dict["branch_op"]
    [raw_task_dict["skipped_op"], raw_task_fix_dict["skipped_op"]]>>laser_stdraw_task_dict["branch_op"]

    laser_env_vars_stdraw_old = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_LASER_STDRAW_{env_for_deployment.upper()}_OLD"}
    laser_stdraw_task_list_old=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="laser_dps_stdraw_old",previous_task_ids=[raw_task_dict["run_op"].task_id],env_vars=laser_env_vars_stdraw_old)
    laser_stdraw_task_dict_old=laser_stdraw_task_list_old.get_ops()
    [raw_task_dict["run_op"], raw_task_fix_dict["run_op"]]>>laser_stdraw_task_dict_old["branch_op"]
    [raw_task_dict["skipped_op"], raw_task_fix_dict["skipped_op"]]>>laser_stdraw_task_dict_old["branch_op"]
    
    laser_env_vars_rs = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_LASER_REDSHIFT_{env_for_deployment.upper()}"}
    laser_rs_task_list = PeptoDatabricksTaskList(dag=dag, env=env, source_id=source_id, process="laser_dps_redshiftloader",previous_task_ids=[laser_stdraw_task_dict["run_op"].task_id],env_vars=laser_env_vars_rs)
    laser_rs_task_dict=laser_rs_task_list.get_ops()
    laser_stdraw_task_dict["run_op"]>>laser_rs_task_dict["branch_op"] 
    laser_stdraw_task_dict["skipped_op"]>>laser_rs_task_dict["branch_op"]

    laser_env_vars_lake = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_LASER_STDRAW_DELTA_LAKE_{env_for_deployment.upper()}"}
    laser_lake_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="laser_dps_stdraw_delta_lake",previous_task_ids=[laser_stdraw_task_dict_old["run_op"].task_id],env_vars=laser_env_vars_lake)
    laser_lake_task_dict=laser_lake_task_list.get_ops()
    laser_stdraw_task_dict_old["run_op"]>>laser_lake_task_dict["branch_op"]
    laser_stdraw_task_dict_old["skipped_op"]>>laser_lake_task_dict["branch_op"]
    
    env_vars_ocv = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_GOTHAM_LEDM_OCV_{env_for_deployment.upper()}"}
    ocv_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_ocv",previous_task_ids=[laser_stdraw_task_dict["run_op"].task_id],env_vars=env_vars_ocv,pool='ocv_pool')
    ocv_task_dict=ocv_task_list.get_ops()
    laser_stdraw_task_dict["run_op"]>>ocv_task_dict["branch_op"]
    laser_stdraw_task_dict["skipped_op"]>>ocv_task_dict["branch_op"]
    
