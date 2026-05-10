import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList

source_id="PSU-PROD-STACK"

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
    # pass in vars for emails to work
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag=dag_factory.assemble_dag()

    deph_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_DEVICE_ENV_PROFILE_HISTORY_{env_for_deployment.upper()}"}
    deph_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="deph",env_vars=deph_vars)
    deph_task_dict=deph_task_list.get_ops()

    ch_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_CH_PROD_STACK_{env_for_deployment.upper()}"}
    ch_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="ch",env_vars=ch_vars)
    ch_task_dict=ch_task_list.get_ops()

    bm_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_BIZ_MODEL_PROD_STACK_{env_for_deployment.upper()}"}
    bm_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="biz_model",env_vars=bm_vars)
    bm_task_dict=bm_task_list.get_ops()

    bm_rs_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_BIZ_MODEL_RSLOAD_{env_for_deployment.upper()}"}
    bm_rs_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="bm_rsloader", previous_task_ids=[bm_task_dict["run_op"].task_id],env_vars=bm_rs_vars)
    bm_rs_task_dict=bm_rs_task_list.get_ops()
    bm_task_dict["run_op"]>>bm_rs_task_dict["branch_op"]

    psu_laser_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_LASER_SOL_PROD_STACK_{env_for_deployment.upper()}"}
    psu_laser_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="psu_laser",env_vars=psu_laser_vars)
    psu_laser_task_dict=psu_laser_task_list.get_ops()
    [bm_task_dict["run_op"], ch_task_dict["run_op"], deph_task_dict["run_op"]]>>psu_laser_task_dict["branch_op"]
    [bm_task_dict["skipped_op"], ch_task_dict["skipped_op"], deph_task_dict["skipped_op"]]>>psu_laser_task_dict["branch_op"]

    psu_laser_dune_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_LASER_DUNE_PROD_STACK_{env_for_deployment.upper()}"}
    psu_laser_dune_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="psu_laser_dune",env_vars=psu_laser_dune_vars)
    psu_laser_dune_task_dict=psu_laser_dune_task_list.get_ops()
    [bm_task_dict["run_op"], ch_task_dict["run_op"], deph_task_dict["run_op"]]>>psu_laser_dune_task_dict["branch_op"]
    [bm_task_dict["skipped_op"], ch_task_dict["skipped_op"], deph_task_dict["skipped_op"]]>>psu_laser_dune_task_dict["branch_op"]

    psu_laser_ya_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_LASER_YA_PROD_STACK_{env_for_deployment.upper()}"}
    psu_laser_ya_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="psu_laser_ya",env_vars=psu_laser_ya_vars)
    psu_laser_ya_task_dict=psu_laser_ya_task_list.get_ops()
    [bm_task_dict["run_op"], ch_task_dict["run_op"], deph_task_dict["run_op"]]>>psu_laser_ya_task_dict["branch_op"]
    [bm_task_dict["skipped_op"], ch_task_dict["skipped_op"], deph_task_dict["skipped_op"]]>>psu_laser_ya_task_dict["branch_op"]

    psu_ink_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_INK_FLEET_PROD_STACK_{env_for_deployment.upper()}"}
    psu_ink_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="psu_ink",env_vars=psu_ink_vars)
    psu_ink_task_dict=psu_ink_task_list.get_ops()
    [bm_task_dict["run_op"], ch_task_dict["run_op"], deph_task_dict["run_op"]]>>psu_ink_task_dict["branch_op"]
    [bm_task_dict["skipped_op"], ch_task_dict["skipped_op"], deph_task_dict["skipped_op"]]>>psu_ink_task_dict["branch_op"]

    psu_ink_dune_vars = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_PSU_INK_DUNE_PROD_STACK_{env_for_deployment.upper()}"}
    psu_ink_dune_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="psu_ink_dune",env_vars=psu_ink_dune_vars)
    psu_ink_dune_task_dict=psu_ink_dune_task_list.get_ops()
    [bm_task_dict["run_op"], ch_task_dict["run_op"], deph_task_dict["run_op"]]>>psu_ink_dune_task_dict["branch_op"]
    [bm_task_dict["skipped_op"], ch_task_dict["skipped_op"], deph_task_dict["skipped_op"]]>>psu_ink_dune_task_dict["branch_op"]

    globals()[dag.dag_id]=dag
