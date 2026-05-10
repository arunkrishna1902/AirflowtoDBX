from airflow.models import Variable

from cumulus_libs.peptobase_cdm import PeptoDAG, PeptoDatabricksTaskList

source_id = "SANDBOX-YSERHIICHUK-DUMMY-JOB"

# dag specific vars
dag_vars = Variable.get("cumulus-cdm-common-variables", deserialize_json=True)
dag_vars.update(Variable.get("cumulus-cdm-individual-dag-variables", deserialize_json=True)
                .get(f"cumulus-{source_id.lower()}", {}))

env_vars = dag_vars

airflow_namespace = "airflow2-dev"
envs = ["DEV"]  # ,"DEV"
env_for_deployment = "DEV"

for env in envs:

    dag = PeptoDAG(source_id=source_id, env=env, vars=env_vars).assemble_dag()

    env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME": f"SANDBOX_YSERHIICHUK_DUMMY_JOB_PIE_STACK_{env_for_deployment.upper()}"}

    stdraw_task_list = PeptoDatabricksTaskList(dag=dag,
                                               env=env,
                                               source_id=source_id,
                                               process="pie_stack",
                                               env_vars=env_vars_stdraw)

    stdraw_task_dict = stdraw_task_list.get_ops()

    globals()[dag.dag_id] = dag
