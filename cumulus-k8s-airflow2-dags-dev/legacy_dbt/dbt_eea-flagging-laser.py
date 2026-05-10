import json

import airflow
from airflow.models import DAG
from airflow.models import Variable

# from airflow.operators.subdag_operator import SubDagOperator

from libs.dataos import send_failure_email, send_success_email
import dbt.dbt_eea_factory_flagging_laser as eea_factory

# Load trident variables from a json sting variable in Airflow
cumulus_vars = json.loads(Variable.get("cumulus-dbt_eea"))

# add cumulus_vars with cumulus-common-variables for vars used by databricks trigger
cumulus_vars["dbx_vars"] = Variable.get(
    "cumulus-common-variables", deserialize_json=True
)
cumulus_vars["dbx_vars"]["job_id"] = cumulus_vars["JOB_ID"]
cumulus_vars["dbx_vars"]["previous_task_ids"] = ""
cumulus_vars["dbx_vars"]["source_id"] = ""
cumulus_vars["dbx_vars"]["env"] = (
    "ITG"
    if (cumulus_vars["env_vars"]["target"]).upper() == "STG"
    else (cumulus_vars["env_vars"]["target"]).upper()
)
cumulus_vars["dbx_vars"]["run_params"] = json.dumps({"environment":cumulus_vars["dbx_vars"]["env"], "allowList_eea_process":"eea_flag", "ink_laser_process":"laser"})

#setting databricks env to stg
if cumulus_vars["dbx_vars"]["env"] != "DEV": 
    cumulus_vars["dbx_vars"]["DATABRICKS_ENDPOINT"] = "https://dataos-cumulus-stg.cloud.databricks.com"
    cumulus_vars["dbx_vars"]["DATABRICKS_CREDENTIALS"] = "arn:aws:secretsmanager:us-west-2:828361281741:secret:itg/k8s/airflow/cumulus/databricks-e2"
    cumulus_vars["dbx_vars"]["service_account_name"] = "cumulus-airflow2-itg"
    cumulus_vars["dbx_vars"]["ecr_image"] = "cumulus-airflow-base:REL-0.0.48"

access_control = cumulus_vars["default_access_control"]
# Input Vars
out_schema = cumulus_vars["out_schema"]
image_tag = cumulus_vars["released_tag"]

# jupyter_image_tag = cumulus_vars['released_jupyter_tag']

args = {
    "owner": "cumulus",
    "start_date": airflow.utils.dates.days_ago(8),
    "depends_on_past": False,
}

dag = DAG(
    dag_id="cumulus-dbt-eea-flagging-laser",
    default_args=args,
    schedule_interval=cumulus_vars.get("schedule"),  # get returns None if key not found
    max_active_runs=1,
    catchup=False,  # trident handles catchup internally
    params={"email_to": cumulus_vars["email_to"]},
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
    access_control=access_control,
    tags=["eea_remediation"],
)

image = Variable.get("ecr_repository") + f"dbt-cumulus:{image_tag}"
# jupyter_image = Variable.get('ecr_repository') + f'dst_jupyter_notebooks:{jupyter_image_tag}'

extra_vars = f"out_schema: {out_schema}"

# sub_dag = SubDagOperator(
#     subdag=eea_factory.build_sub_dag(parent_dag=dag,
#                                          child_dag_name='eea',
#                                          image=image,
#                                         #  jupyter_image=jupyter_image,
#                                          cumulus_vars=cumulus_vars,
#                                          first_run=False,
#                                          run_compare=True,
#                                          extra_vars=extra_vars),
#     task_id='eea',
#     dag=dag
# )

eea_factory.populate_dag(
    dag=dag, image=image, cumulus_vars=cumulus_vars, extra_vars=extra_vars
)
