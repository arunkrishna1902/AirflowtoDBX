
import json
import boto3
import requests
import logging
import airflow
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable, DAG
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from libs.dataos import send_failure_email, send_success_email
from cumulus_libs.aws_support import aws_secrets_manager_get_secret
from datetime import datetime
from airflow import settings
from sqlalchemy.orm.session import Session

session = settings.Session()

node_selector = Variable.get('cumulus_node_selector', deserialize_json=True)
tolerations = Variable.get("cumulus_tolerations", deserialize_json=True)
startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))

def get_parquet_compare_pod(dag, name, namespace, image, command, env_vars, service_account_name, volumes=[], volume_mounts=[], do_xcom_push=False):
    name = name.replace("_", "-")
    pod = ParquetComparePodOperator(  namespace=namespace,
                                  image=image,
                                  name=name,
                                  cmds=["bash", "-cx"],
                                  arguments=[command],
                                  task_id=name,
                                  get_logs=True,
                                  in_cluster=True,
                                  is_delete_operator_pod=True,
                                  dag=dag,
                                  image_pull_policy='Always',
                                  startup_timeout_seconds=startup_timeout_seconds,
                                  tolerations=tolerations,
                                  node_selector=node_selector,
                                  service_account_name=service_account_name,
                                  env_vars=env_vars,
                                  do_xcom_push=do_xcom_push,
                                  volumes=volumes, 
                                  volume_mounts=volume_mounts,
                                  configmaps=["databricks-cumulus-job-ids"],
                               )
    return pod


class ParquetComparePodOperator(DataOSPodOperator):

    # Expects following env_vars passed in:
    # - DAG_ID
    # - TASK_ID
    # - DATABRICKS_ENDPOINT
    # - DATABRICKS_CREDENTIALS
    # - NOTEBOOK_PARAMS
    # - JOB_ID

    def pre_execute(self, context):
        """
        This gets called automatically pre execution and sets the xcom result from
        the previous task to an environment variable in the pod called 'XCOM'.
        """
        
        vars = dict(zip(
            [item.name for item in self.env_vars], 
            [item.value for item in self.env_vars]
        ))
        
        
        dag_runs = DagRun.find(dag_id=vars.get("DAG_ID"))
        if len(dag_runs) > 0:
          last_run = dag_runs[-1]
          dag_state = last_run.state
          execution_date = last_run.execution_date
          start_date = last_run.start_date.strftime("%Y-%m-%d")
          TI = TaskInstance
          ti = session.query(TI).filter(
              TI.dag_id == vars.get("DAG_ID"),
              TI.execution_date == execution_date,
              TI.task_id == vars.get("TASK_ID")
          ).all()
          if ti:
            task_state = ti[0].state
          else:
            task_state = "NA"
        else:
          dag_state = "dag doesn't exist"
          task_state = "dag doesn't exist"
          start_date = "XXXX-XX-XX"
        
        self.env_vars += [
            {
                "name" : "TASK_STATE",
                "value" : task_state,
                "value_from" : None,
            },
            {
                "name" : "DAG_STATE",
                "value" : dag_state,
                "value_from" : None,
            },
            {
                "name" : "START_DATE",
                "value" : start_date,
                "value_from" : None,
            },
        ]
        
        return super().pre_execute(context)
