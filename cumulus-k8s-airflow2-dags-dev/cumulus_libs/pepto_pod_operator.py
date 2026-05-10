
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

class PeptoPodOperator(DataOSPodOperator):

    def pre_execute(self, context):
        """
        This gets called automatically pre execution and sets the xcom result from
        the previous task to an environment variable in the pod called 'XCOM'.
        """
        
        try:
            vars    = dict(zip(
                [item.name for item in self.env_vars], 
                [item.value for item in self.env_vars]
            ))

            ti      = context['ti']
            task_id = vars.get("previous_task_ids")
            key     = vars.get("previous_task_key", "return_value") # airflow's default is return_value
            xcom    = str(ti.xcom_pull(
                key = key,
                task_ids = task_id,
            ))

            self.env_vars += [
                {
                    "name" : "XCOM",
                    "value" : xcom,
                    "value_from" : None,
                },
            ]
        
        except Exception as e:
            logging.info(str(e))
            
        return super().pre_execute(context)
        
        
