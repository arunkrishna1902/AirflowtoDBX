import airflow
import logging
import time
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from airflow.operators.python_operator import PythonOperator
from kubernetes.client import models as k8s
from airflow.models import DAG
from airflow.models import Variable
from libs.dataos import send_failure_email

import json
import boto3

# This is the name of your dag
DAG_ID = 'isaac-test'

# Each team must use the correct node_selector and tolerations that have been setup for them.
node_selector = Variable.get('cumulus_node_selector', deserialize_json=True)
tolerations   = Variable.get('cumulus_tolerations', deserialize_json=True)

# Define the image that you want to be using. This will be the name of your
# ECR repository with the specific tag that you want to use added to the end.
image = '740156627385.dkr.ecr.us-west-2.amazonaws.com/cumulus-airflow-base:4c00875bfd5a90650455bdde056152363ce2eace'

# This is where the DAG level permissions are defined as a dict() and restricted
# based on the name of roles. Using DST team as example since they are first with
# defined roles. See line 78 for the second half of this process.
access_control = {
  'cumulus-operator': ['can_read','can_edit'],
  'cumulus-admin': ['can_read','can_edit']
}

pod_template = Variable.get('default_pod_template_x_account')

# This is the name of the k8s service account that you want to use. These service accounts
# are what makes the pod operator so critical in that we can grant fine grained permissions
# to each pod. These irsa service accounts are managed by the platform team, so to request
# a role be created or updated, reach out to the platform ops team.
service_account_name    = 'cumulus-airflow2'

# The namespace should ALWAYS be taken from the airflow variables because they are specific
# to each airflow instance.
namespace               = Variable.get('namespace')
startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))

# This is the dag definition with all of the configuration
dag = DAG(
    dag_id=DAG_ID,
    default_args={
      'owner': 'cumulus',
      'depends_on_past': False, 

    #   Use the following params to trigger email on task failure. A distribution list should be used 
    #   and it is required to have the email address white listed with the platform-ops team. 
    #   'on_failure_callback': send_failure_email,
    #   "params": {"email_to": ["isaac.chan1@hp.com"] },

      'start_date': airflow.utils.dates.days_ago(1),
    },

    schedule_interval=None,
    access_control=access_control,
)

# The Kubernetes Executor configuration that will be used to run each of the airflow tasks. 
# Make sure to the the node_selector and tolerations that have been setup for your team.
executor_config={
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="base")],
            node_selector=node_selector,
            tolerations=[k8s.V1Toleration(**toleration) for toleration in tolerations]
        )
    )
}

def aws_secrets_manager_get_secret(endpoint_url, region_name, secret_name):
    client_aws_secret_mgr= boto3.client(
        service_name='secretsmanager',
        region_name=region_name,
        endpoint_url=endpoint_url)
    try:
        print(client_aws_secret_mgr.get_secret_value( SecretId=secret_name)['SecretString'])
    except Exception as e:
        print(e)
    time.sleep(10)
    return True

python_operator_secret = PythonOperator(
        task_id='python_operator_secret',
        python_callable=aws_secrets_manager_get_secret,
        op_kwargs={'endpoint_url': 'https://secretsmanager.us-west-2.amazonaws.com', 'region_name': 'us-west-2', 'secret_name':'arn:aws:secretsmanager:us-west-2:740156627385:secret:codeway/databricks/team-cumulus-run-user-LnYmbo'},
        dag=dag,
        executor_config=executor_config
    )

def hello_world(message):
    logging.info(message)
    time.sleep(10)

python_operator_hello = PythonOperator(
        task_id='python_operator_hello',
        python_callable=hello_world,
        op_kwargs={'message': 'Hello from PythonOperator'},
        dag=dag,
        executor_config=executor_config
    )

pod_operator_hello = DataOSPodOperator(  
        namespace=namespace,
        image=image,
        name="pod_operator_hello",
        cmds=["bash", "-cx"],
        arguments=["python run_databricks_job.py"],
        task_id="pod_operator_hello",
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        pod_template_file=pod_template,
        dag=dag,
        image_pull_policy='Always',
        startup_timeout_seconds=startup_timeout_seconds,
        service_account_name=service_account_name,
        env_vars={},
        tolerations=tolerations,
        node_selector=node_selector,
        executor_config=executor_config                                
    )


python_operator_secret >> python_operator_hello >> pod_operator_hello
