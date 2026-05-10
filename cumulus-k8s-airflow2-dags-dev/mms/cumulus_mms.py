import airflow
import logging
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from airflow.models import DAG
from airflow.models import Variable
import json
from libs.dataos import send_failure_email, send_success_email
from kubernetes.client import models as k8s

DAG_ID                  = 'cumulus-mms'

node_selector           = Variable.get('cumulus_node_selector', deserialize_json=True)
tolerations             = Variable.get("cumulus_tolerations", deserialize_json=True)
startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))
service_account_name    = "airflow2-cumulus-mms"


def get_k8s_pod(dag, name, namespace, image, command, env_vars):
    name = name.replace("_", "-")
    pod = DataOSPodOperator(  namespace=namespace,
                                image=image,
                                name=name,
                                cmds=["bash", "-cx"],
                                arguments=[command],
                                task_id=name,
                                get_logs=True,
                                node_selector=node_selector,
                                in_cluster=True,
                                is_delete_operator_pod=True,
                                dag=dag,
                                image_pull_policy='Always',
                                startup_timeout_seconds=startup_timeout_seconds,
                                tolerations=tolerations,
                                service_account_name=service_account_name,
                                env_vars=env_vars,
                                resources={
                                  "request_memory" : "500Mi", 
                                  "limit_memory" : "500Mi", 
                                },
                                executor_config={
                                  "pod_override": k8s.V1Pod(
                                                    spec=k8s.V1PodSpec(
                                                          containers=[k8s.V1Container(name="base")],
                                                          node_selector=node_selector, 
                                                          tolerations=[k8s.V1Toleration(**toleration) for toleration in tolerations]
                                                        )
                                                  )
                                },
                               )
    return pod


args = {
    'owner': 'cumulus',
    'depends_on_past': True,
    'on_failure_callback': send_failure_email,
    'on_success_callback': send_success_email,
    "params": {"email_to": ["ryan.schreiber@hp.com", "sujithkrishnan.pallath@hp.com"] },
    'start_date': airflow.utils.dates.days_ago(10),
}

variables  = Variable.get('cumulus_mms', deserialize_json=True)

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval='0 4 * * 0',
    access_control={
    'cumulus-operator': ['can_read','can_edit'],
    'cumulus-admin': ['can_read','can_edit'],
  }
)

namespace            = Variable.get('namespace')
image                = Variable.get('ecr_repository') + 'cumulus-mms:REL-2.1.3'
dynamic_tasks        = []

## Adding the volume/mount to get the config file, the config can be found in the
## gitops repo, in the path "flux/namespaces/infra/airflow-XXX/cost-monitoring-cm.yaml"
environment = Variable.get("environment").lower()


## creating task for reference data
command  = "mms gather ink"
dynamic_tasks.append(get_k8s_pod(dag, "gather-ink", namespace, image, command, variables))

command  = "mms gather laser"
dynamic_tasks.append(get_k8s_pod(dag, "gather-laser", namespace, image, command, variables))

command  = "mms load ink"
dynamic_tasks.append(get_k8s_pod(dag, "load-ink", namespace, image, command, variables))

command  = "mms load laser"
dynamic_tasks.append(get_k8s_pod(dag, "load-laser", namespace, image, command, variables))

dynamic_tasks[0] >> dynamic_tasks[2]
dynamic_tasks[1] >> dynamic_tasks[3]

### chaining tasks
# for i in range(len(dynamic_tasks)):
#   if i not in [0]: 
#     dynamic_tasks[i-1] >> dynamic_tasks[i]
