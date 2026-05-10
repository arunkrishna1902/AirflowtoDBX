import airflow
import logging
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from kubernetes.client import models as k8s
import airflow.configuration as config
from airflow.models import DAG
from airflow.models import Variable
import json
from libs.dataos import send_failure_email, send_success_email


class PeptoPodOperator(DataOSPodOperator):

    def pre_execute(self, context):
        """
        This gets called automatically pre execution and sets the xcom result from
        the previous task to an environment variable in the pod called 'XCOM'.
        """
        ti      = context['ti']
        
        # make the env vars easier to work with
        vars    = dict(zip(
            [item.name for item in self.env_vars], 
            [item.value for item in self.env_vars]
        ))
        task_id = vars.get("PREVIOUS_TASK_ID")
        key     = vars.get("PREVIOUS_TASK_KEY", "return_value") # airflow's default is return_value
        xcom    = str(ti.xcom_pull(
            key = key,
            task_ids = task_id,
        ))
        
        env_vars = [
            {
                "name" : "XCOM",
                "value" : xcom,
                "value_from" : None,
            },
        ]
        self.env_vars += env_vars
        return super().pre_execute(context)

    
DAG_ID                  = 'cumulus-xcom-test'

affinity                = Variable.get('default_affinity', deserialize_json=True)
tolerations             = Variable.get('default_tolerations', deserialize_json=True)
startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))
service_account_name    = "airflow2-cumulus-mms"

def get_k8s_pod(dag, name, namespace, image, command, env_vars, volumes=[], volume_mounts=[], do_xcom_push=False):
    name = name.replace("_", "-")
    pod = PeptoPodOperator(  namespace=namespace,
                                  image=image,
                                  name=name,
                                  cmds=["bash", "-cx"],
                                  arguments=[command],
                                  task_id=name,
                                  get_logs=True,
                                  affinity=affinity,
                                  in_cluster=True,
                                  is_delete_operator_pod=True,
                                  dag=dag,
                                  image_pull_policy='Always',
                                  startup_timeout_seconds=startup_timeout_seconds,
                                  tolerations=tolerations,
                                  service_account_name=service_account_name,
                                  env_vars=env_vars,
                                  do_xcom_push=do_xcom_push,
                                  volumes=volumes, 
                                  volume_mounts=volume_mounts,
                                  configmaps=["databricks-cumulus-job-ids"],
                               )
    return pod


args = {
    'owner': 'cumulus',
    'depends_on_past': True,
    'on_failure_callback': send_failure_email,
    "params": {"email_to": ["ryan.schreiber@hp.com"] },
    'start_date': airflow.utils.dates.days_ago(1),
}

variables  = Variable.get('cumulus_mms', deserialize_json=True)

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=None,
    access_control={
        'cumulus-operator': ['can_read','can_edit'],
        'cumulus-admin': ['can_read','can_edit'],
    }
)

namespace            = Variable.get('namespace')
image                = Variable.get('ecr_repository') + 'cumulus-mms:024a99ec3d83bbf61cc714fca2a11fa85f9a621f'
dynamic_tasks        = []

## Adding the volume/mount to get the config file, the config can be found in the
## gitops repo, in the path "flux/namespaces/infra/airflow-XXX/cost-monitoring-cm.yaml"
environment = Variable.get("environment").lower()


## creating task for reference data
# command  = "chmod +x ./test-xcom.sh && ./test-xcom.sh"
# dynamic_tasks.append(get_k8s_pod(dag, "write-xcom", namespace, image, command, variables, do_xcom_push=True))

# command  = "env"
# variables["PREVIOUS_TASK_ID"] = "write-xcom"
# dynamic_tasks.append(get_k8s_pod(dag, "pull-xcom", namespace, image, command, variables))

command  = "sleep 600"
dynamic_tasks.append(get_k8s_pod(dag, "write-xcom", namespace, image, command, variables))

## chaining tasks
for i in range(len(dynamic_tasks)):
  if i not in [0]: 
    dynamic_tasks[i-1] >> dynamic_tasks[i]
