import json
import logging
import airflow
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models import Variable, DAG
from libs.dataos import send_failure_email, send_success_email
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from datetime import datetime
from airflow import settings
from sqlalchemy.orm.session import Session
from kubernetes.client import models as k8s

namespace=Variable.get("namespace")

today_date = datetime.now().strftime('%Y-%m-%d')
sender = 'dps-big-data-test-team@hp8.us'
receivers = "cumulus_dev@groups.hp.com,dataos.cumulus.rcb@external.groups.hp.com"
currentDate = str(datetime.now().date())

if namespace == 'airflow2-dev':
    environment = "DEV"
    bucket = "hp-bigdata-databricks-metrics-testresults"
elif namespace == 'airflow2-itg':
    environment = "ITG"
    bucket="hp-bigdata-databricks-itg-metrics-testresults"
elif namespace == 'airflow2-prod':
    environment = "PROD"
    bucket="hp-bigdata-databricks-prod-metrics-testresults"

subject = "Metrics Validator Results " + environment + " " + currentDate

metricsValueCheck = '''{}'''
message = '''[""]'''
allSourceTestAnswerList = '''[]'''
oneSourceTestResult = '''[]'''

optional_Default_keys = '''{
    'job' : ['queue_name','output_size_bytes'],
    'landscape' : 'number_of_spot_instances'
}'''

metrics_Value_Default = '''{'step_index': 0, 'bad_count': 0}'''
operations_default_dict_step = '''{"step_index": 1,  # (0,1,...)  // first step is 0; put the index here instead of as a record "key" to make it easier for ELK to ingest
    "step_name":  "String",
    "notebook_to_run": "String",  # path to the notebook that is run for this
    "start_ts": "String", #(YYYY-MM-DD hh:mm:ss in UTC)>
    "start_epoch": 1.0,
    "elapsed_sec": 1.0,
    "status": "String" # ( PASSED | FAILED) # this information could be obtained from BAT
    }'''
operations_default_dict='''{
    "source_type" : "String",
    "app":"String", # (ENV-SOURCEID-PROCESSNAME),
    "job": {
      "job_id":"String", # ID of job e.g.job-221707  extracted from Spark environment variable: spark.databricks.clusterUsageTags.clusterName
      "run_id": "String", # spark run ID e.g. run-1   extracted from Spark environment variable: spark.databricks.clusterUsageTags.clusterName
      "queue_name": "String", # needed for pipelines that can process different types of queues
      "source_id": "String", # for example, JAM MHIT, JAM LEDM
      "process_date":"String", #(YYYY-MM-DD) The date we ran
      "date_to_process":"String", #(YYYY-MM-DD|YYYY-MM-DD--YYYY-MM-DD) receive date
      "start_epoch": 1.0, # ELK friendly start time -- requested by CSO
      "end_epoch": 1.0, #ELK friendly end time
      "start_ts": "String", #(YYYY-MM-DD hh:mm:ss UTC)>, // when the job started
      "end_ts": "String", #(YYYY-MM-DD hh:mm:ss UTC)>,
      "elapsed_sec": 1.0, #sure, this can be computed, but it's convenience to have called out here
      "input_path": "String", # path in s3 to the input data, if any
      "output_path": "String",
      "input_size_bytes": 1, # might not be needed if we're not using this for Zabbix
      "output_size_bytes": 1,
      "input_count": 1, # number of records ingested by this queue/pipeline (first stage); every pipeline processes data -- we need this count for velocity/threshold monitoring
      "good_count": 1, # number of "good" records, if applicable
      "bad_count": 1, # number of "bad" records,
      "total_count": 1, #Total number of records,
      "good_percentage" : 1.0,
      "overall_status": "String", #enum: (PASSED | FAILED)>, // overall status of the workflow (from BAT)
      "error_message": "String" # brief message indicating why the job failed; "NA" if there is no error; user can find the failed job in Databricks using the info in this file
    },
    "landscape": {
       "environment_name": "String", #enum: (DEV| DAILYCI | ITG | PROD |PRODSMOKEINITG)>,
       "number_of_instances": 1,  # total number of instances used
       "number_of_spot_instances": 1, #<optional>,  // number of those instances that were SPOT -- if we can't get it, forget it. ;-)
       "instance_type": "String", # r4.2xl
       "databricks_runtime_version": "String",  # 2.4.0, use spark.version
       "python_version": "String",  # 2.7.12 (default, Nov 12 2018, 14:36:49), use sys.version (import sys)
       "package_version": "String" # from package manifest
    },
    "steps": [ {"step_index": 1,  # (0,1,...)  // first step is 0; put the index here instead of as a record "key" to make it easier for ELK to ingest
    "step_name":  "String",
    "notebook_to_run": "String",  # path to the notebook that is run for this
    "start_ts": "String", #(YYYY-MM-DD hh:mm:ss in UTC)>
    "start_epoch": 1.0,
    "elapsed_sec": 1.0,
    "status": "String" # ( PASSED | FAILED) # this information could be obtained from BAT
                }
    ]
  }'''

args = {
    'owner': 'airflow pepto',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
    'depends_on_past': False
}

dag = DAG(
        dag_id='metrics_validator',
        default_args=args,
        schedule_interval='00 17 * * *',
        params={"email_to": "krishna.vuyyuru@hp.com"},
        on_failure_callback=send_failure_email,
        on_success_callback=None,
        access_control={
            'cumulus-operator': ['can_read', 'can_edit'],
            'cumulus-admin': ['can_read', 'can_edit']
        }
)

session = settings.Session()

vars = Variable.get("cumulus-common-variables", deserialize_json=True)
env_vars = vars
env_vars.update({'sender': sender,
                'receivers': receivers,
                'currentDate': currentDate,
                'subject': subject,
                'environment': environment,
                'bucket': bucket,
                'airflow_namespace': namespace,
                'optional_Default_keys': optional_Default_keys,
                'metrics_Value_Default': metrics_Value_Default,
                'operations_default_dict_step': operations_default_dict_step,
                'operations_default_dict': operations_default_dict,
                'metricsValueCheck': metricsValueCheck,
                'message': message,
                'allSourceTestAnswerList': allSourceTestAnswerList,
                'oneSourceTestResult': oneSourceTestResult
                })

service_account_name = env_vars.get('service_account_name')
namespace = Variable.get("namespace")
node_selector = Variable.get('cumulus_node_selector', deserialize_json=True)
tolerations = Variable.get("cumulus_tolerations", deserialize_json=True)
startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))
pod_template = Variable.get('default_pod_template_x_account')
image = f"{Variable.get('ecr_repository')}cumulus-airflow-base:REL-0.0.43"
data = "cumulus-dags"
d = json.loads(Variable.get(data))


executor_config={
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(name="base")],
            node_selector=node_selector,
            tolerations=[k8s.V1Toleration(**toleration) for toleration in tolerations]
        )
    )
}

class MetricsValidatorPodOperator(DataOSPodOperator):
  def get_task_status(self,dag_id,task_id,execution_date):
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == dag_id,
            TI.execution_date == execution_date,
            TI.task_id == task_id
            ).all()
        logging.info ("Task Instance: " + str(ti))
        if ti:
            task_state = ti[0].state
        else:
            task_state = "NA"
        return task_state

  def pre_execute(self, context):
    
    self.metrics_list = []
    for dag_id in d:
        dag_runs = DagRun.find(dag_id=dag_id)
        if len(dag_runs) > 0:
            last_run = dag_runs[-1]
            dag_state = last_run.state
            execution_date = last_run.execution_date
            start_date = last_run.start_date.strftime("%Y-%m-%d")
            logging.info("Dag id: " + dag_id)
            logging.info ("Dag State is: " + dag_state)
            logging.info ("Dag start Date is: " + start_date)
            logging.info ("Dag execution Date is: "+ str(execution_date))
            if start_date == today_date:
                if dag_state == "success" or "failed":
                    for i in range(len(d[dag_id]['task'])):
                        task_state = self.get_task_status(dag_id, d[dag_id]['task'][i], execution_date)
                        logging.info ("Task state for " + d[dag_id]['task'][i] + " is " + str(task_state))
                        if task_state == "success":
                            self.metrics_list.append(d[dag_id]['metrics'][i])
                        elif task_state in ["upstream_failed" "failed"]:
                            logging.info ("{} workflow failed".format(d[dag_id]['task'][i]))
                        else:
                            logging.info ('task Status: '+ str(task_state))
                else:
                    logging.info ("dag_state is not valid")
            else:
                logging.info ("Dag didn't ran today")
        metrics_list = json.dumps(self.metrics_list).replace('"','').replace(' ','')
        self.env_vars += [
                        {
                            "name": "sourceMap",
                            "value": metrics_list,
                            "value_from": None,
                        }
                        ]
    return super().pre_execute(context)


def get_k8s_pod(dag, name, namespace, image, command, env_vars, do_xcom_push):
    name = name.replace("_", "-")
    pod = MetricsValidatorPodOperator(namespace=namespace,
                        image=image,
                        name=name,
                        cmds=["python", "-c"],
                        arguments=[command],
                        task_id=name,
                        get_logs=True,
                        node_selector=node_selector,
                        pod_template_file=pod_template,
                        in_cluster=True,
                        is_delete_operator_pod=True,
                        dag=dag,
                        image_pull_policy='Always',
                        startup_timeout_seconds=startup_timeout_seconds,
                        tolerations=tolerations,
                        executor_config=executor_config,
                        service_account_name=service_account_name,
                        env_vars=env_vars,
                        do_xcom_push=do_xcom_push,
                        )
    return pod


allSourceTestResults = get_k8s_pod(dag=dag,
                        name='get_all_source_list',
                        namespace=namespace,
                        image=image,
                        command="from metrics_validator import *; finalReport(allSourceTestResults())",
                        env_vars=env_vars,
                        do_xcom_push=False,
                        )

allSourceTestResults
