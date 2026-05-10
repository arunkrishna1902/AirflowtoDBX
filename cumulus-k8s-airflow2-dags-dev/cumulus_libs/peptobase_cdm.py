import airflow
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import DAG, Variable
from airflow.utils.trigger_rule import TriggerRule
import airflow.hooks.S3_hook
from airflow.models.crypto import get_fernet
import airflow.settings
from airflow.models import DagModel
from libs.dataos import send_failure_email
from kubernetes.client import models as k8s

import logging
import requests, time, json, boto3, re, pendulum
from datetime import datetime, timedelta, date
from colorama import Fore, Back, Style 
from collections import defaultdict

from cumulus_libs.aws_support import get_spot_pricing, aws_secrets_manager_get_secret
from cumulus_libs.pepto_pod_operator import PeptoPodOperator

def check_step_in_source_steps(dag_id,process,**context):
    steps_json=json.loads(Variable.get("cumulus_cdm_steps"))
    step_list=steps_json[dag_id].split(",")
    #proceed with job id retreival or skip
    return  "-".join([dag_id,process,"run"]).replace("_", "-") if process in step_list else "-".join([dag_id,process,"skipped"]).replace("_", "-")

def switch_dag_pause_state(dag_id):
    """
    A way to programatically unpause a DAG.
    :param dag_id: string
    :return: dag.is_paused is switched
    """
    session = airflow.settings.Session()
    try:
        qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        d = qry.first()
        logging.info(str(dag_id)+" paused state is "+str(d.is_paused))
        d.is_paused = not d.is_paused
        session.commit()
        logging.info(str(dag_id)+" paused state is "+str(d.is_paused))
    except:
        session.rollback()
    finally:
        session.close()

def is_dag_paused(dag_id):
    is_paused=None
    session = airflow.settings.Session()
    try:
        qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
        d = qry.first()
        is_paused=d.is_paused
    except Exception as e:
        logging.warning(str(e))
    finally:
        session.close()
        return is_paused
    
def branch_to_trigger(dag_id_to_check, task_id_true, task_id_false):
    return task_id_true if is_dag_paused(dag_id_to_check) else task_id_false

def trigger_downstream_dag(dag, env, base_task, downstream_dag):
    ##IF DAG IS NOT PAUSED,Then TRIGGER##    
    branch_to_trigger_op = BranchPythonOperator(
      task_id="_".join([env.lower(), "dag_check_if_unpaused"]),
      dag=dag,
      python_callable=branch_to_trigger,
      op_kwargs={
        "dag_id_to_check": downstream_dag,
        "task_id_true": dag.dag_id+"_skip_trigger",
        "task_id_false": dag.dag_id+"_triggering_"+downstream_dag
      },
      trigger_rule='none_failed'
    )    

    base_task>>branch_to_trigger_op

    trigger_blksea_lf_op = TriggerDagRunOperator(
            task_id= dag.dag_id+"_triggering_"+downstream_dag,
            trigger_dag_id=downstream_dag,
            conf={"message": "Triggered by successful run of "+str(dag.dag_id)},
            #if output needed downstream use context["dag_run"].conf["key_name"]
            dag=dag
    )    

    skip_trigger_op = DummyOperator(task_id=dag.dag_id+"_skip_trigger", dag=dag)          

    branch_to_trigger_op>>trigger_blksea_lf_op    
    branch_to_trigger_op>>skip_trigger_op
    
class PeptoDAG:
    def __init__(self,source_id="",env="",process="",stack="",owner="",depends_on_past=False,catchup=False,max_active_runs=2, vars=dict(), access_control=["cumulus-operator", "cumulus-admin"],):
        self.SEP="##############################################################################\n"
        self.vars = vars

        self.__ENVIRONMENT = str(env)
        self.__SOURCE_ID=str(source_id)
        self.__PROCESS=str(process)
        self.__STACK=str(stack)
        
        #__VAR BASE will be the dag name as well as the base for retrieving variables
        if process!="":
            self.__VAR_BASE="_".join([self.__ENVIRONMENT.lower(),self.__PROCESS.lower(),self.__SOURCE_ID.lower()])
        
        else:
            self.__VAR_BASE="_".join([self.__ENVIRONMENT.lower(),self.__SOURCE_ID.lower()])

        self.dag_base_name=self.__VAR_BASE        
        self.__SCHEDULE_INTERVAL=self.get_dag_schedule_interval()
        self.__START_DATE=self.get_dag_start_date()
        self.__MAX_ACTIVE_RUNS=int(max_active_runs)
        self.__CATCHUP=bool(catchup)
        
        self.__dag_default_args={}
        self.__dag_default_args["on_failure_callback"] = send_failure_email
        self.__dag_default_args["owner"]="cumulus" if owner=="" else str(owner)
        self.__dag_default_args["depends_on_past"]=bool(depends_on_past)
        if "alert_email" in vars:
          self.__dag_default_args["params"] = {"email_to": vars.get("alert_email", "").split(",") }
        
        self.__ACCESS_CONTROL = { user: ['can_read','can_edit'] for user in access_control }
        
    def get_dag_schedule_interval(self):        
        try:            
            schedule=self.vars["schedule_interval"]
            if schedule=="None":
                schedule=None
            logging.info(schedule)
        except:
            logging.warning(self.__VAR_BASE+"[schedule_interval] was not found in variables. Schedule_interval will be 	0 11 * * *")    #####3 am PST
            schedule="0 11 * * *"
        return schedule

    def get_dag_start_date(self):
        try:            
            start_date_json=json.loads(Variable.get(f"cumulus-{self.__SOURCE_ID.lower()}"))
            start_date=start_date_json["start_date"]
            start_date=datetime.strptime(start_date,'%Y-%m-%d')  
        except:
            logging.warning(self.__VAR_BASE+"[start_date]"+" was not found in variables. Start Date will be set to 2020-02-01")
            start_date=datetime.strptime("2020-02-01",'%Y-%m-%d')#airflow.utils.dates.days_ago(1)
        return start_date

    def set_email_settings(self,email_list=[],email_on_failure=False):
        self.__dag_default_args["params"] = {"email_to": email_list }

    def set_retry_logic(self,retries=0,retry_delay=1):
        #retry delays in mins
        self.__dag_default_args["retries"]=int(retries)
        self.__dag_default_args["retry_delay"]=timedelta(minutes=int(retry_delay))

    def print_settings(self):        
        logging.info("Enviroment: "+self.__ENVIRONMENT)
        logging.info("SourceId: "+self.__SOURCE_ID)
        logging.info("Process: "+self.__PROCESS)
        logging.info("Schedule Interval:"+str(self.__SCHEDULE_INTERVAL))
        logging.info("Max Active Runs: "+str(self.__MAX_ACTIVE_RUNS))
        logging.info("Catchup: "+str(self.__CATCHUP))
        json_friendly_default_args = {**self.__dag_default_args}
        json_friendly_default_args.pop("on_failure_callback")
        logging.info("Dag Default Args: "+json.dumps(json_friendly_default_args,indent=4,sort_keys=True))

    def assemble_dag(self,scheduled_by_trigger=False):
        self.print_settings() 
        if scheduled_by_trigger:
            logging.info("Overriding schedule interval and use trigger from previous dag")       
        return DAG(dag_id =self.__VAR_BASE,\
            default_args=self.__dag_default_args,\
            schedule_interval= None if scheduled_by_trigger else self.__SCHEDULE_INTERVAL,\
            start_date=self.__START_DATE,\
            catchup=self.__CATCHUP,\
            max_active_runs=self.__MAX_ACTIVE_RUNS,
            tags=['cdm', 'west'],
            access_control=self.__ACCESS_CONTROL)


class PeptoDatabricksTaskList:
    def __init__(self, dag,env,source_id,process,previous_task_ids=None,pool=None, env_vars={}, team="cumulus"):

        # common vars
        self.namespace = Variable.get("namespace")
        self.node_selector = Variable.get(f'{team}_node_selector', deserialize_json=True)
        self.tolerations = Variable.get(f"{team}_tolerations", deserialize_json=True)
        self.pod_template = Variable.get('default_pod_template_x_account')
        self.startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout")) * 2
        # ^^ making the startup time double to possibly mitigate against ASG taking too long to spin up

        # k8s operator specific vars
        self.image = f"{Variable.get('ecr_repository')}{env_vars.get('ecr_image')}"
        self.service_account_name = env_vars.get('service_account_name')
        self.configmaps = env_vars.get("configmaps")
        self.command = env_vars.get("run_command")
        self.schedule = env_vars.get("schedule_interval")
        self.executor_config={
                "pod_override": k8s.V1Pod(
                                spec=k8s.V1PodSpec(
                                        containers=[k8s.V1Container(name="base")],
                                        node_selector=self.node_selector, 
                                        tolerations=[k8s.V1Toleration(**toleration) for toleration in self.tolerations]
                                    )
                                )
            }
        #update env_vars with previous task ids and pool
            # couldn't pass list as env_vars to pod operator
            # hack to join prev task ids into a string and unpack it later
        if previous_task_ids is not None:
            env_vars = {**env_vars, "previous_task_ids":'|'.join(previous_task_ids), "pool":pool}
        else:
            env_vars = {**env_vars, "previous_task_ids":previous_task_ids, "pool":pool}

    #####creating the final details for the Job#####
        self.ENV=env
        self.SOURCE_ID=source_id
        self.PROCESS=process
        self.JOB_NAME="cumulus-"+dag.dag_id+"-"+process
        dag_name= "cumulus_"+str(env)+"_"+str(source_id).replace("_", "-")
        self.DAG_NAME = dag_name.lower()
        self.POD_NAME = self.JOB_NAME
        self.ENV_VARS = env_vars
        self.POOL = pool

        logging.info("\n".join([self.JOB_NAME,self.ENV,self.SOURCE_ID]))

        self.ENV_VARS.update({
            "process":process,
            "source_id":source_id,
            "env":env,
            "job_name":"cumulus-"+str(self.SOURCE_ID).lower()+"-"+str(process)+"-"+str(env) #Let RCB know about the path reconstruction to match codeway job. How to add Laser for WPPGEN2 and GOTHAM? 
        })
                            
    #####Branch DAG Creation based on process and creating the task_id######
        self.branch_op = BranchPythonOperator(
            task_id="-".join([dag.dag_id, process, "branching"]).replace("_", "-"),
            dag=dag,
            provide_context=True,
            python_callable=check_step_in_source_steps,
            op_kwargs={"dag_id":dag.dag_id,"process":process},
            trigger_rule='none_failed',
            executor_config=self.executor_config
        )
        
    ############run_op for running the Databricks Job using the method - run_databricks_job##############
        ########Looping for the OCV process - it will help us to run just one job at a time!#########
        if pool:
            self.run_op=self.get_k8s_pod(
              dag=dag,
              name="-".join([dag.dag_id,process,"run"]),
              namespace=self.namespace,
              image=self.image,
              command=self.command,
              pool=self.POOL,
              executor_config=self.executor_config,
              env_vars=self.ENV_VARS
            )
        else:
            self.run_op=self.get_k8s_pod(
              dag=dag,
              name="-".join([dag.dag_id,process,"run"]),
              namespace=self.namespace,
              image=self.image,
              command=self.command,
              executor_config=self.executor_config,
              env_vars=self.ENV_VARS
            )

    ########SKIP operation using DummyOperator#########
        self.skip_op= DummyOperator(task_id="_".join([dag.dag_id,process,"skipped"]).replace("_", "-"), dag=dag)  

    #####Upstream Specification#####
        self.branch_op >> self.run_op
        self.branch_op >> self.skip_op

    ####### Getting the op details to fetch the previous_task_ids details to check the result and process next step########
    def get_ops(self):
        return {"branch_op":self.branch_op,"skipped_op":self.skip_op, "run_op":self.run_op}

    def get_k8s_pod(self, dag, name, namespace, image, command, env_vars, executor_config, pool='default_pool'):
        name = name.replace("_", "-")
        pod = PeptoPodOperator(namespace=namespace,
                                    image=image,
                                    name=name,
                                    cmds=["bash", "-cx"],
                                    arguments=[command],
                                    task_id=name,
                                    get_logs=True,
                                    node_selector=self.node_selector,
                                    pod_template_file=self.pod_template,
                                    in_cluster=True,
                                    is_delete_operator_pod=True,
                                    dag=dag,
                                    image_pull_policy='Always',
                                    startup_timeout_seconds=self.startup_timeout_seconds,
                                    tolerations=self.tolerations,
                                    env_vars=env_vars,
                                    service_account_name=self.service_account_name,
                                    configmaps=[self.configmaps],
                                    volumes=[],
                                    volume_mounts=[],
                                    do_xcom_push=True,
                                    pool=pool,
                                    resources={
                                      "request_memory" : "500Mi", 
                                      "limit_memory" : "500Mi", 
                                    },
                                    executor_config=executor_config
                                )
        return pod