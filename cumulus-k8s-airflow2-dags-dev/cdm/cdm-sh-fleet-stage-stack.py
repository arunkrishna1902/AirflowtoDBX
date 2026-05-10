import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from libs.dataos import send_failure_email, send_success_email

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList

source_id="SH_FLEET_STAGE_STACK"

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
    #def __init__(self,source_id="",env="",process="",depends_on_past=False,catchup=False,max_active_runs=2) 
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag=dag_factory.assemble_dag()
         
    #PeptoDatabricksTaskList: __init__(self, dag,env,source_id,process, previous_task_id="")
    # raw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="dps_raw",super_package="hppk_pepto",package="dps_raw_hppk_pepto")
    # raw_task_dict=raw_task_list.get_ops()

    # env_vars_stdraw = {**env_vars, "job_id":vars["job_id"]["prod_stack"]}
    env_vars_stdraw = {**env_vars, "DATABRICKS_DEPLOYMENT_NAME" : f"CUMULUS_SH_FLEET_STAGE_STACK_{env_for_deployment.upper()}"}
    stdraw_task_list=PeptoDatabricksTaskList(dag=dag, env=env,source_id=source_id,process="stage_stack",env_vars=env_vars_stdraw)
    stdraw_task_dict=stdraw_task_list.get_ops()
    # raw_task_dict["run_op"]>>stdraw_task_dict["branch_op"]
    # raw_task_dict["skipped_op"]>>stdraw_task_dict["branch_op"]

    globals()[dag.dag_id]=dag





   





















# import os
# import logging
# from airflow.models import DAG, Variable
# from airflow.configuration import conf
# from airflow.operators.bash_operator import BashOperator
# from airflow.utils import dates
# from airflow.operators.subdag_operator import SubDagOperator
# from datetime import timedelta
# from libs.peptobase import PeptoDAG, PeptoDatabricksTaskList

# import airflow
# from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.models import DAG, Variable
# from airflow.utils.trigger_rule import TriggerRule
# import airflow.hooks.S3_hook
# from airflow.models.crypto import get_fernet
# import airflow.settings
# from airflow.models import DagModel

# import logging
# import requests,time,json,boto3, re, pendulum
# from datetime import datetime, timedelta
# from colorama import Fore, Back, Style 
# from collections import defaultdict

# from libs.aws_support import get_spot_pricing, aws_secrets_manager_get_secret

# airflow_namespace=Variable.get("namespace")
# if airflow_namespace=="airflow-dev":
#     env=["dailyCI"] #,"DEV"
# if airflow_namespace=="airflow-itg":
#     env=["ITG"]
# if airflow_namespace=="airflow_prod":
#     env=["PROD"]

# ENVIRONMENT = "dailyCI"
# SOURCE_ID="nextgen"
# PROCESS="dps_stdraw"
# schedule="0 11 * * *"
# super_package="dailyCI"
# package="dailyCI"
# start_date=datetime.strptime("2020-02-01",'%Y-%m-%d')#airflow.utils.dates.days_ago(1)





# def check_step_in_source_steps(dag_id,process):
#     # steps_json=json.loads(Variable.get("pepto_steps"))
#     step_list="dps_stdraw"
#     #proceed with job id retreival or skip
#     return  "_".join([dag_id,process,"run"]) if process in step_list else "_".join([dag_id,process,"skipped"])

# def switch_dag_pause_state(dag_id):
#     """
#     A way to programatically unpause a DAG.
#     :param dag_id: string
#     :return: dag.is_paused is switched
#     """
#     session = airflow.settings.Session()
#     try:
#         qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
#         d = qry.first()
#         logging.info(str(dag_id)+" paused state is "+str(d.is_paused))
#         d.is_paused = not d.is_paused
#         session.commit()
#         logging.info(str(dag_id)+" paused state is "+str(d.is_paused))
#     except:
#         session.rollback()
#     finally:
#         session.close()

# def is_dag_paused(dag_id):
#     is_paused=None
#     session = airflow.settings.Session()
#     try:
#         qry = session.query(DagModel).filter(DagModel.dag_id == dag_id)
#         d = qry.first()
#         is_paused=d.is_paused
#     except Exception as e:
#         logging.warning(str(e))
#     finally:
#         session.close()
#         return is_paused

# def get_json(bucket, key):
#     logging.info("Getting json using s3 client get_object for Bucket="+bucket+" and Key="+key) 
#     s3 = boto3.client('s3')            
#     content_object = s3.get_object(Bucket=bucket,Key= key)  
#     file_content = content_object['Body'].read().decode('utf-8')
#     json_content = json.loads(file_content)
#     return json_content

# def get_s3_contents(bucket, key):
#     s3 = boto3.client('s3')
#     print(bucket)
#     print(key)
#     content_object = s3.get_object(Bucket=bucket, Key=key)  
#     file_content = content_object['Body'].read().decode('utf-8')
#     return file_content

        
# def run_databricks_job(DATABRICKS_ENDPOINT,ENV, JOB_NAME, DATABRICKS_USER, DATABRICKS_PASSWORD):
#     previous_task_ids=None 
#     timeout=60*60*23
#     poll_interval=10
#     logging.info("##########################run_databricks_job###################################")
#     logging.info("\n".join(["run_databricks args:",str(previous_task_ids),str(timeout),str(poll_interval)]))                
#     start_time = datetime.now()

#     #retrieve results of latest job info
#     # latest_job_info_result = context['task_instance'].xcom_pull(task_ids=self.get_latest_job_id_op.task_id)
#     job_id="381726"  # FEtch jobid from variables later
#     logging.info("Job Id Found: "+str(job_id))
#     logging.info("Job URL - "+str(DATABRICKS_ENDPOINT)+"/#job/"+str(job_id))
    
#     #NOTE send arguments even if createjob defaults them
#     templates_dict= {
#                     "CurrentLandscapeLocation":ENV, #todo consolidate
#                     "environment":ENV,
#                     "jobName":JOB_NAME }

#     #submit run now to databricks api
#     run_job_json = {'job_id': job_id, 'notebook_params': templates_dict}
#     logging.info("Json to send to Databricks API"+json.dumps(run_job_json,indent=4))         
#     res = requests.post(DATABRICKS_ENDPOINT + "/api/2.0/jobs/run-now", json=run_job_json, auth=(DATABRICKS_USER, DATABRICKS_PASSWORD))
    
#     if res.status_code != requests.codes['ok']:
#         logging.error(res.text)
#         res.raise_for_status()

#     run_id = res.json()['run_id']
#     logging.info("Run Id: "+str(run_id))
#     # see documentation on runs-get-output and life_cycle_state here https://docs.databricks.com/api/latest/jobs.html#runs-get-output
    
#     httpstatuschecklist = ['429', '500', '502', '503']
#     running = True
#     retry_check = 0 
    
#     while running:
#         #check timeout
#         elapsed_time = (datetime.now() - start_time).total_seconds()
#         if elapsed_time > timeout:
#             logging.error("Submiting Cancel to databricks: elapsed_time " + str(elapsed_time) + " greater than: " + str(timeout))
#             res = requests.get(DATABRICKS_ENDPOINT + "/api/2.0/jobs/runs/cancel",  json={'run_id': run_id}, auth=(DATABRICKS_USER, DATABRICKS_PASSWORD))
#             running = False
#             raise ValueError("elapsed_time " + str(elapsed_time) + " greater than: " + str(timeout))
#         #get heartbeat
#         time.sleep(poll_interval)
#         res = requests.get(DATABRICKS_ENDPOINT + "/api/2.0/jobs/runs/get-output?run_id=" + str(run_id), auth=(DATABRICKS_USER, DATABRICKS_PASSWORD))
#         if res.status_code != requests.codes['ok']:
#             if str(res.status_code) in httpstatuschecklist:
#                 logging.warning("*********Response Status found in the checklist! Error Response Code - " + str(res.status_code) + " ***************")
#                 logging.info("Retrying to connect.........")
#             ######## 5 Minute Retry for every 10 seconds #######
#                 retry_check += 1   
#                 if retry_check > 30:
#                     print("######### Failed to Connect. Retried 5 Minutes to fetch response from Databricks endpoint############")
#                     running = False
#                     res.raise_for_status()
#             else:
#                 logging.warning("##### Bad Response ##### Exiting with the Response Status Code - " + str(res.status_code))
#                 logging.error(res.text)
#                 running = False
#                 res.raise_for_status()
    
#         else: retry_check = 0
                
#         #check lifecycle of heartbeat
#         life_cycle_state = res.json()['metadata']['state']['life_cycle_state']            
#         if life_cycle_state.lower() in ['terminated', 'skipped', 'internal_error']:
#             logging.error("life_cycle_state: " + life_cycle_state)
#             running = False
#         else:
#             logging.info("life_cycle_state: " + life_cycle_state)

#     response_json=res.json()
#     #get final result state
#     result_state = response_json['metadata']['state']['result_state']
#     if result_state.lower() not in ['success']:
#         metadata_state=response_json['metadata']['state']
#         logging.error("Metadata State: "+json.dumps(metadata_state,indent=4))
#         raise ValueError(json.dumps(metadata_state))
#     #check notebook result        
#     notebook_output_result=json.loads(response_json['notebook_output']['result'])
#     if str(notebook_output_result['result']).upper() not in ["SUCCESS","PROCESSING_SUCCEEDED","PASS","PASSED","TRUE"]:
#         logging.error("Notebook Failure\n"+json.dumps(notebook_output_result,indent=4))
#         raise ValueError("Job exited with result status: " + str(notebook_output_result))
#     #return result of workflow as string
#     return json.dumps(notebook_output_result)



# owner=""
# depends_on_past=False
# dag_default_args={} 
# dag_default_args["owner"]="airflow pepto" if owner=="" else str(owner)
# dag_default_args["depends_on_past"]=bool(depends_on_past)        

# VAR_BASE="_".join([ENVIRONMENT.lower(),SOURCE_ID.lower()])
# dag_base_name=VAR_BASE        
# SCHEDULE_INTERVAL=schedule
# START_DATE=start_date
# MAX_ACTIVE_RUNS=int(2)
# CATCHUP=False
# scheduled_by_trigger = False

# dag = DAG(dag_id =VAR_BASE,\
#         default_args=dag_default_args,\
#         schedule_interval= None if scheduled_by_trigger else SCHEDULE_INTERVAL,\
#         start_date=START_DATE,\
#         catchup=CATCHUP,\
#         max_active_runs=MAX_ACTIVE_RUNS)
# # def set_email_settings(self,email_list="",email_on_failure=False):
# #     dag_default_args["email_list"]=email_list
# #     dag_default_args["email_on_failure"]=bool(email_on_failure)

# # def set_retry_logic(self,retries=0,retry_delay=1):
# #     #retry delays in mins
# #     dag_default_args["retries"]=int(retries)
# #     dag_default_args["retry_delay"]=timedelta(minutes=int(retry_delay))

# # def print_settings(self):        
# logging.info("Enviroment: "+ENVIRONMENT)
# logging.info("SourceId: "+SOURCE_ID)
# logging.info("Process: "+PROCESS)
# logging.info("Schedule Interval:"+str(SCHEDULE_INTERVAL))
# logging.info("Max Active Runs: "+str(MAX_ACTIVE_RUNS))
# logging.info("Catchup: "+str(CATCHUP))
# logging.info("Dag Default Args: "+json.dumps(dag_default_args,indent=4,sort_keys=True))



# #todo finalize package and superpackages  
# # class PeptoDatabricksTaskList:
#     # def init(self, dag,env,source_id,process,super_package="dailyCI",package="dailyCI", previous_task_ids=None,pool=None):

# previous_task_ids=None
# pool=None
# process = PROCESS 
#     #####Fetching Databricks Keys from AWS Secrets using the variables/common_variables#####
# databricks_access_key=Variable.get("databricks_access_key")
# logging.info("Databricks Access KEY: "+databricks_access_key )
# DATABRICKS_USER = aws_secrets_manager_get_secret(secret_name=databricks_access_key, secrets_name_key="username")
# DATABRICKS_PASSWORD=aws_secrets_manager_get_secret(secret_name=databricks_access_key, secrets_name_key="password")               
# DATABRICKS_ENDPOINT = Variable.get("databricks_endpoint")
# logging.info("Databricks User: "+ DATABRICKS_USER )
# logging.info("Databricks Endpoint:"+ DATABRICKS_ENDPOINT)

# #####creating the final details for the Job#####
# SOURCE_ID = SOURCE_ID.lower()
# SUPER_PACKAGE=super_package
# PACKAGE=package
# ENV=ENVIRONMENT
# JOB_NAME="k8s-"+str(ENV.lower())+"codeway-CDM"
# dag_name= str(env)+"_"+str(SOURCE_ID)
# DAG_NAME = dag_name.lower()
# dag_id = dag_name

# ###########################END OF FIL##################################

# logging.info("\n".join([JOB_NAME,ENV,SOURCE_ID,SUPER_PACKAGE,PACKAGE]))

# #####Branch DAG Creation based on process and creating the task_id######
# branch_op = BranchPythonOperator(
#     task_id="_".join([dag.dag_id, process, "branching"]),
#     dag=dag,
#     provide_context=True,
#     python_callable=check_step_in_source_steps(dag_id,process),
#     op_kwargs={"dag_id":dag.dag_id,"process":process},
#     trigger_rule='none_failed'
# )
# ############run_op for running the Databricks Job using the method - run_databricks_job##############
# ########Looping for the OCV process - it will help us to run just one job at a time!#########
# if pool:
#     run_op=PythonOperator(                                                              
#     task_id="_".join([dag.dag_id,process,"run"]),
#     provide_context=True,
#     python_callable=run_databricks_job(DATABRICKS_ENDPOINT,ENV, JOB_NAME, DATABRICKS_USER, DATABRICKS_PASSWORD),
#     pool=pool,
#     trigger_rule="none_failed",
#     op_kwargs={"previous_task_ids":previous_task_ids},
#     dag=dag)
# else:
#     run_op=PythonOperator(                                                              
#         task_id="_".join([dag.dag_id,process,"run"]),
#         provide_context=True,
#         python_callable=run_databricks_job(DATABRICKS_ENDPOINT,ENV, JOB_NAME, DATABRICKS_USER, DATABRICKS_PASSWORD),
#         trigger_rule="none_failed",
#         op_kwargs={"previous_task_ids":previous_task_ids},
#         dag=dag)

# ########SKIP operation using DummyOperator#########
# #todo may change to retrieve a file from s3?
# skip_op= DummyOperator(task_id="_".join([dag.dag_id,process,"skipped"]), dag=dag)  

# #####Upstream Specification#####
# branch_op>> run_op
# branch_op>>skip_op



# globals()[dag.dag_id]=dag
