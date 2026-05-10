import airflow
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from cumulus_libs.peptobase_cdm import PeptoDAG,PeptoDatabricksTaskList

source_id="EXTERNAL_TRIGGER"

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
    dag_factory=PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag=dag_factory.assemble_dag()

    def test_func_one():
        '''
        Dummy function
        '''
        msg = "Hello from test_func_one"
        print(msg)
        return msg
    python_task_one = PythonOperator(
        task_id="python_task_one",
        python_callable=test_func_one,
        dag = dag
    )

    def test_func_two():
        '''
        Dummy function
        '''
        msg = "Hello from test_func_two"
        print(msg)
        return msg
    python_task_two = PythonOperator(
        task_id="python_task_two",
        python_callable=test_func_two,
        dag = dag
    )

    python_task_one >> python_task_two
    globals()[dag.dag_id]=dag
