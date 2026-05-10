import airflow
from airflow import DAG, settings
from airflow.decorators import dag, task
from airflow.models import Variable, Connection
from airflow.providers.http.operators.http import SimpleHttpOperator

from cumulus_libs.peptobase_cdm import PeptoDAG
from cumulus_libs.aws_support import aws_secrets_manager_get_secret

import logging
import json

# Name DAG
source_id="etl_dag"

# namespace config
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

# dag specific vars
vars = Variable.get("cumulus-cdm-common-variables", deserialize_json=True)
vars.update(Variable.get("cumulus-cdm-individual-dag-variables", deserialize_json=True).get(f"cumulus-{source_id.lower()}",{}))
env_vars = vars

@task # Sets a new connection ID in Airflow dev
def set_conn_id(conn_id, conn_type, host, login, password):
    conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password
    ) # create a connection object
    session = settings.Session() # get the session

    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    ### query session w/ conn_id == conn.conn_id

    if str(conn_name) == str(conn_id):
        return logging.info(f"Connection {conn_id} already exists")

    session.add(conn) # it will insert the connection object
    session.commit()
    logging.info(Connection.log_info(conn))
    logging.info(f'Connection {conn_id} is created')

@task # delete test connections
def del_conn_id(conn_ids: list):
    session = settings.Session()
    for id in conn_ids:
        query_id =  session.query(Connection).filter(Connection.conn_id == id).first()
        if str(query_id) == str(id):
            session.delete(query_id)
            session.commit()
            logging.info(f'Connection {str(id)} was deleted')
    session.close()

@task
def extract():
    '''
    ### Extract task
    '''
    data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
    order_data_dict = json.loads(data_string)

    print(f'returned dictionary --> {order_data_dict}')
    return order_data_dict

@task(multiple_outputs=True)
def transform(order_data_dict: dict):
    """
    ### Transform task
    """
    total_order_value = 0

    for value in order_data_dict.values():
        total_order_value += value

    return {"total_order_value": total_order_value}

@task
def load(total_order_value: float):
    """
    #### Load task
    """
    logging.info("load task accessed.")
    print(f"Total order value is: {total_order_value:.2f}")

@task
def check_creds(secret_name, username, password):
    """
    check credentials
    """
    logging.info(f"secretname is: {secret_name}, username is: {username}, password is {password}")

### Testing
secret_name = 'dev/k8s/airflow/rest-users/cumulus'
login = aws_secrets_manager_get_secret(secret_name,'username', region_name="us-west-2") # returning None
password = aws_secrets_manager_get_secret(secret_name,'password', region_name="us-west-2") # returning None
connection_id = 'testing1-dev-int'

# body of POST request
date = '{{ logical_date }}' # execution_date is deprecated use 'logical_date' or 'data_interval_start' instead
request_body = { "logical_date": date }
json_body = json.dumps(request_body)
###

for env in envs:
    dag_factory = PeptoDAG(source_id=source_id,env=env,vars=env_vars)
    dag = dag_factory.assemble_dag()

    with dag:
        trigger_http_post_req = SimpleHttpOperator(
            task_id="trigger_http_post_req",
            http_conn_id=connection_id,
            endpoint='/api/v1/dags/dailyci_external_trigger/dagRuns',
            method='POST',
            data=json_body,
            headers={"Content-Type": "application/json"}
            # response_check=lambda response: response.json()['json']['priority'] == 5,
            # response_filter=lambda response: json.loads(response.text),
            # extra_options: Optional[Dict[str, Any]] = None,
            # log_response: bool = False,
            # auth_type: Type[AuthBase] = HTTPBasicAuth,
        )
        
        check_creds(secret_name, login, password)
        del_ids = del_conn_id(['test','test2','test3','test4-dev-int','test5-dev-int','test6-dev-int','test7-dev-int','test8-dev-int'])
        del_ids >> set_conn_id(connection_id,'http', 'https://dataos-airflow2-dev-int.bdbt-dev.hp8.us', str(login), str(password)) >> trigger_http_post_req
        data = extract()
        total_data = transform(data)
        load(total_data["total_order_value"])

