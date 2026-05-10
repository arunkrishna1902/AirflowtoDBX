import os
import re
import yaml
from datetime import timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from kubernetes.client import models as k8s
import datetime

PROJECT = "cumulus"
PROJECT_PATH = "/cumulus"

startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))
service_account_name = Variable.get("cumulus-dbt_eea", deserialize_json=True).get(
    "service_account_name"
)

tolerations = Variable.get("dst_tolerations", deserialize_json=True)
node_selector = Variable.get("dst_node_selector", deserialize_json=True)
pod_template_file = Variable.get("default_pod_template_x_account")

home_dir_dbt = "/root"

dbt_common_vars = Variable.get("dbt", deserialize_json=True)

nonquoted_comma = '(?!\B"[^"]*),(?![^"]*"\B)'
nonquoted_colon = '(?!\B"[^"]*):(?![^"]*"\B)'

#creates a generator for date range by interval
#    batch = (total days between date range)/interval
#    ex: list(date_range()) == [start,  start+batch.....end]
#    ex: ['2010-02-01', '2011-01-24', ..... '2021-11-01']
def date_range(start, end, interval):
    start = datetime.datetime.strptime(start,"%Y-%m-%d")
    end = datetime.datetime.strptime(end,"%Y-%m-%d")
    diff = (end  - start ) / interval
    for i in range(interval):
        yield (start + diff * i).strftime("%Y-%m-%d")
    yield end.strftime("%Y-%m-%d")

#creates list of lists of batches
#ex: [[START_DATE, '2011-01-23'], ['2011-01-24', '2012-01-16'], ....['2020-11-08', END_DATE]]
def batch_dates(date_list):
    result = list()
    for i in range(len(date_list)-1):
        curr_batch = datetime.datetime.strptime(date_list[i],"%Y-%m-%d")
        next_batch = datetime.datetime.strptime(date_list[i+1],"%Y-%m-%d")

        if i == len(date_list)-2: #last batch
            date1 = curr_batch
            date2 = next_batch
        else:
            date1 = curr_batch
            date2 = next_batch - datetime.timedelta(days=1)
            
        result.append([date1.strftime("%Y-%m-%d"),date2.strftime("%Y-%m-%d")])
    return result

def _check_run_weekly(execution_date, force_weekly, **kwargs):
    # execution_date corresponds to the start of the period and the job is scheduled at the end of the period. This makes the execution_date look like a day behind the actual run date
    if execution_date.weekday() == 6 or force_weekly:
        return "weekly"
    return "skip-weekly"


def _check_run_monthly(execution_date, force_monthly, **kwargs):
    # execution_date corresponds to the start of the period and the job is scheduled at the end of the period. This makes the execution_date look like a day behind the actual run date
    # we are triggering monthly on the 2nd
    if execution_date.day == 1 or force_monthly:
        return "monthly"
    return "skip-monthly"


def _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs):
    pod_name = re.sub("[_.]", "-", f"{dag.dag_id}-{name}")
    pod = DataOSPodOperator(
        namespace=namespace,
        image=image,
        cmds=["bash", "-cx"],
        arguments=[cmd],
        name=pod_name,
        task_id=name,
        get_logs=True,
        in_cluster=True,
        is_delete_operator_pod=True,
        retry_delay=timedelta(seconds=300),
        image_pull_policy="Always",
        startup_timeout_seconds=startup_timeout_seconds,
        service_account_name=service_account_name,
        pod_template_file=pod_template_file,
        node_selector=node_selector,
        pool="rem_pool_laser",
        tolerations=tolerations,
        dag=dag,
        env_vars=env_vars,
        **kwargs,
    )

    return pod


def _get_dbt_task(
    dag, name, namespace, image, dbt_cmd, selector, vars, env_vars, **kwargs
):
    run_id = "{{ run_id }}"
    full_task_name = ".".join([dag.dag_id, name])
    version_cmd = 'echo "version: ${sw_ver}"'
    dbt_profile_cmd = f"sleep 3 && python /{PROJECT}/analysis/etl_create_dbt_profile.py"
    # dbt_profile_cmd = f"sleep 3 && python -m etl_dbt.create_dbt_profile default"
    execution_date = "{{ execution_date }}"
    run_argument = ", ".join(
        [f'\\"{k}\\": \\"{v}\\"' for (k, v) in vars_to_dict(vars).items()]
    )
    vars = vars + f', run_argument: "{{{run_argument}}}", run_id: {execution_date}'
    dbt_exec_cmd = f"python -m dbt_lib.execute_dbt {PROJECT} {PROJECT_PATH} {full_task_name} {run_id} {dbt_cmd} '{selector}' '{vars}'"

    cmd = f"{version_cmd} && {dbt_profile_cmd} && {dbt_exec_cmd}"
    env_vars["HOME_DIR"] = home_dir_dbt

    return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


def _get_dbt_operation_task(
    dag, name, namespace, image, dbt_cmd, selector, vars, env_vars, **kwargs
):
    run_id = "{{ run_id }}"
    full_task_name = ".".join([dag.dag_id, name])
    version_cmd = 'echo "version: ${sw_ver}"'
    dbt_profile_cmd = f"sleep 3 && python /{PROJECT}/analysis/etl_create_dbt_profile.py"
    # dbt_profile_cmd = f"sleep 3 && python -m etl_dbt.create_dbt_profile default"
    execution_date = "{{ execution_date }}"
    run_argument = ", ".join(
        [f'\\"{k}\\": \\"{v}\\"' for (k, v) in vars_to_dict(vars).items()]
    )
    vars = vars + f', run_argument: "{{{run_argument}}}", run_id: {execution_date}'
    dbt_exec_cmd = f"python /{PROJECT}/analysis/execute_dbt_wrapper.py {PROJECT} {PROJECT_PATH} {full_task_name} {run_id} {dbt_cmd} '{selector}' '{vars}'"

    cmd = f"{version_cmd} && {dbt_profile_cmd} && {dbt_exec_cmd}"
    env_vars["HOME_DIR"] = home_dir_dbt

    return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


def _get_analysis_task(dag, name, namespace, image, script, vars, env_vars, **kwargs):
    version_cmd = 'echo "version: ${sw_ver}"'
    # dbt_profile_cmd = f"sleep 3 && python -m etl_dbt.create_dbt_profile default"
    dbt_profile_cmd = f"sleep 3 && python /{PROJECT}/analysis/etl_create_dbt_profile.py"
    analysis_exec_cmd = (
        f"python /{PROJECT}/scripts/run_analysis.py {script} --vars '{{ {vars} }}'"
    )

    cmd = f"{version_cmd} && {dbt_profile_cmd} && {analysis_exec_cmd}"
    env_vars["HOME_DIR"] = home_dir_dbt

    return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


def _get_python_task(dag, name, namespace, image, script, args, env_vars, **kwargs):
    run_id = "{{ run_id }}"
    full_task_name = ".".join([dag.dag_id, name])
    version_cmd = 'echo "version: ${sw_ver}"'
    dbt_profile_cmd = f"sleep 3 && python /{PROJECT}/analysis/etl_create_dbt_profile.py"
    py_exec_cmd = f"python /{PROJECT}/{script} {args}"
    # vars = vars + f', run_argument: \"{{{run_argument}}}\", run_id: {execution_date}'
    # dbt_exec_cmd = f"python -m dbt_lib.execute_dbt {PROJECT} {PROJECT_PATH} {full_task_name} {run_id} {dbt_cmd} '{selector}' '{vars}'"

    cmd = f"{version_cmd} && {dbt_profile_cmd} && {py_exec_cmd}"
    env_vars["HOME_DIR"] = home_dir_dbt

    return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


def _get_databricks_task(dag, name, namespace, image, cmd, env_vars, **kwargs):
    return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


# def _get_jupyter_task(dag, name, namespace, image, notebook, yml_vars, env_vars, **kwargs):
#     (notebook_path, notebook_file_name) = os.path.split(notebook)

#     cmd = f'papermill -y "{yml_vars}" --cwd "{notebook_path}" {notebook} -'
#     env_vars["HOME_DIR"] = home_dir_jupyter

#     return _get_k8s_pod(dag, name, namespace, image, cmd, env_vars, **kwargs)


# Assumes each arg is a string of comma separated key-value pairs (colon between key and value)
# returns a merged list where later strings can override values from earlier strings
def make_vars(*args):
    var_dict = {}

    for arg in args:
        if isinstance(arg, str) and not arg == "":
            var_dict.update(
                dict(
                    [x.strip() for x in re.split(nonquoted_colon, chunk)]
                    for chunk in re.split(nonquoted_comma, arg)
                )
            )

    return ", ".join([f"{k}: {v}" for (k, v) in var_dict.items()])


def vars_to_dict(var_string):
    return {
        var[0].strip(): var[1].strip()
        for var in list(
            map(
                lambda chunk: re.split(nonquoted_colon, chunk),
                re.split(nonquoted_comma, var_string.strip()),
            )
        )
    }


def __make_env_vars(input_vars):
    env_vars = input_vars["env_vars"]
    env_vars["dbt_s3_bucket"] = dbt_common_vars.get("dbt_s3_bucket")
    env_vars["dbt_s3_prefix"] = dbt_common_vars.get("dbt_s3_prefix")
    return env_vars


def populate_dag(dag, image, cumulus_vars, extra_vars=None, retry_on_failure=False):
    """
    :param dag: the parent DAG for the subdag
    :type dag: DAG
    :param image: Docker image to use
    :type image: str
    :param extra_vars: comma separated string of additional dbt vars
    :type extra_vars: str
    :return: sub DAG
    """

    # Leaving in place if we want to add retry logic
    if retry_on_failure:
        retry_count = 2
    else:
        retry_count = 0

    if "email_to" in dag.params:
        send_email = True
        email_to = dag.params["email_to"]
    else:
        send_email = False
        email_to = ""

    dbt_run_cmd = "run"
    dbt_run_operation_cmd = "run-operation"
    dbt_seed_cmd = "seed"
    dbt_test_cmd = "test"

    namespace = Variable.get("namespace")

    env_vars = __make_env_vars(cumulus_vars)

    _target = cumulus_vars["env_vars"]["target"]
    environment = "itg" if (_target == "stg") else _target

    # Setup the vars
    dbt_vars = make_vars(
        cumulus_vars["dbt_vars"],
        #  'first_run: ' + str(first_run).lower(),
        extra_vars,
    )

    dummy_task = DummyOperator(task_id="start", dag=dag)
    current_task = dummy_task

    dbt_vars_dict = vars_to_dict(dbt_vars)

    # vars for databricks
    dbx_image = (
        f"{Variable.get('ecr_repository')}{cumulus_vars['dbx_vars'].get('ecr_image')}"
    )
    dbx_command = cumulus_vars["dbx_vars"].get("run_command")

    # process dates
    start_date_pelaser = cumulus_vars["process_dates"]["start_date_pelaser"]
    end_date_pelaser = cumulus_vars["process_dates"]["end_date_pelaser"]

    start_date_gotham = cumulus_vars["process_dates"]["start_date_gotham"]
    end_date_gotham = cumulus_vars["process_dates"]["end_date_gotham"]

    start_date_wppgen2 = cumulus_vars["process_dates"]["start_date_wppgen2"]
    end_date_wppgen2 = cumulus_vars["process_dates"]["end_date_wppgen2"]

    start_date_hppk = cumulus_vars["process_dates"]["start_date_hppk"]
    end_date_hppk = cumulus_vars["process_dates"]["end_date_hppk"]

    start_date_samsung = cumulus_vars["process_dates"]["start_date_samsung"]
    end_date_samsung = cumulus_vars["process_dates"]["end_date_samsung"]

    start_date_pre_target_laser = cumulus_vars["process_dates"][
        "start_date_pre_target_laser"
    ]
    end_date_pre_target_laser = cumulus_vars["process_dates"][
        "end_date_pre_target_laser"
    ]

    start_date_post_target_laser = cumulus_vars["process_dates"][
        "start_date_post_target_laser"
    ]
    end_date_post_target_laser = cumulus_vars["process_dates"][
        "end_date_post_target_laser"
    ]
    start_date_ocv = cumulus_vars["process_dates"]["start_date_ocv"]
    end_date_ocv = cumulus_vars["process_dates"]["end_date_ocv"]
    num_ocv_batches = int(cumulus_vars["num_ocv_batches"])
    
    date_list = batch_dates(
        list(
            date_range(start_date_ocv,end_date_ocv,num_ocv_batches)
            ))

    # Keeping incase we want to add this logic to our setup
    # setup schema lets each dev user have their own custom schema to run in for prototyping
    # clean on start will clean out the schema before running

    # if setup_schema:
    #     setup = _get_analysis_task(sub_dag, 'setup', namespace, image, 'setup_schema.sql',
    #                                dbt_vars, env_vars=env_vars)
    #     current_task >> setup
    #     current_task = setup

    # if clean_on_start:
    #     clean_start = _get_analysis_task(sub_dag, 'clean-start', namespace, image,
    #                                      'clean_schema.sql', dbt_vars, env_vars=env_vars)
    #     current_task >> clean_start
    #     current_task = clean_start

    # eea = _get_dbt_task(dag, 'eea', namespace, image, dbt_run_cmd,
    #                          selector='--models tag:test', vars=dbt_vars, env_vars=env_vars, retries=retry_count)

    test_ocv_flagging = _get_dbt_task(
        dag,
        "test-ocv-flagging",
        namespace,
        image,
        dbt_test_cmd,
        selector="--profile ocv --models tag:ocv_flagging",
        vars=dbt_vars,
        env_vars=env_vars,
        retries=retry_count,
    )

    # TODO: OCV Disabled for now, we will need to figure out the OCV Secrets as we need to use auto_ocv for flagging to have the correct permissions set
    prev_flagging = current_task
    for i in date_list:
        batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
        batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
    
        dbt_vars = make_vars(
            cumulus_vars["dbt_vars"],
            extra_vars,
            f"start_date_ocv_model: {i[0]}",
            f"end_date_ocv_model: {i[1]}",
        )
        
        ocv_flagging = _get_dbt_task(
            dag,
            f"flagging-ocv-{batch_start}-{batch_end}",
            namespace,
            image,
            dbt_run_cmd,
            selector="--profile ocv --models tag:ocv_flagging",
            vars=dbt_vars,
            env_vars=env_vars,
            retries=retry_count,
        )
        
        prev_flagging >> ocv_flagging
        prev_flagging = ocv_flagging
    
    ocv_flagging >> test_ocv_flagging
    return dag
