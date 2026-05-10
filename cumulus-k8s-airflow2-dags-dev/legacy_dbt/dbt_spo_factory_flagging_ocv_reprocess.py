import os
import re
import yaml
import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
from dataos_operators.dataos_pod_operator import DataOSPodOperator
from kubernetes.client import models as k8s
from airflow.utils.task_group import TaskGroup

PROJECT = "cumulus"
PROJECT_PATH = "/cumulus"

startup_timeout_seconds = int(Variable.get("pod_operator_startup_timeout"))
service_account_name = Variable.get("cumulus-dbt_spo", deserialize_json=True).get(
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

def build_taskgroup(dag,
                    group_id,
                    image,
                    cumulus_vars,
                    extra_vars=None,
                    retry_on_failure=False,
                    date_list=None,
                    source_id='',
                    is_test=False,
                    selector_stmt=''
                    ):

    tooltip = f'TaskGroup for {group_id}'
    with TaskGroup(group_id=group_id, dag=dag, tooltip=tooltip) as taskgroup:

        if retry_on_failure:
            retry_count = 2
        else:
            retry_count = 0

        if 'email_to' in dag.params:
            send_email = True
            email_to = dag.params['email_to']
        else:
            send_email = False
            email_to = ''

        dbt_run_cmd = "run"
        dbt_run_operation_cmd = "run-operation"
        dbt_seed_cmd = "seed"
        dbt_test_cmd = "test"

        namespace = Variable.get('namespace')

        env_vars = __make_env_vars(cumulus_vars)

        # Setup the vars
        dbt_vars = make_vars(cumulus_vars['dbt_vars'],
                             extra_vars)

        #dummy_task = DummyOperator(task_id='start', dag=dag)
        #current_task = dummy_task

        dbt_vars_dict = vars_to_dict(dbt_vars)

        current_task = DummyOperator(task_id=f"{group_id}_start", dag=dag)

        prev_task = current_task
        if date_list != None:
            for i in date_list:
                batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
                batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
                
                dynamic_task = _get_dbt_operation_task(
                    dag,
                    f"batch-{batch_start}-{batch_end}",
                    namespace,
                    image,
                    dbt_run_operation_cmd,
                    selector=f"""reprocess_synthetic_privacy_object_ocv --profile ocv --args "{{table_name: ocv_history, source_id: {source_id}, start_date: {i[0]}, end_date: {i[1]}}}" """,
                    vars=dbt_vars,
                    env_vars=env_vars,
                    retries=retry_count,
                )
                prev_task >> dynamic_task
                prev_task = dynamic_task

        if is_test:
            dynamic_task = _get_dbt_task(
                dag,
                "test",
                namespace,
                image,
                dbt_test_cmd,
                selector=selector_stmt,
                vars=dbt_vars,
                env_vars=env_vars,
                retries=retry_count,
            )
            prev_task >> dynamic_task
            prev_task = dynamic_task
        return taskgroup
