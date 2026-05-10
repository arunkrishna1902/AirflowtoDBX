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
                    selector=f"""update_synthetic_privacy_object_ocv --profile ocv --args "{{table_name: ocv_history, source_id: {source_id}, start_date: {i[0]}, end_date: {i[1]}}}" """,
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

# def populate_dag(dag, image, cumulus_vars, extra_vars=None, retry_on_failure=False):
#     """
#     :param dag: the parent DAG for the subdag
#     :type dag: DAG
#     :param image: Docker image to use
#     :type image: str
#     :param extra_vars: comma separated string of additional dbt vars
#     :type extra_vars: str
#     :return: sub DAG
#     """

#     # Leaving in place if we want to add retry logic
#     if retry_on_failure:
#         retry_count = 2
#     else:
#         retry_count = 0

#     if "email_to" in dag.params:
#         send_email = True
#         email_to = dag.params["email_to"]
#     else:
#         send_email = False
#         email_to = ""

#     dbt_run_cmd = "run"
#     dbt_run_operation_cmd = "run-operation"
#     dbt_seed_cmd = "seed"
#     dbt_test_cmd = "test"

#     namespace = Variable.get("namespace")

#     env_vars = __make_env_vars(cumulus_vars)

#     _target = cumulus_vars["env_vars"]["target"]
#     environment = "itg" if (_target == "stg") else _target

#     # Setup the vars
#     dbt_vars = make_vars(
#         cumulus_vars["dbt_vars"],
#         #  'first_run: ' + str(first_run).lower(),
#         extra_vars,
#     )

#     dummy_task = DummyOperator(task_id="start", dag=dag)
#     current_task = dummy_task

#     dbt_vars_dict = vars_to_dict(dbt_vars)

#     # vars for databricks
#     dbx_image = (
#         f"{Variable.get('ecr_repository')}{cumulus_vars['dbx_vars'].get('ecr_image')}"
#     )
#     dbx_command = cumulus_vars["dbx_vars"].get("run_command")

#     # process dates

#     start_date_gotham = cumulus_vars["process_dates"]["start_date_gotham"]
#     end_date_gotham = cumulus_vars["process_dates"]["end_date_gotham"]

#     start_date_hppk = cumulus_vars["process_dates"]["start_date_hppk"]
#     end_date_hppk = cumulus_vars["process_dates"]["end_date_hppk"]

#     start_date_jam_ledm = cumulus_vars["process_dates"]["start_date_jam_ledm"]
#     end_date_jam_ledm = cumulus_vars["process_dates"]["end_date_jam_ledm"]

#     start_date_jam_mhit = cumulus_vars["process_dates"]["start_date_jam_mhit"]
#     end_date_jam_mhit = cumulus_vars["process_dates"]["end_date_jam_mhit"]

#     start_date_pelaser = cumulus_vars["process_dates"]["start_date_pelaser"]
#     end_date_pelaser = cumulus_vars["process_dates"]["end_date_pelaser"]

#     start_date_printernet = cumulus_vars["process_dates"]["start_date_printernet"]
#     end_date_printernet = cumulus_vars["process_dates"]["end_date_printernet"]

#     start_date_pmhit = cumulus_vars["process_dates"]["start_date_pmhit"]
#     end_date_pmhit = cumulus_vars["process_dates"]["end_date_pmhit"]

#     start_date_samsung = cumulus_vars["process_dates"]["start_date_samsung"]
#     end_date_samsung = cumulus_vars["process_dates"]["end_date_samsung"]

#     start_date_wja_ledm = cumulus_vars["process_dates"]["start_date_wja_ledm"]
#     end_date_wja_ledm = cumulus_vars["process_dates"]["end_date_wja_ledm"]

#     start_date_wja_mhit = cumulus_vars["process_dates"]["start_date_wja_mhit"]
#     end_date_wja_mhit = cumulus_vars["process_dates"]["end_date_wja_mhit"]

#     start_date_wppgen2 = cumulus_vars["process_dates"]["start_date_wppgen2"]
#     end_date_wppgen2 = cumulus_vars["process_dates"]["end_date_wppgen2"]

#     num_gotham_batches = (int(cumulus_vars["num_gotham_batches"]) * 2)
#     num_hppk_batches = (int(cumulus_vars["num_hppk_batches"]) * 2)
#     num_jam_ledm_batches = (int(cumulus_vars["num_jam_ledm_batches"]) * 2)
#     num_jam_mhit_batches = (int(cumulus_vars["num_jam_mhit_batches"]) * 2)
#     num_pelaser_batches = (int(cumulus_vars["num_pelaser_batches"]) * 2)
#     num_printernet_batches = (int(cumulus_vars["num_printernet_batches"]) * 2)
#     num_pmhit_batches = (int(cumulus_vars["num_pmhit_batches"]) * 2)
#     num_samsung_batches = (int(cumulus_vars["num_samsung_batches"]) * 2)
#     num_wja_ledm_batches = (int(cumulus_vars["num_wja_ledm_batches"]) * 2)
#     num_wja_mhit_batches = (int(cumulus_vars["num_wja_mhit_batches"]) * 2)
#     num_wppgen2_batches = (int(cumulus_vars["num_wppgen2_batches"]) * 2)

#     date_list_gotham = batch_dates(
#         list(
#             date_range(start_date_gotham,end_date_gotham,num_gotham_batches)
#             ))
#     date_list_hppk = batch_dates(
#         list(
#             date_range(start_date_hppk,end_date_hppk,num_hppk_batches)
#             ))
#     date_list_jam_ledm = batch_dates(
#         list(
#             date_range(start_date_jam_ledm,end_date_jam_ledm,num_jam_ledm_batches)
#             ))
#     date_list_jam_mhit = batch_dates(
#         list(
#             date_range(start_date_jam_mhit,end_date_jam_mhit,num_jam_mhit_batches)
#             ))
#     date_list_pelaser = batch_dates(
#         list(
#             date_range(start_date_pelaser,end_date_pelaser,num_pelaser_batches)
#             ))
#     date_list_printernet = batch_dates(
#         list(
#             date_range(start_date_printernet,end_date_printernet,num_printernet_batches)
#             ))
#     date_list_pmhit = batch_dates(
#         list(
#             date_range(start_date_pmhit,end_date_pmhit,num_pmhit_batches)
#             ))
#     date_list_samsung = batch_dates(
#         list(
#             date_range(start_date_samsung,end_date_samsung,num_samsung_batches)
#             ))
#     date_list_wja_ledm = batch_dates(
#         list(
#             date_range(start_date_wja_ledm,end_date_wja_ledm,num_wja_ledm_batches)
#             ))
#     date_list_wja_mhit = batch_dates(
#         list(
#             date_range(start_date_wja_mhit,end_date_wja_mhit,num_wja_mhit_batches)
#             ))
#     date_list_wppgen2 = batch_dates(
#         list(
#             date_range(start_date_wppgen2,end_date_wppgen2,num_wppgen2_batches)
#             ))
            

#     gotham_task = DummyOperator(task_id="gotham_start", dag=dag)
#     hppk_task = DummyOperator(task_id="hppk_start", dag=dag)
#     jam_ledm_task = DummyOperator(task_id="jam_ledm_start", dag=dag)
#     jam_mhit_task = DummyOperator(task_id="jam_mhit_start", dag=dag)
#     pelaser_task = DummyOperator(task_id="pelaser_start", dag=dag)
#     printernet_task = DummyOperator(task_id="printernet_start", dag=dag)
#     pmhit_task = DummyOperator(task_id="pmhit_start", dag=dag)
#     samsung_task = DummyOperator(task_id="samsung_start", dag=dag)
#     wja_ledm_task = DummyOperator(task_id="wja_ledm_start", dag=dag)
#     wja_mhit_task = DummyOperator(task_id="wja_mhit_start", dag=dag)
#     wppgen2_task = DummyOperator(task_id="wppgen2_start", dag=dag)

            
#     # start_date_pre_target_laser = cumulus_vars["process_dates"][
#     #     "start_date_pre_target_laser"
#     # ]
#     # end_date_pre_target_laser = cumulus_vars["process_dates"][
#     #     "end_date_pre_target_laser"
#     # ]

#     # start_date_post_target_laser = cumulus_vars["process_dates"][
#     #     "start_date_post_target_laser"
#     # ]
#     # end_date_post_target_laser = cumulus_vars["process_dates"][
#     #     "end_date_post_target_laser"
#     # ]


#     # Keeping incase we want to add this logic to our setup
#     # setup schema lets each dev user have their own custom schema to run in for prototyping
#     # clean on start will clean out the schema before running

#     # if setup_schema:
#     #     setup = _get_analysis_task(sub_dag, 'setup', namespace, image, 'setup_schema.sql',
#     #                                dbt_vars, env_vars=env_vars)
#     #     current_task >> setup
#     #     current_task = setup

#     # if clean_on_start:
#     #     clean_start = _get_analysis_task(sub_dag, 'clean-start', namespace, image,
#     #                                      'clean_schema.sql', dbt_vars, env_vars=env_vars)
#     #     current_task >> clean_start
#     #     current_task = clean_start

#     # spo = _get_dbt_task(dag, 'spo', namespace, image, dbt_run_cmd,
#     #                          selector='--models tag:test', vars=dbt_vars, env_vars=env_vars, retries=retry_count)

#     prev_task = gotham_task
#     for i in date_list_gotham:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_gotham = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-gotham-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: GOTHAM_LEDM, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_gotham
#         prev_task = add_spo_flag_gotham

#     prev_task >> hppk_task
#     prev_task = hppk_task
#     for i in date_list_hppk:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_hppk = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-hppk-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: HPPK_AMPV, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_hppk
#         prev_task = add_spo_flag_hppk

#     prev_task >> jam_ledm_task
#     prev_task = jam_ledm_task
#     for i in date_list_jam_ledm:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_jam_ledm = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-jam-ledm-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: JAM_LEDM_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_jam_ledm
#         prev_task = add_spo_flag_jam_ledm

#     prev_task >> jam_mhit_task
#     prev_task = jam_mhit_task
#     for i in date_list_jam_mhit:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_jam_mhit = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-jam-mhit-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: JAM_MHIT_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_jam_mhit
#         prev_task = add_spo_flag_jam_mhit

#     prev_task >> pelaser_task
#     prev_task = pelaser_task
#     for i in date_list_pelaser:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_pelaser = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-pelaser-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: PELASER_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_pelaser
#         prev_task = add_spo_flag_pelaser
        
#     prev_task >> printernet_task
#     prev_task = printernet_task
#     for i in date_list_printernet:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_printernet = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-printernet-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: PELASER_PRINTERNET, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_printernet
#         prev_task = add_spo_flag_printernet

#     prev_task >> pmhit_task
#     prev_task = pmhit_task
#     for i in date_list_pmhit:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_pmhit = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-pmhit-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: PRINTERNET_MHIT_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_pmhit
#         prev_task = add_spo_flag_pmhit

#     prev_task >> samsung_task
#     prev_task = samsung_task
#     for i in date_list_samsung:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_samsung = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-samsung-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: SAMSUNG_AMPV, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_samsung
#         prev_task = add_spo_flag_samsung

#     prev_task >> wja_ledm_task
#     prev_task = wja_ledm_task
#     for i in date_list_wja_ledm:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_wja_ledm = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-wja-ledm-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: WJA_LEDM_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_wja_ledm
#         prev_task = add_spo_flag_wja_ledm

#     prev_task >> wja_mhit_task
#     prev_task = wja_mhit_task
#     for i in date_list_wja_mhit:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_wja_mhit = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-wja-mhit-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: WJA_MHIT_DPP, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_wja_mhit
#         prev_task = add_spo_flag_wja_mhit

#     prev_task >> wppgen2_task
#     prev_task = wppgen2_task
#     for i in date_list_wppgen2:
#         batch_start = datetime.datetime.strptime(i[0], "%Y-%m-%d").strftime("%Y.%m.%d")
#         batch_end = datetime.datetime.strptime(i[1], "%Y-%m-%d").strftime("%Y.%m.%d")
        
#         add_spo_flag_wppgen2 = _get_dbt_operation_task(
#             dag,
#             f"add-spo-flag-wppgen2-{batch_start}-{batch_end}",
#             namespace,
#             image,
#             dbt_run_operation_cmd,
#             selector=f"""update_synthetic_privacy_object --profile laser --args "{{source_id: WPPGEN2_SCHEDULED, source_name: laser, start_date: {i[0]}, end_date: {i[1]}}}" """,
#             vars=dbt_vars,
#             env_vars=env_vars,
#             retries=retry_count,
#         )
#         prev_task >> add_spo_flag_wppgen2
#         prev_task = add_spo_flag_wppgen2
        
#     test_laser_spo = _get_dbt_task(
#         dag,
#         "test-laser-spo",
#         namespace,
#         image,
#         dbt_test_cmd,
#         selector="--profile laser --models tag:synthetic_privacy_object_laser",
#         vars=dbt_vars,
#         env_vars=env_vars,
#         retries=retry_count,
#     )
#     prev_task >> test_laser_spo
    
#     return dag
