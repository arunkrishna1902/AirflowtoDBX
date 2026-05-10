import json

import airflow
from airflow.models import DAG
from airflow.models import Variable

# from airflow.operators.subdag_operator import SubDagOperator

from libs.dataos import send_failure_email, send_success_email
import dbt.dbt_spo_factory_flagging_laser as spo_factory

# Load trident variables from a json sting variable in Airflow
cumulus_vars = json.loads(Variable.get("cumulus-dbt_spo"))

# add cumulus_vars with cumulus-common-variables for vars used by databricks trigger
cumulus_vars["dbx_vars"] = Variable.get(
    "cumulus-common-variables", deserialize_json=True
)
# cumulus_vars["dbx_vars"]["job_id"] = cumulus_vars["JOB_ID"]
cumulus_vars["dbx_vars"]["previous_task_ids"] = ""
cumulus_vars["dbx_vars"]["source_id"] = ""
cumulus_vars["dbx_vars"]["env"] = (
    "ITG"
    if (cumulus_vars["env_vars"]["target"]).upper() == "STG"
    else (cumulus_vars["env_vars"]["target"]).upper()
)
# cumulus_vars["dbx_vars"]["run_params"] = json.dumps({"environment":cumulus_vars["dbx_vars"]["env"], "allowList_spo_process":"spo_flag", "laser_laser_process":"laser"})

#setting databricks env to stg
if cumulus_vars["dbx_vars"]["env"] != "DEV": 
    cumulus_vars["dbx_vars"]["DATABRICKS_ENDPOINT"] = "https://dataos-cumulus-stg.cloud.databricks.com"
    cumulus_vars["dbx_vars"]["DATABRICKS_CREDENTIALS"] = "arn:aws:secretsmanager:us-west-2:828361281741:secret:itg/k8s/airflow/cumulus/databricks-e2"
    cumulus_vars["dbx_vars"]["service_account_name"] = "cumulus-airflow2-itg"
    cumulus_vars["dbx_vars"]["ecr_image"] = "cumulus-airflow-base:REL-0.0.48"

access_control = cumulus_vars["default_access_control"]
# Input Vars
out_schema = cumulus_vars["out_schema"]
image_tag = cumulus_vars["released_tag"]

# jupyter_image_tag = cumulus_vars['released_jupyter_tag']

args = {
    "owner": "cumulus",
    "start_date": airflow.utils.dates.days_ago(8),
    "depends_on_past": False,
}

dag = DAG(
    dag_id="cumulus-dbt-spo-flagging-laser",
    default_args=args,
    schedule_interval=cumulus_vars.get("schedule"),  # get returns None if key not found
    max_active_runs=1,
    catchup=False,  # trident handles catchup internally
    params={"email_to": cumulus_vars["email_to"]},
    on_failure_callback=send_failure_email,
    on_success_callback=send_success_email,
    access_control=access_control,
    tags=["spo_remediation"],
)

image = Variable.get("ecr_repository") + f"dbt-cumulus:{image_tag}"
# jupyter_image = Variable.get('ecr_repository') + f'dst_jupyter_notebooks:{jupyter_image_tag}'

extra_vars = f"out_schema: {out_schema}"

# sub_dag = SubDagOperator(
#     subdag=spo_factory.build_sub_dag(parent_dag=dag,
#                                          child_dag_name='spo',
#                                          image=image,
#                                         #  jupyter_image=jupyter_image,
#                                          cumulus_vars=cumulus_vars,
#                                          first_run=False,
#                                          run_compare=True,
#                                          extra_vars=extra_vars),
#     task_id='spo',
#     dag=dag
# )

# spo_factory.populate_dag(
    # dag=dag, image=image, cumulus_vars=cumulus_vars, extra_vars=extra_vars
# )

# process dates
start_date_gotham = cumulus_vars["process_dates"]["start_date_gotham"]
end_date_gotham = cumulus_vars["process_dates"]["end_date_gotham"]

start_date_hppk = cumulus_vars["process_dates"]["start_date_hppk"]
end_date_hppk = cumulus_vars["process_dates"]["end_date_hppk"]

start_date_jam_ledm = cumulus_vars["process_dates"]["start_date_jam_ledm"]
end_date_jam_ledm = cumulus_vars["process_dates"]["end_date_jam_ledm"]

start_date_jam_mhit = cumulus_vars["process_dates"]["start_date_jam_mhit"]
end_date_jam_mhit = cumulus_vars["process_dates"]["end_date_jam_mhit"]

start_date_pelaser = cumulus_vars["process_dates"]["start_date_pelaser"]
end_date_pelaser = cumulus_vars["process_dates"]["end_date_pelaser"]

start_date_printernet = cumulus_vars["process_dates"]["start_date_printernet"]
end_date_printernet = cumulus_vars["process_dates"]["end_date_printernet"]

start_date_pmhit = cumulus_vars["process_dates"]["start_date_pmhit"]
end_date_pmhit = cumulus_vars["process_dates"]["end_date_pmhit"]

start_date_samsung = cumulus_vars["process_dates"]["start_date_samsung"]
end_date_samsung = cumulus_vars["process_dates"]["end_date_samsung"]

start_date_wja_ledm = cumulus_vars["process_dates"]["start_date_wja_ledm"]
end_date_wja_ledm = cumulus_vars["process_dates"]["end_date_wja_ledm"]

start_date_wja_mhit = cumulus_vars["process_dates"]["start_date_wja_mhit"]
end_date_wja_mhit = cumulus_vars["process_dates"]["end_date_wja_mhit"]

start_date_wppgen2 = cumulus_vars["process_dates"]["start_date_wppgen2"]
end_date_wppgen2 = cumulus_vars["process_dates"]["end_date_wppgen2"]

num_gotham_batches = (int(cumulus_vars["num_gotham_batches"]))
num_hppk_batches = (int(cumulus_vars["num_hppk_batches"]))
num_jam_ledm_batches = (int(cumulus_vars["num_jam_ledm_batches"]))
num_jam_mhit_batches = (int(cumulus_vars["num_jam_mhit_batches"]))
num_pelaser_batches = (int(cumulus_vars["num_pelaser_batches"]))
num_printernet_batches = (int(cumulus_vars["num_printernet_batches"]))
num_pmhit_batches = (int(cumulus_vars["num_pmhit_batches"]))
num_samsung_batches = (int(cumulus_vars["num_samsung_batches"]))
num_wja_ledm_batches = (int(cumulus_vars["num_wja_ledm_batches"]))
num_wja_mhit_batches = (int(cumulus_vars["num_wja_mhit_batches"]))
num_wppgen2_batches = (int(cumulus_vars["num_wppgen2_batches"]))

date_list_gotham = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_gotham,end_date_gotham,num_gotham_batches)
        ))
date_list_hppk = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_hppk,end_date_hppk,num_hppk_batches)
        ))
date_list_jam_ledm = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_jam_ledm,end_date_jam_ledm,num_jam_ledm_batches)
        ))
date_list_jam_mhit = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_jam_mhit,end_date_jam_mhit,num_jam_mhit_batches)
        ))
date_list_pelaser = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_pelaser,end_date_pelaser,num_pelaser_batches)
        ))
date_list_printernet = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_printernet,end_date_printernet,num_printernet_batches)
        ))
date_list_pmhit = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_pmhit,end_date_pmhit,num_pmhit_batches)
        ))
date_list_samsung = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_samsung,end_date_samsung,num_samsung_batches)
        ))
date_list_wja_ledm = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_wja_ledm,end_date_wja_ledm,num_wja_ledm_batches)
        ))
date_list_wja_mhit = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_wja_mhit,end_date_wja_mhit,num_wja_mhit_batches)
        ))
date_list_wppgen2 = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_date_wppgen2,end_date_wppgen2,num_wppgen2_batches)
        ))
        
# CONSUMER SOURCES

gotham = spo_factory.build_taskgroup(dag=dag,
    group_id='gotham',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_gotham,
    source_id='GOTHAM_LEDM'
    )
prev_task = gotham

hppk = spo_factory.build_taskgroup(dag=dag,
    group_id='hppk',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_hppk,
    source_id='HPPK_AMPV'
    )
prev_task >> hppk
prev_task = hppk


pelaser = spo_factory.build_taskgroup(dag=dag,
    group_id='pelaser',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_pelaser,
    source_id='PELASER_DPP'
    )
prev_task >> pelaser
prev_task = pelaser

samsung = spo_factory.build_taskgroup(dag=dag,
    group_id='samsung',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_samsung,
    source_id='SAMSUNG_AMPV'
    )
prev_task >> samsung
prev_task = samsung

gen2 = spo_factory.build_taskgroup(dag=dag,
    group_id='gen2',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_wppgen2,
    source_id='WPPGEN2_SCHEDULED'
    )
prev_task >> gen2
prev_task = gen2

# ENTERPRISE SOURCES

jam_ledm = spo_factory.build_taskgroup(dag=dag,
    group_id='jam_ledm',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_jam_ledm,
    source_id='JAM_LEDM_DPP'
    )
prev_task >> jam_ledm
prev_task = jam_ledm

jam_mhit = spo_factory.build_taskgroup(dag=dag,
    group_id='jam_mhit',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_jam_mhit,
    source_id='JAM_MHIT_DPP'
    )
prev_task >> jam_mhit
prev_task = jam_mhit

printernet = spo_factory.build_taskgroup(dag=dag,
    group_id='printernet',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_printernet,
    source_id='PELASER_PRINTERNET'
    )
prev_task >> printernet
prev_task = printernet

pmhit = spo_factory.build_taskgroup(dag=dag,
    group_id='pmhit',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_pmhit,
    source_id='PRINTERNET_MHIT_DPP'
    )
prev_task >> pmhit
prev_task = pmhit

wja_ledm = spo_factory.build_taskgroup(dag=dag,
    group_id='wja_ledm',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_wja_ledm,
    source_id='WJA_LEDM_DPP'
    )
prev_task >> wja_ledm
prev_task = wja_ledm

wja_mhit = spo_factory.build_taskgroup(dag=dag,
    group_id='wja_mhit',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_wja_mhit,
    source_id='WJA_MHIT_DPP'
    )
prev_task >> wja_mhit
prev_task = wja_mhit

# TESTS

test_laser_spo = spo_factory.build_taskgroup(dag=dag,
    group_id='test_laser_spo',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    is_test=True,
    selector_stmt="--profile laser --models tag:synthetic_privacy_object_laser"
    )
prev_task >> test_laser_spo
prev_task = test_laser_spo

test_laser_strnec = spo_factory.build_taskgroup(dag=dag,
    group_id='test_laser_strnec',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    is_test=True,
    selector_stmt="--profile laser --models tag:eea_strnec_flag_laser"
    )
prev_task >> test_laser_strnec
prev_task = test_laser_strnec

# CATCHUP RELATED BINS, SAME FOR ALL SOURCES
start_catchup = "2022-03-20"
end_catchup = "2022-03-31"
date_list_catchup = spo_factory.batch_dates(
    list(
        spo_factory.date_range(start_catchup,end_catchup,1)
        ))

# CATCHUP STEPS
# CONSUMER SOURCES

gotham_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='gotham_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='GOTHAM_LEDM'
    )
prev_task >> gotham_catchup
prev_task = gotham_catchup

hppk_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='hppk_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='HPPK_AMPV'
    )
prev_task >> hppk_catchup
prev_task = hppk_catchup


pelaser_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='pelaser_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='PELASER_DPP'
    )
prev_task >> pelaser_catchup
prev_task = pelaser_catchup

samsung_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='samsung_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='SAMSUNG_AMPV'
    )
prev_task >> samsung_catchup
prev_task = samsung_catchup

gen2_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='gen2_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='WPPGEN2_SCHEDULED'
    )
prev_task >> gen2_catchup
prev_task = gen2_catchup

# ENTERPRISE SOURCES

jam_ledm_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='jam_ledm_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='JAM_LEDM_DPP'
    )
prev_task >> jam_ledm_catchup
prev_task = jam_ledm_catchup

jam_mhit_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='jam_mhit_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='JAM_MHIT_DPP'
    )
prev_task >> jam_mhit_catchup
prev_task = jam_mhit_catchup

printernet_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='printernet_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='PELASER_PRINTERNET'
    )
prev_task >> printernet_catchup
prev_task = printernet_catchup

pmhit_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='pmhit_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='PRINTERNET_MHIT_DPP'
    )
prev_task >> pmhit_catchup
prev_task = pmhit_catchup

wja_ledm_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='wja_ledm_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='WJA_LEDM_DPP'
    )
prev_task >> wja_ledm_catchup
prev_task = wja_ledm_catchup

wja_mhit_catchup = spo_factory.build_taskgroup(dag=dag,
    group_id='wja_mhit_catchup',
    image=image,
    cumulus_vars=cumulus_vars,
    extra_vars=extra_vars,
    date_list=date_list_catchup,
    source_id='WJA_MHIT_DPP'
    )
prev_task >> wja_mhit_catchup
prev_task = wja_mhit_catchup
