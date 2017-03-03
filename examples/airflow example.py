from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 2, 6),
    'email': ['zhen-peng.yang@hpe.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    'end_date': datetime(2017, 3, 8),
}

dag = DAG(
    'test_arvin', default_args=default_args, schedule_interval=timedelta(1))

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='Start',
    bash_command='echo Start',
    dag=dag)

t2 = BashOperator(
    task_id='fscl_prd',
    bash_command='spark-submit --master yarn /opt/app/NewTechPOC/src/radar/ic/ic_fi1_1051041_fscl_prd.py',
    retries=3,
    dag=dag)


t2.set_upstream(t1)
