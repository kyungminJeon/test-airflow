from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# DAG 정의
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),  # DAG를 얼마나 자주 실행할 것인지 정의
)

# DAG에 추가할 태스크 정의
task1 = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello Airflow!"',
    dag=dag,
)

task2 = BashOperator(
    task_id='print_world',
    bash_command='echo "World!"',
    dag=dag,
)

# 태스크 간의 실행 순서 정의
task1 >> task2

# 만든 DAG를 Airflow에 추가
if __name__ == "__main__":
    dag.cli()
