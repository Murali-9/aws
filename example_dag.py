from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator

def print_hello():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 27),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('example_dag', default_args=default_args, schedule_interval='@daily')

#Dummy start Task
start_task= DummyOperator(
        task_id='start',
        dag=dag,
    )   

# PythonOperator
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)

# BashOperator
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "This is a Bash command"',
    dag=dag
)

# EmailOperator
email_task = EmailOperator(
    task_id='email_task',
    to='murali.123.m1@gmail.com',
    subject='Airflow DAG Execution',
    html_content='The Airflow DAG has been executed successfully.',
    dag=dag
)

#Dummy Task End

end_task = DummyOperator(
        task_id = 'end',
      dag=dag  
)

# Define task dependencies
start_task >> hello_task
start_task >> bash_task
hello_task >> email_task
bash_task >> end_task
email_task >> end_task
