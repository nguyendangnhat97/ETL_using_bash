# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


working_directory = "/home/nhat/Documents/Projects/Data_Engineering/DE_with_python/final_assignment"

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'nhatnguyendang',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG

# define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

# define the unzip task
unzip_data = BashOperator(
    task_id='unzip',
    bash_command='cd /home/nhat/Documents/Projects/Data_Engineering/DE_with_python/final_assignment; tar -xvf tolldata.tgz',
    cwd=working_directory,
    dag=dag
)

#Extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='echo $(pwd); cut -d "," -f1-4 vehicle-data.csv > csv_data.csv',
    cwd=working_directory,
    dag=dag
)

#Extract data from tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 tollplaza-data.tsv | tr "\t" "," | tr -d \'\\r\' > tsv_data.csv',
    cwd=working_directory,
    dag=dag
    )

#Extract data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 59- payment-data.txt | tr " " "," > fixed_width_data.csv',
    cwd=working_directory,
    dag=dag
)

#Consolidate
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d ",," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    cwd=working_directory,
    dag=dag,
)

# define the second task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk \'$5 = toupper($5)\' < extracted_data.csv  > staging/transformed_data.csv',
    cwd=working_directory,
    dag=dag,
)

# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data