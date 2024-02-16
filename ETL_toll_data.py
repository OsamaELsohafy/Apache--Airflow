from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Defining DAG arguments
default_args = {
    'owner': 'Osama ELsohafy',
    'start_date': days_ago(0),
    'email': ['imb@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Define the first task

# Define the task to unzip the data using BashOperator
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='unzip -o /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -d /home/project/airflow/dags/finalassignment/staging/ || exit 1',
    dag=dag,
)

# Define the task to extract data from CSV using BashOperator
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='csvcut -c Rowid,Timestamp,"Anonymized Vehicle number","Vehicle type" /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv || exit 1',
    dag=dag,
)

# Define the task to extract data from TSV using BashOperator
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='awk -F"\t" \'{print $1 "," $2 "," $3}\' /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv || exit 1',
    dag=dag,
)

# Define the task to extract data from fixed width file using BashOperator
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='awk \'BEGIN { FIELDWIDTHS = "3 5 10" } {print $1 "," $2}\' /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv || exit 1',
    dag=dag,
)

# Define the task to consolidate data using BashOperator
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d "," /home/project/airflow/dags/finalassignment/staging/csv_data.csv /home/project/airflow/dags/finalassignment/staging/tsv_data.csv /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv || exit 1',
    dag=dag,
)

# Define the task to transform data using BashOperator
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='awk \'BEGIN {FS = OFS = ","} { $4 = toupper($4) } 1\' /home/project/airflow/dags/finalassignment/staging/extracted_data.csv > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv || exit 1',
    dag=dag,
)

# Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
