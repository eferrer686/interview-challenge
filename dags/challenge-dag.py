from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from includes.main import get_api_data_into_db, write_aggregated_data, join_csv_data_and_latest
from includes.db_manager import setup_db

args = {
    "owner": "Eduardo Ferrer",
    'start_date': days_ago(0)
}

dag = DAG(
    dag_id='challenge_dag',
    default_args=args,
    schedule_interval='@hourly'
)

with dag:
    setup_postgres_db_task = PythonOperator(
        task_id='setup_postgres_db',
        python_callable=setup_db
    )

    retrieve_api_data_task = PythonOperator(
        task_id='retrieve_api_data',
        python_callable=get_api_data_into_db
    )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=write_aggregated_data
    )

    aggregate_customer_data_task = PythonOperator(
        task_id='aggregate_customer_data',
        python_callable=join_csv_data_and_latest
    )



    setup_postgres_db_task.set_downstream(retrieve_api_data_task)
    retrieve_api_data_task.set_downstream(aggregate_data_task)
    retrieve_api_data_task.set_downstream(aggregate_customer_data_task)