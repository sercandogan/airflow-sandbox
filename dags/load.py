from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta
import pandas as pd
import numpy as np

TYPES = {"customer_id": str,
         "gender": str,
         "senior_citizen": bool,
         "partner": bool,
         "dependents": bool,
         "tenure": int,
         "phone_service": bool,
         "multiple_lines": bool,
         "internet_service": str,
         "online_security": bool,
         "online_backup": bool,
         "device_protection": bool,
         "tech_support": bool,
         "streaming_tv": bool,
         "streaming_movies": bool,
         "contract": str,
         "paperless_billing": bool,
         "payment_method": str,
         "monthly_charges": float,
         "total_charges": float,
         "churn": bool
         }
POSTGRES_CONN = "dw"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 12),
    'email': ['sercan@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


def clean_data(data_path, **kwargs):
    df = pd.read_csv(data_path)
    # list of whitespace columns
    whitespace_columns = (df.isin([" ", ""]).sum() > 0)[df.isin([" ", ""]).sum() > 0].keys().to_list()
    # replace with NAN
    for col in whitespace_columns:
        df[col] = df[col].replace(" ", np.nan)
    df.dropna(inplace=True)  # dropna
    df.reset_index(drop=True, inplace=True)

    # cats = list(df.select_dtypes(include=['object']).columns)

    df = df.replace({
        "No phone service": "No",
        "No internet service": "No"
    })

    df = df.replace({
        "No": "0",
        "Yes": "1"
    })

    for col, t in TYPES.items():
        if t is not bool:
            df[col] = df[col].astype(t)
        else:
            df[col] = df[col].astype(str)

    df.to_csv(f"src/data_{kwargs.get('ds')}.csv", index=False)


def load_data(table_name, filepath, **kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    df = pd.read_csv(filepath, dtype=TYPES)
    hook.insert_rows(table_name, rows=df.values.tolist())


def ensure_table_exists(table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    sql = f"select COUNT(1) from pg_catalog.pg_tables WHERE tablename='{table_name}'"
    row = hook.get_records(sql)

    if row[0][0]:
        return "do_nothing"
    return "create_table"


with DAG('load_data', default_args=default_args,
         schedule_interval='@once', catchup=False, tags=['workshop']) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id=POSTGRES_CONN,
        sql="""CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
            customer_id VARCHAR(50) NOT NULL,
            gender VARCHAR(6) NOT NULL,
            senior_citizen Boolean NOT NULL,
            partner Boolean NOT NULL,
            dependents Boolean NOT NULL,
            tenure INTEGER NOT NULL,
            phone_service Boolean NOT NULL,
            multiple_lines Boolean NOT NULL,
            internet_service VARCHAR (25) NOT NULL,
            online_security Boolean NOT NULL,
            online_backup Boolean NOT NULL,
            device_protection Boolean NOT NULL,
            tech_support Boolean NOT NULL,
            streaming_tv Boolean NOT NULL,
            streaming_movies Boolean NOT NULL,
            contract VARCHAR (25) NOT NULL,
            paperless_billing Boolean NOT NULL,
            payment_method VARCHAR (25) NOT NULL,
            monthly_charges FLOAT NOT NULL,
            total_charges FLOAT NOT NULL,
            churn Boolean NOT NULL 
            );
        """,
        params={"table_name": "user_churn_features"}
    )

    branch_table_exists = BranchPythonOperator(
        task_id='ensure_table_exists',
        provide_context=True,
        trigger_rule="all_done",
        python_callable=ensure_table_exists,
        op_kwargs={"table_name": "user_churn_features"}
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
        op_kwargs={"data_path": "src/churn.csv"},
        provide_context=True,
        trigger_rule="all_done",
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        op_kwargs={"filepath": "src/data_{{ ds }}.csv", "table_name": "user_churn_features"},
        provide_context=True,
    )

    start = DummyOperator(
        task_id='start')

    do_nothing = DummyOperator(
        task_id='do_nothing')

    done = DummyOperator(
        trigger_rule="all_done",
        task_id='done')

start >> branch_table_exists >> [create_table, do_nothing] >> clean >> load >> done
