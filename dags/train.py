from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from datetime import datetime, timedelta
import pandas as pd
from pickle import dump, load

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


def get_data(table_name, filepath, **kwargs):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    df = hook.get_pandas_df(sql=f"SELECT * FROM {table_name}")
    if not len(df):
        raise ValueError("There is no row")
    df.to_csv(filepath, index=False)


def preprocess(filepath, **kwargs):
    df = pd.read_csv(filepath, dtype=TYPES)
    bool_columns = list(df.select_dtypes(include=['bool']).columns)
    df[bool_columns] = df[bool_columns].astype(int)  # bool to binary
    df["extra_charges"] = df["total_charges"] - (df["monthly_charges"] * df["tenure"])  # create new column
    df = df.rename(columns={"gender": "is_male"})  # rename column
    df["is_male"] = df["is_male"].replace({"Male": 1, "Female": 0})
    df = pd.get_dummies(data=df, columns=["internet_service", "payment_method", "contract"])
    return df


def train_data(filepath, **kwargs):
    df = preprocess(filepath)
    y_train = df["churn"]
    df = df.drop("churn", axis=1)
    features = ['internet_service_Fiber optic',
                'internet_service_DSL', 'payment_method_Credit card (automatic)',
                'payment_method_Electronic check', 'payment_method_Mailed check',
                'contract_One year', 'contract_Two year', 'paperless_billing', 'streaming_tv', 'tech_support',
                'multiple_lines', 'dependents', 'senior_citizen', 'is_male', 'tenure', 'monthly_charges',
                'extra_charges']
    kwargs['ti'].xcom_push(key="features", value=features)
    scaler = StandardScaler()
    df[['monthly_charges', 'extra_charges']] = scaler.fit_transform(df[['monthly_charges', 'extra_charges']])
    # save the scaler
    with open(f"scaler_{kwargs.get('ds')}.pickle", 'wb') as f:
        dump(scaler, f)
    log = LogisticRegression(random_state=42)
    # fit and save the model
    log.fit(df[features], y_train)
    with open(f"log_model_{kwargs.get('ds')}.pickle", 'wb') as f:
        dump(log, f)
    return log.coef_


def predictions(filepath, table_name, test=True, **kwargs):
    df = preprocess(filepath)
    y_test = df["churn"]
    features = kwargs["ti"].xcom_pull(key="features", task_ids="train")
    with open(f"scaler_{kwargs.get('ds')}.pickle", 'rb') as f:
        scaler = load(f)
    df[['monthly_charges', 'extra_charges']] = scaler.transform(df[['monthly_charges', 'extra_charges']])
    with open(f"log_model_{kwargs.get('ds')}.pickle", 'rb') as f:
        log = load(f)
    pred = log.predict(df[features])
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN)
    if test:
        report = classification_report(y_test, pred, output_dict=True)
        hook.insert_rows(table_name, rows=[(datetime.now(),
                                            report["accuracy"],
                                            report["macro avg"]["recall"],
                                            report["macro avg"]["precision"],
                                            report["macro avg"]["f1-score"],
                                            )])
    else:
        rows = df[["customer_id", "churn"]]
        rows["churn"] = pred.astype(bool)
        hook.insert_rows(table_name, rows=rows.values.tolist())


def split_train_test(filepath, **kwargs):
    df = pd.read_csv(filepath, dtype=TYPES)
    features = df.drop("churn", axis=1)
    X_train, X_test, y_train, y_test = train_test_split(features, df["churn"], test_size=0.2,
                                                        random_state=42, stratify=df["churn"])

    X_train["churn"] = y_train
    X_test["churn"] = y_test
    X_train.to_csv(f"src/train_{kwargs.get('ds')}.csv", index=False)
    X_train.to_csv(f"src/test_{kwargs.get('ds')}.csv", index=False)


with DAG('train_predict_churn', default_args=default_args,
         schedule_interval='0 0 1 * *', catchup=False, tags=['workshop']) as dag:
    start = DummyOperator(
        task_id='start'
    )

    done = DummyOperator(
        task_id='done'
    )

    save_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
        op_kwargs={"filepath": "src/train_data_{{ ds }}.csv", "table_name": "user_churn_features"},
        provide_context=True,
    )

    split_data = PythonOperator(
        task_id="split_data",
        python_callable=split_train_test,
        op_kwargs={"filepath": "src/train_data_{{ ds }}.csv"},
        provide_context=True,
    )

    train = PythonOperator(
        task_id="train",
        python_callable=train_data,
        op_kwargs={"filepath": "src/train_{{ ds }}.csv"},
        provide_context=True,
    )

    save_metrics = PythonOperator(
        task_id="save_metrics",
        python_callable=predictions,
        op_kwargs={"filepath": "src/test_{{ ds }}.csv", "table_name": "metrics_report", "test": True},
        provide_context=True,
    )

    create_metrics_table = PostgresOperator(
        task_id="create_metrics_table",
        postgres_conn_id=POSTGRES_CONN,
        sql="""CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
            created_at timestamp default NULL,
            accuracy FLOAT NOT NULL,
            recall FLOAT NOT NULL,
            precision FLOAT NOT NULL,
            f1_score FLOAT NOT NULL
            );
        """,
        params={"table_name": "metrics_report"}
    )

    make_predictions = PythonOperator(
        task_id="make_predictions",
        python_callable=predictions,
        op_kwargs={"filepath": "src/train_data_{{ ds }}.csv", "table_name": "predictions_{{ ds_nodash }}",
                   "test": False},
        provide_context=True,
    )

    create_predictions_table = PostgresOperator(
        task_id="create_predictions_table",
        postgres_conn_id=POSTGRES_CONN,
        sql="""CREATE TABLE IF NOT EXISTS predictions_{{ ds_nodash }} (
            customer_id VARCHAR(50) NOT NULL,
            churn Boolean NOT NULL
            );
        """,
        # params={"table_name": "predictions_{{ ds_nodash }}"}
    )

start >> save_data >> split_data >> train >> create_metrics_table >> save_metrics >> done
train >> create_predictions_table >> make_predictions >> done
