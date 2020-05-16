FROM puckel/docker-airflow:1.10.9

WORKDIR /usr/local/airflow
COPY ./requirements.txt /usr/local/airflow

RUN pip install --user -r requirements.txt
