#HERE DECLARE LIBS AND OPERATORS
import airflow
from airflow import DAG
from airflow.operators.sensors import TimeDeltaSensor
from airflow.models import Variable
from datetime import datetime, timedelta, date, time
from airflow.contrib.kubernetes.pod import Resources
from airflow.operators.python_operator import PythonOperator
from airflow.operators.[Import-Operator] import BteqOperator
import logging
import os
import sys


#HERE DECLARE DAG DEFINITION
start = datetime.today() - timedelta(days=1)
default_args = {
    'owner': 'Udacity-DAG',
    'start_date': start,
    'email': 'carrascocamilo@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

dag = DAG('00_DAG_UDACITY_BTEQ_OPERATOR', default_args=default_args, schedule_interval="0 12 * * 1-5",tags=['GIT','BTEQ-OPERATOR']) 

#HERE DECLARE BTEQ OPERATOR ARGS BTEQ NAME, CONECTION & QUERIES FOLDER 

def define_bteq_op(bteq_file, bteq_name, bteq_params={}):
    """ Define operador BTEQ para ejecutar en contenedor"""
    return BteqOperator(
        bteq=os.path.join(queries_folder, os.path.basename(bteq_file)),
        task_id=bteq_name,
        conn_id='Teradata-Conection
        pool='teradata-prod',
        depends_on_past=True,
        provide_context=True,
        xcom_push=True,
        xcom_all=True,
        dag=dag)

queries_folder = 'BTEQs' #STORAGE BTEQs SCRIPTS IN THIS FOLDER
ext_file = '.sql' #INCLUDE EXTENSION SQL
bteq_files = [f for f in glob.glob('%s/*%s' % (os.path.join(__location__, queries_folder), ext_file)) if
              f.endswith(ext_file)]

for bteq_file in sorted(bteq_files):
    try:
        query_task_id = os.path.basename(bteq_file).split(ext_file)[0]
    except:
        query_task_id = str(uuid.uuid4())
        logging.info("Archivo con el nombre malo : " + bteq_file)
        pass
    dag_tasks.append(define_bteq_op(bteq_file, query_task_id))

# AUTOGENERATE DEPENDENCES

for seq_pair in zip(dag_tasks, dag_tasks[1:]):
    seq_pair[0] >> seq_pair[1]


