from datetime import datetime
from tracemalloc import start
from unicodedata import name
from unittest import result
#Ficheros
import csv
import re
import random

from sqlalchemy import null

names=['Izmael','Juan','Martin','Ramiro','Leonel','Dorian']
class alumno:
    def __init__(self,name,key,prom):
        self.name = name
        self.key = key
        self.prom = prom
    def getvalues(self):
        return (self.name+' '+str(self.key)+' '+str(self.prom)+'*')

def extract(path):
    with open(path,"r") as f:
        text = f.readline().strip()
    text = re.sub(",","",text)
    print("")
    print("String is: "+text)
    row=''
    for i in text:
        if i=='*':
            print(row)
            row=''
            continue
        row+=i
    data = text.split('*')
    print("")
    return text

def transform(data):
    from datetime import datetime
    now = datetime.now()
    random.seed(now.microsecond)
    alumn = alumno(random.choice(names),
        random.randint(1,99999999),
        random.randint(1,99999999))
    tdata = str(data)+str(alumn.getvalues())
    return tdata

def load(data,path):
    with open(path,"w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    return

# def build_flow():
#         data = extract("/home/sensei/Escritorio/airflow/airflow/dags/values.csv")
#         tdata = transform(data)
#         result = load(tdata,"/home/sensei/Escritorio/airflow/airflow/dags/values.csv")

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

default_args = {
    'owner':'sensei',
    'depends_on_past':False,
    'email':['anamarianamalotapalara@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':True,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}
data=null
tdata=null

def scrape():
    data = extract("/home/sensei/Escritorio/airflow/airflow/dags/values.csv")

def process():
    tdata = transform(data)

def save():
    result = load(tdata,"/home/sensei/Escritorio/airflow/airflow/dags/values.csv")

def comenzar():  
    print("Comenzando tareas")

def terminado():  
    print("Tareas terminadas")

with DAG(
    'My_Example',
    default_args=default_args,
    description='Mi primer dag',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['My_Example'],

) as dag:
    comenzar_task = PythonOperator(task_id="moenzar",python_callable=comenzar)
    scrape_task = PythonOperator(task_id="scrape",python_callable=scrape)
    process_task = PythonOperator(task_id="process",python_callable=process)
    save_task = PythonOperator(task_id="save",python_callable=save)
    terminado_task = PythonOperator(task_id="terminado",python_callable=terminado)
    
    comenzar_task >> scrape_task >> process_task >>save_task >> terminado_task



