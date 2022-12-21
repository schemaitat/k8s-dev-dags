import datetime
import pendulum
from airflow import models
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
from kubernetes.client import models as k8s_models

# A Secret is an object that contains a small amount of sensitive data such as
# a password, a token, or a key. Such information might otherwise be put in a
# Pod specification or in an image; putting it in a Secret object allows for
# more control over how it is used, and reduces the risk of accidental
# exposure.



default_args = {
    'owner': 'airflow',
}

with DAG(
        dag_id="k8s-pod",
        default_args=default_args,
        schedule_interval=None,
        tags=["ex"],
        start_date=pendulum.datetime(2015, 12, 1)
    ) as dag:

    start = DummyOperator(task_id='run_this_first', dag=dag)

    passing = KubernetesPodOperator(namespace='airflow',
                            image="python:3.6",
                            cmds=["python","-c"],
                            arguments=["print('hello world')"],
                            labels={"foo": "bar"},
                            name="passing-test",
                            task_id="passing-task",
                            get_logs=True,
                            )

    failing = KubernetesPodOperator(namespace='airflow',
                            image="ubuntu:22.04",
                            cmds=["python","-c"],
                            arguments=["print('hello world from ubuntu')"],
                            labels={"foo": "bar"},
                            name="yey",
                            task_id="failing-task",
                            get_logs=True,
                            )
                        
    start >> passing >> failing