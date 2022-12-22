"""
This is an example dag for using a Kubernetes Executor Configuration.
"""
import logging
import datetime
import pendulum
import os

from airflow import DAG
from airflow.example_dags.libs.helper import print_stuff
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
}

log = logging.getLogger(__name__)

try:
    from kubernetes.client import models as k8s

    with DAG(
        dag_id="single_job",
        default_args=default_args,
        schedule=None,
        tags=["ex"],
        start_date=pendulum.datetime(2015, 12, 1)
    ) as dag:

        # You can use annotations on your kubernetes pods!
        start_task = PythonOperator (
            task_id="start_task",
            python_callable=print_stuff,
            executor_config={
                "pod_override": k8s.V1Pod(
                    metadata=k8s.V1ObjectMeta(annotations={"test": "annotation"})
                )
            },
            queue="kubernetes",
        )

        # [START task_with_sidecar]
        sidecar_task = PythonOperator(
            task_id="task_with_sidecar",
            python_callable=print_stuff,
            queue="kubernetes",
            executor_config={
                "pod_override": k8s.V1Pod(
                    spec=k8s.V1PodSpec(
                        containers=[
                            k8s.V1Container(
                                name="base",
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        mount_path="/shared/", name="shared-empty-dir"
                                    )
                                ],
                            ),
                            k8s.V1Container(
                                name="sidecar",
                                image="ubuntu",
                                args=['echo "retrieved from mount" > /shared/test.txt'],
                                command=["bash", "-cx"],
                                volume_mounts=[
                                    k8s.V1VolumeMount(
                                        mount_path="/shared/", name="shared-empty-dir"
                                    )
                                ],
                            ),
                        ],
                        volumes=[
                            k8s.V1Volume(
                                name="shared-empty-dir",
                                empty_dir=k8s.V1EmptyDirVolumeSource(),
                            ),
                        ],
                    )
                ),
            },
        )
        # [END task_with_sidecar]

        other_ns_task = PythonOperator(
            task_id="other_namespace_task",
            python_callable=print_stuff,
            queue = "kubernetes",
            executor_config={
                "KubernetesExecutor": {
                    "namespace": "airflow-compute",
                    "labels": {"release": "stable"},
                }
            },
        )

        start_task >> [sidecar_task, other_ns_task]

except ImportError as e:
    log.warning(
        "Could not import DAGs in example_kubernetes_executor_config.py: %s", str(e)
    )
    log.warning(
        "Install kubernetes dependencies with: pip install apache-airflow['cncf.kubernetes']"
    )
