from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {"owner": "you", "retries": 0}

with DAG(
    dag_id="crazy_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["fanout", "layers", "crazy"],
) as dag:

    layers = []  # keep track of tasks layer by layer
    total_layers = 6  # number of iterations

    # build layers
    for i in range(total_layers):
        layer_size = i + 1  # grows: 1, 2, 3, ...
        layer_tasks = []

        for j in range(layer_size):
            t = BashOperator(
                task_id=f"layer_{i+1}_task_{j+1}",
                bash_command=f'echo "This is task {j+1} of layer {i+1}"'
            )
            layer_tasks.append(t)

        layers.append(layer_tasks)

        # connect this layer to the previous one (if not the first layer)
        if i > 0:
            for prev_task in layers[i-1]:
                prev_task >> layer_tasks
