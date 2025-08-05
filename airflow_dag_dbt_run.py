
## THIS IS THE CODE STRUCTURE AND MIGHT NOT BE FULLY ACCURATE

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from git import Repo 

# Define the DAG
dag = DAG(
    'dbt_pipeline',
    default_args=default_args,
    description='A simple dbt pipeline',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Function to run dbt command
def run_dbt_task(task_name, git_repo):
    import subprocess
    import os
    
    # Clone the dbt project from Git repository
    project_path = '/tmp/dbt_project'  # Temporary directory for cloning
    if os.path.exists(project_path):
        repo = Repo(project_path)
        origin = repo.remotes.origin
        origin.pull()
    else:
        Repo.clone_from(git_repo, project_path)
    
    dbt_command = f'dbt run --models {task_name}'
    
    # Change directory to dbt project and execute dbt command
    os.chdir(project_path)
    subprocess.run(dbt_command, shell=True)

# Define tasks
staging_tasks = [
    'stg_system_a',
    'stg_system_b',
    'stg_system_c',
    'stg_ind_heirarchy_a',
    'stg_ind_heirarchy_b',
    'stg_ind_heirarchy_c',
    'stg_multi_source_heirarchy'
]

transformation_tasks = [
    'systems_combined',
    'heirarchy_combined'
]

mart_task = [
    'master_report'
]

# Git repository URL for dbt project
git_repo = 'https://github.com/bhowmikk21/analytics-engineering-case-study.git'

# Create PythonOperator tasks for staging tasks
for task in staging_tasks:
    run_staging = PythonOperator(
        task_id=f'run_{task}',
        python_callable=run_dbt_task,
        op_args=[task, git_repo],
        dag=dag,
    )

# Create PythonOperator tasks for transformation tasks
for task in transformation_tasks:
    run_transformation = PythonOperator(
        task_id=f'run_{task}',
        python_callable=run_dbt_task,
        op_args=[task, git_repo],
        dag=dag,
    )

# Create PythonOperator tasks for mart tasks
for task in mart_tasks:
    run_transformation = PythonOperator(
        task_id=f'run_{task}',
        python_callable=run_dbt_task,
        op_args=[task, git_repo],
        dag=dag,
    )

# Define dependencies
for staging_task in staging_tasks:
    for transformation_task in transformation_tasks:
        globals()[staging_task] >> globals()[f'run_{transformation_task}'] >> globals()[f'run_{mart_task}']
