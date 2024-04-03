#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import re
import os
import shutil
import random

## Task 1 (DataFetch Pipeline)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
with DAG(
    dag_id='ncei_data_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Run manually
) as dag:

    # Define year as a placeholder
    target_year = '2011'

    # Define a filename to store the HTML content
    html_filename = f'/tmp/ncei_data_{target_year}.html'

    # Define the number of data files to select
    num_files_to_fetch = 3

    # Define base URL and download directory
    base_url = f"https://www.ncei.noaa.gov/data/local-climatological-data/access/{target_year}/"
    download_dir = '/tmp/ncei_data'

    # Task to fetch the HTML page and save it to a file
    fetch_page = BashOperator(
        task_id='fetch_page',
        bash_command=f"curl -s -o {html_filename} {base_url}",
    )

    # Function to extract CSV filenames from the provided HTML file path
    def extract_csv_names(html_path, ti):
        with open(html_path, 'r') as file:
            html_content = file.read()

        # Extract text within quotation marks for anchor tags with href ending in '.csv'
        csv_filenames = re.findall(r'<a href="([^"]+\.csv)">', html_content)
       
        ti.xcom_push(key='selected_files', value=csv_filenames)

    # Task to extract CSV filenames from the saved HTML
    csv_extraction = PythonOperator(
        task_id='extract_csv_names',
        python_callable=extract_csv_names,
        op_args=[html_filename],
        provide_context=True,
    )

    # Function to select random CSV files
    def select_random_files(num_files, ti):
        csv_filenames = ti.xcom_pull(task_ids='extract_csv_names', key='selected_files')
        if num_files > len(csv_filenames):
            raise ValueError(f"Cannot select {num_files} files from {len(csv_filenames)} available files")

        ti.xcom_push(key='selected_files', value=random.sample(csv_filenames, num_files)) 

    # Task to select random CSV files
    file_selection = PythonOperator(
        task_id='select_files',
        python_callable=select_random_files,
        op_args=[num_files_to_fetch],
        provide_context=True,
    )

    # Function to fetch individual data files from the base URL
    def fetch_data_files(base_url, download_dir, ti):
        selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
        for filename in selected_files:
            download_path = os.path.join(download_dir, filename)
            os.makedirs(download_dir, exist_ok=True)  # Create directory if it doesn't exist
            os.system(f"curl -s -o {download_path} {base_url}{filename}")

    # Task to fetch individual data files
    data_fetch = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data_files,
        op_args=[base_url, download_dir],
        provide_context=True,
    )
    
        # Function to zip selected data files
    def zip_files(download_dir, ti):
        selected_files = ti.xcom_pull(task_ids='select_files', key='selected_files')
        zip_filename = f'ncei_data_{target_year}_selected_files.zip'
        zip_filepath = os.path.join(download_dir, zip_filename)
        
        with shutil.ZipFile(zip_filepath, 'w') as zipf:
            for filename in selected_files:
                file_path = os.path.join(download_dir, filename)
                zipf.write(file_path, filename)

        ti.xcom_push(key='zip_filepath', value=zip_filepath)

    # Task to zip selected data files
    zip_files_task = PythonOperator(
        task_id='zip_files',
        python_callable=zip_files,
        op_args=[download_dir],
        provide_context=True,
    )

    # Function to move the zip file to a required location
    def move_zip_file(ti):
        zip_filepath = ti.xcom_pull(task_ids='zip_files', key='zip_filepath')
        target_location = '/opt/airflow/downloads'
        target_filepath = os.path.join(target_location, os.path.basename(zip_filepath))
        shutil.move(zip_filepath, target_filepath)

    # Task to move the zip file to a required location
    move_zip_file_task = PythonOperator(
        task_id='move_zip_file',
        python_callable=move_zip_file,
        provide_context=True,
    )

    # Set task dependencies
    fetch_page >> csv_extraction >> file_selection >> data_fetch >> zip_files_task >> move_zip_file_task

