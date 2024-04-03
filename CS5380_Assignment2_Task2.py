#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# Imports
import os
from datetime import datetime
import json
import requests
import zipfile
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.dates import days_ago
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

# DAG Configuration
dag = DAG(
    'Analytics_dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval=None,  # Run manually
    catchup=False
)

# File Paths
path_zip = "/opt/airflow/downloads/ncei_data.zip"
path_unzip = "/opt/airflow/downloads/ncei_data_2011"

# Task 1: File Sensor to check the availability of the archive
check_archive_available = FileSensor(
    task_id="archive_available",
    filepath=path_zip,
    timeout=5,
    poke_interval=1,
    dag=dag,
)

# Task 2: Unzip the archive
unzip_task = BashOperator(
    task_id='unzip_archive',
    bash_command=f'if unzip -tq {path_zip}; then unzip -o {path_zip} -d {path_unzip}; else exit 1; fi',
    dag=dag,
    trigger_rule='all_success',
)

# Functions for Data Processing
def csv_to_df(csv_file):
    # Function to convert CSV file to DataFrame
    with open(csv_file, 'r') as file:
        df = pd.read_csv(file)

    tuple_data = []
    col_anal = ['HourlyDryBulbTemperature', 'HourlyWindSpeed']
    all_analyse = ['LATITUDE', 'LONGITUDE'] + col_anal
    df[all_analyse] = df[all_analyse].apply(pd.to_numeric, errors='coerce')
    df.dropna(how='any', inplace=True)

    for index, row in df.iterrows():
        lat = row['LATITUDE']
        long = row['LONGITUDE']
        date = pd.to_datetime(row['DATE']).month
        hourly_temp = row['HourlyDryBulbTemperature']
        hourly_speed = row['HourlyWindSpeed']
        tuple_data.append((date, float(lat), float(long), float(hourly_temp), float(hourly_speed)))

    return tuple_data

def csv_to_df_filter(path_folder):
    # Function to process CSV files and write to text file
    with beam.Pipeline(runner='DirectRunner') as pipeline:
        csv_files = [os.path.join(path_folder, file) for file in os.listdir(path_folder) if file.endswith('.csv')]
        result = (
            pipeline 
            | beam.Create(csv_files)
            | beam.Map(csv_to_df)
            | beam.Map(monthly_average)
            | beam.io.WriteToText('/opt/airflow/tuple_data.txt')
        )

text_address = '/opt/airflow/tuple_data.txt-00000-of-00001'

def monthly_average(data_list):
    # Function to compute monthly averages
    data_df = pd.DataFrame(data_list, columns=['Month', 'Latitude', 'Longitude', 'Temperature', 'WindSpeed'])
    data_df.dropna(inplace=True)
    final_result = []
    result_df = data_df.groupby(['Month', 'Latitude', 'Longitude']).agg({'Temperature': 'mean', 'WindSpeed': 'mean'}).reset_index()
    for _, row in result_df.iterrows():
        lat = row['Latitude']
        long = row['Longitude']
        month = row['Month']
        temp = row['Temperature']
        speed = row['WindSpeed']
        final_result.append([lat, long, month, temp, speed])
    return json.dumps(final_result)

# Data Visualization
def heat_map(link):
    # Function to generate heat maps for temperature and wind speed
    avg_list = []
    with open(link, 'r') as file:
        for line in file:
            local_data = json.loads(line)
            avg_list.extend(local_data)  # Extend instead of append to flatten the nested lists
    
    latitude = [lat for lat, lon, month, temp, wspeed in avg_list]
    longitude = [lon for lat, lon, month, temp, wspeed in avg_list]
    month = [month for lat, lon, month, temp, wspeed in avg_list]
    temperature = [temp for lat, lon, month, temp, wspeed in avg_list]
    wspeed = [wspeed for lat, lon, month, temp, wspeed in avg_list]
    data_df = pd.DataFrame({
        'Latitude': latitude,
        'Longitude': longitude,
        'Month': month,
        'AvgTemperature': temperature,
        'AvgWSpeed': wspeed
    })
    
    gdf = gpd.GeoDataFrame(data_df, geometry=gpd.points_from_xy(data_df.Longitude, data_df.Latitude))
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    # Temperature Visualization
    ax_temp = world.plot(color='green', edgecolor='black', figsize=(12, 12))
    gdf.plot(ax=ax_temp, column='AvgTemperature', cmap='viridis', markersize=50, alpha=0.75, legend=True)
    ax_temp.set_title('Temperature Visualization')
    ax_temp.set_xlabel('Longitude')
    ax_temp.set_ylabel('Latitude')
    plt.grid(False)
    plt.axis('equal')
    plt.savefig('/opt/airflow/temperature_visualizatio_plot.png')
    plt.show()
    
    # Wind Speed Visualization
    ax_speed = world.plot(color='green', edgecolor='black', figsize=(12, 12))
    gdf.plot(ax=ax_speed, column='AvgWSpeed', cmap='coolwarm', markersize=50, alpha=0.75, legend=True)
    ax_speed.set_title('Speed Visualization')
    ax_speed.set_xlabel('Longitude')
    ax_speed.set_ylabel('Latitude')
    plt.grid(False)
    plt.axis('equal')
    plt.savefig('/opt/airflow/speed_visualization_plot.png')
    plt.show()

# Task 3: Generate Tuple Averages
tuple_avg_generation = PythonOperator(
    task_id="generate_tuple_averages",
    python_callable=csv_to_df_filter,
    op_kwargs={"path_folder": path_unzip},
    dag=dag,
)

# Task 4: Data Processing
# Task 5: Generate Heat Maps
heat_map_generation = PythonOperator(
    task_id="generate_heat_maps",
    python_callable=heat_map,
    op_kwargs={"link": text_address},
    dag=dag,
)

# Task 6: Delete CSV Folder
delete_csv_folder = BashOperator(
    task_id="delete_csv_folder",
    bash_command=f'rm -r {path_unzip}',
    dag=dag,
    trigger_rule='all_success',
)

# Task Dependency Flow
check_archive_available >> unzip_task >> tuple_avg_generation >> heat_map_generation >> delete_csv_folder

