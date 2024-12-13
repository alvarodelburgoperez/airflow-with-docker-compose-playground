from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd

def download_file_1(**context):
    """Descarga del primer archivo"""
    # Simula descarga - reemplazar con lógica real de descarga
    import requests
    url = "https://github.com/covid19india/CovidCrowd/blob/master/data/raw_data.csv"
    response = requests.get(url)
    with open("/tmp/archivo1.csv", "wb") as f:
        f.write(response.content)
    context['ti'].xcom_push(key='file1_path', value='/tmp/archivo1.csv')

def download_file_2(**context):
    """Descarga del segundo archivo"""
    # Simula descarga - reemplazar con lógica real de descarga
    import requests
    url = "https://github.com/covid19india/CovidCrowd/blob/master/data/travel_history.csv"
    response = requests.get(url)
    with open("/tmp/archivo2.csv", "wb") as f:
        f.write(response.content)
    context['ti'].xcom_push(key='file2_path', value='/tmp/archivo2.csv')

def merge_files(**context):
    """Merge de archivos descargados"""
    ti = context['ti']
    file1_path = ti.xcom_pull(task_ids='download_file_1', key='file1_path')
    file2_path = ti.xcom_pull(task_ids='download_file_2', key='file2_path')
    
    # Cargar archivos
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    
    # Ejemplo de merge - ajustar según necesidad
    merged_df = pd.merge(df1, df2, on='columna_comun', how='inner')
    
    merged_path = '/tmp/merged_file.csv'
    merged_df.to_csv(merged_path, index=False)
    
    ti.xcom_push(key='merged_file_path', value=merged_path)

def generate_report(**context):
    """Genera informe de salida"""
    ti = context['ti']
    merged_file_path = ti.xcom_pull(task_ids='merge_files', key='merged_file_path')
    
    df = pd.read_csv(merged_file_path)
    
    # Generar informe de resumen
    report_content = f"""
    Informe de Análisis de Datos:
    
    Total de Registros: {len(df)}
    Columnas: {', '.join(df.columns)}
    
    Resumen Estadístico:
    {df.describe().to_string()}
    """
    
    with open('/tmp/informe_salida.txt', 'w') as f:
        f.write(report_content)

with DAG(
    'descarga_merge_informe',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    
    download_task_1 = PythonOperator(
        task_id='download_file_1',
        python_callable=download_file_1,
        provide_context=True
    )
    
    download_task_2 = PythonOperator(
        task_id='download_file_2',
        python_callable=download_file_2,
        provide_context=True
    )
    
    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        provide_context=True
    )
    
    report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True
    )
    
    # Definir dependencias
    [download_task_1, download_task_2] >> merge_task >> report_task