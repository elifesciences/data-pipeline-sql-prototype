from datetime import timedelta
import os
from typing import Tuple

from airflow import DAG
from airflow.operators.sensors import S3KeySensor
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import boto3


default_args = {
    'owner': 'elife',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['foo@bar.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

SUCCESS = True
FAILURE = False
TEMP_DIR = 'temp-dir'
OUTPUT_DIR = 'csv_output'
ARCHIVE_BUCKET = os.environ.get('ARCHIVE_BUCKET')
INCOMING_BUCKET = os.environ.get('INCOMING_BUCKET')


def _set_upstreams(upstream_map: Tuple[Tuple[str]]) -> None:
    """takes a map of operators and their target upstream
    operators and sets the upstream operator value.

    :param upstream_map: tuple
    :return:
    """
    for operator, upstream in upstream_map:
        operator.set_upstream(upstream)


def _get_file_keys(*args, **kwargs) -> bool:
    """Retrieves a list of object keys from the
    `INCOMING_BUCKET` and stores them in the `xcom` layer.

    :param args:
    :param kwargs:
    :return: bool
    """
    client = boto3.client('s3')
    response = client.list_objects(Bucket=INCOMING_BUCKET)
    file_keys = [f['Key'] for f in response['Contents']]

    if len(file_keys):
        kwargs['ti'].xcom_push('file_keys', file_keys)
        return SUCCESS

    return FAILURE


def _download_zip_files(*args, **kwargs):
    """Download `n` files from `INCOMING_BUCKET`, and stores the
    file paths as `zip_files` in the `xcom` layer.

    Expects `file_keys` to be present on the `xcom` layer to provide
    the files to download.

    :param args:
    :param kwargs:
    :return: bool
    """
    client = boto3.client('s3')
    file_keys = kwargs['ti'].xcom_pull(task_ids=None, key='file_keys')

    if len(file_keys):
        os.makedirs(TEMP_DIR, exist_ok=True)

        for zip_file in file_keys:
            client.download_file(INCOMING_BUCKET, zip_file, os.path.join(TEMP_DIR, zip_file))

        kwargs['ti'].xcom_push('zip_files', os.listdir(TEMP_DIR))

        return SUCCESS

    return FAILURE


def _convert_zips_to_csvs(*args, **kwargs) -> bool:
    """Runs target `zip_files` through the `csv_generator`.

    :param args:
    :param kwargs:
    :return: bool
    """

    # temp hack to avoid import errors at build time,
    # once `csv_generator` is a package on `pypi` then this can move
    # back up to the top level
    from csv_generator.process_xml_zip import process_zip_or_extracted_dir

    zip_files = kwargs['ti'].xcom_pull(task_ids=None, key='zip_files')

    if len(zip_files):
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        for zip_file in zip_files:
            process_zip_or_extracted_dir(os.path.join(TEMP_DIR, zip_file), OUTPUT_DIR, batch=True)

        return SUCCESS

    return FAILURE


def _move_zip_files_to_archive(*args, **kwargs):
    """Copies files from `INCOMING_BUCKET` to the
    `ARCHIVE_BUCKET` then deletes the original files.

    Expects `file_keys` to be present on the `xcom` layer to provide
    the files to copy.

    :param args:
    :param kwargs:
    :return:
    """
    client = boto3.client('s3')
    file_keys = kwargs['ti'].xcom_pull(task_ids=None, key='file_keys')

    if len(file_keys):
        for zip_file in file_keys:
            client.copy_object(
                Key=zip_file,
                CopySource='{0}/{1}'.format(INCOMING_BUCKET, zip_file),
                Bucket=ARCHIVE_BUCKET
            )
            client.delete_object(
                Key=zip_file,
                Bucket=INCOMING_BUCKET
            )

        return SUCCESS

    return FAILURE


def _run_db_manager(*args, **kwargs):
    # TODO implement
    return SUCCESS


dag = DAG('data_pipeline', default_args=default_args, schedule_interval='@daily')

bucket_watcher = S3KeySensor(
    task_id='bucket_watcher',
    poke_interval=5,
    timeout=300,
    soft_fail=True,
    wildcard_match=True,
    bucket_key='*',
    bucket_name=INCOMING_BUCKET,
    dag=dag
)

get_file_keys = PythonOperator(
    task_id='get_file_keys',
    provide_context=True,
    python_callable=_get_file_keys,
    dag=dag
)

download_zip_files = PythonOperator(
    task_id='download_zip_files',
    provide_context=True,
    python_callable=_download_zip_files,
    dag=dag
)

move_zip_files_to_archive = PythonOperator(
    task_id='move_zip_files_to_archive',
    provide_context=True,
    python_callable=_move_zip_files_to_archive,
    dag=dag
)

convert_zips_to_csvs = PythonOperator(
    task_id='convert_zips_to_csvs',
    provide_context=True,
    python_callable=_convert_zips_to_csvs,
    dag=dag
)

run_db_manager = PythonOperator(
    task_id='run_db_manager',
    provide_context=True,
    python_callable=_run_db_manager,
    dag=dag
)

upstream_map = (
    (get_file_keys, bucket_watcher),
    (download_zip_files, get_file_keys),
    (convert_zips_to_csvs, download_zip_files),
    (move_zip_files_to_archive, download_zip_files),
    (run_db_manager, convert_zips_to_csvs),
)

_set_upstreams(upstream_map)
