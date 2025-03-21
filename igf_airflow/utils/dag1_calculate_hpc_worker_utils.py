import logging
import base64
import pandas as pd
import numpy as np
from io import StringIO
import requests, json, redis
from typing import List, Tuple
from yaml import load, SafeLoader
from igf_data.utils.dbutils import read_json_data
from requests.auth import HTTPBasicAuth
from igf_data.utils.fileutils import check_file_path
from datetime import timedelta
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.decorators import task
from igf_airflow.utils.generic_airflow_utils import send_airflow_failed_logs_to_channels

log = logging.getLogger(__name__)

MS_TEAMS_CONF = \
  Variable.get(
    'ms_teams_conf', default_var=None)
CELERY_FLOWER_CONFIG_FILE = \
  Variable.get(
    'celery_flower_config', default_var=None)
HPC_QUEUE_LIST = \
  Variable.get(
    'hpc_queue_list_yaml', default_var=None)
REDIS_CONF_FILE = \
  Variable.get(
      'redis_conn_file', default_var=None)

def get_celery_flower_workers(
      celery_flower_config_file: str,
      flower_url_key: str = 'flower_url',
      flower_user_key: str = 'flower_user',
      flower_pass_key: str = 'flower_pass') -> List[str]:
  """
  A function for fetching celery flower workers

  :param celery_flower_config_file: A json file containing celery flower config
  :param flower_url_key: A key for celery flower url in the config file
  :param flower_user_key: A key for celery flower user in the config file
  :param flower_pass_key: A key for celery flower pass in the config file
  :returns: A list of dictionaries with worker_id, active_jobs and queue_lists
  """
  try:
    flower_config = read_json_data(celery_flower_config_file)
    flower_config = flower_config[0]
    if flower_user_key not in flower_config or \
       flower_url_key not in flower_config or \
       flower_pass_key not in flower_config:
      raise KeyError(
        'Missing flower config in the config file')
    celery_url = \
      f'{flower_config.get(flower_url_key)}/api/workers?refresh=True'
    res = \
      requests.get(
        celery_url,
        auth=HTTPBasicAuth(
          flower_config.get(flower_user_key),
          flower_config.get(flower_pass_key)))
    if res.status_code != 200:
      raise ValueError('Failed to get celery flower workers.')
    data = res.content.decode()
    data = json.loads(data)
    worker_list = list()
    for worker_id, val in data.items():
      worker_list.append({
        'worker_id': worker_id,
        'active_jobs': len(val.get('active')),
        'queue_lists': [i.get('name') for i in val.get('active_queues')]})
    return worker_list

  except Exception as e:
    raise ValueError(f'Failed to get celery flower workers, error: {e}')


def fetch_queue_list_from_redis_server(
      redis_conf_file: str) -> List[dict]:
  """
  A function for fetching pending job counts from redis db

  :param redis_conf_file: A json file containing redis_db as key and redis db connection URL as value
  :returns: A list of dictionaries with queue name as key and pending job counts as the value
  """
  try:
    queue_list = list()
    with open(redis_conf_file,'r') as jp:
      json_data = json.load(jp)
    if 'redis_db' not in json_data:
      raise ValueError('redis_db key not present in the conf file')
    url = json_data.get('redis_db')
    r = redis.from_url(url)
    for i in r.keys():
      if not isinstance(i, str):
        queue = i.decode()
      else:
        queue = i
      if not queue.startswith('_') and \
         not queue.startswith('unacked'):
        q_len = r.llen(queue)
        queue_list.append({queue: q_len})
    return queue_list
  except Exception as e:
    raise ValueError(
      f'Failed to fetch from redis server, error: {e}')


def combine_celery_and_hpc_worker_info(
      hpc_worker_info: str,
      celery_flower_worker_info: List[dict],
      redis_queue_info: List[dict],
      generic_queue_name: str = 'generic',
      max_items_in_queue: int = 3,
      total_hpc_jobs: int = 30) -> \
        Tuple[List[dict], List[dict]]:
    """
    A function for combining celery and hpc worker info for scaling operations

    :param hpc_worker_info: A string containing hpc worker info
    :param celery_flower_worker_info: A list of dictionaries with worker_id, active_jobs and queue_lists
    :param redis_queue_info: A list of dictionaries with queue name as key and pending job counts as the value
    :param generic_queue_name: A string for generic queue name
    :param max_items_in_queue: Maximum number of items allowed in the queue
    :param total_hpc_jobs: Total number of hpc jobs allowed
    :returns: A tuple of scaled worker data and raw worker data
    """
    try:
      ## process hpc output
      hpc_jobs_df = \
        pd.DataFrame([{}],
          columns = [
            'job_id',
            'queue_name',
            'job_status']).dropna()
      if isinstance(hpc_worker_info, str):
        hpc_worker_info = \
          base64.b64decode(
            hpc_worker_info.encode('ascii')).\
          decode('utf-8').\
          strip()
      elif isinstance(hpc_worker_info, bytes):
        hpc_worker_info = hpc_worker_info.decode('utf-8')
      if hpc_worker_info != "" and \
         hpc_worker_info is not None:
        hpc_jobs_df = pd.read_csv(StringIO(hpc_worker_info), sep=",", header=None)
        if len(hpc_jobs_df.columns) != 3:
          raise ValueError(
            f'Expected 3 columns in the hpc worker info, got {len(hpc_jobs_df.columns)}')
        hpc_jobs_df.columns = [
          'job_id',
          'queue_name',
          'job_status']
      ## process celery flower output
      celery_worker_df = pd.DataFrame(celery_flower_worker_info)
      required_celery_columns = [
        'worker_id',
        'active_jobs',
        'queue_lists']
      for c in required_celery_columns:
        if c not in celery_worker_df.columns:
          raise KeyError(
            f'Columns {required_celery_columns} not found in the celery worker info')
      celery_worker_df["queue_lists"] = \
        celery_worker_df["queue_lists"].map(lambda x: ','.join(x))
      filt_worker_df = \
        celery_worker_df[celery_worker_df['queue_lists'] != generic_queue_name].copy()
      ## using last queue name as the main queue name
      filt_worker_df["queue_name"] = \
        filt_worker_df["queue_lists"].\
          map(lambda x: x.split(",")[-1])
      filt_worker_df["worker_info"] = \
        filt_worker_df["worker_id"].\
          map(lambda x: x.replace("celery@", "").split("-"))
      if len(filt_worker_df.index) > 0:
        filt_worker_df[["job_id", "queue_name_tag"]] = \
          pd.DataFrame(
            filt_worker_df['worker_info'].to_list(),
            index=filt_worker_df.index)
        filt_worker_df = \
          filt_worker_df[[
            "job_id",
            "worker_id",
            "queue_name",
            "active_jobs"]]
      ## merge data
      filt_merged_data = \
        pd.DataFrame(
          [{}],
          columns=[
            'job_id',
            'worker_id',
            'job_status',
            "active_jobs",
            'queue_name',
            'hpc_r',
            'hpc_q',
            'task_r',
            'task_i']).dropna()
      agg_df = \
        pd.DataFrame(
          [{}],
          columns=[
            'queue_name',
            'hpc_r',
            'hpc_q',
            'task_r',
            'task_i']).dropna()
      agg_df.set_index(
        'queue_name', inplace=True)
      if len(hpc_jobs_df.index) > 0 and len(filt_worker_df.index) > 0:
        merged_data = \
          hpc_jobs_df.set_index(['job_id','queue_name']).\
            join(
              filt_worker_df.set_index(['job_id','queue_name']),
              how='outer').\
            reset_index()
        ## add missing values
        merged_data = \
          merged_data.fillna({
            "worker_id":"U",
            "job_status": "U",
            "active_jobs": 0})
        # merged_data['job_status'] = \
        #   merged_data['job_status'].fillna('U')
        # merged_data['active_jobs'] = \
        #   merged_data['active_jobs'].fillna(0)
        merged_data = \
          merged_data.astype({'active_jobs':int})
        merged_data = \
          merged_data[merged_data['job_status'] !='U']
        ## create a new column to store the job count per worker status
        ## for hpc active workers
        merged_data['hpc_r'] = \
          np.select([
            merged_data["job_status"] == "R",],[
            1,],
            default=0)
        ## for hpc queued workers
        merged_data['hpc_q'] = \
          np.select([
            merged_data["job_status"] == "Q",],[
            1,],
            default=0)
        ## for celery active workers
        merged_data['task_r'] = \
          np.select([
            (merged_data["job_status"] == "R")&(merged_data["active_jobs"] == 1),],[
            1,],
            default=0)
        ## for celery inactive workers
        merged_data['task_i'] = \
          np.select([
            (merged_data["job_status"] == "R") & (merged_data["active_jobs"] == 0),],[
            1,],
            default=0)
        ## keep subset of columns
        filt_merged_data = \
          merged_data[[
            'job_id',
            'worker_id',
            'queue_name',
            'hpc_r',
            'hpc_q',
            'task_r',
            'task_i']].copy()
        ## count the number of jobs per queue
        ## setting queue_name as the index
        agg_df = \
          filt_merged_data.\
            groupby('queue_name').agg("sum")
      ## fix for empty hpc worker and queued jobs
      ## add the queue info from redis
      if len(redis_queue_info) > 0:
        queue_data = [
          {'queue_name': queue_name, 'queued': queued_jobs}
            for entry in redis_queue_info
              for queue_name, queued_jobs in entry.items()]
        queue_df = pd.DataFrame(queue_data)
        ## filter the generic queue
        queue_df = \
          queue_df[queue_df["queue_name"] != generic_queue_name]
        final_agg_df = \
          agg_df.join(
            queue_df.set_index("queue_name"),
            how="outer")
        ## fix for empty hpc worker
        final_agg_df = \
          final_agg_df.fillna(0)
      else:
        final_agg_df = agg_df.copy()
        final_agg_df['queued'] = 0
      ## format the final dataframe
      final_agg_df = \
        final_agg_df.\
          fillna(0).\
          astype({
            'hpc_r': int,
            'hpc_q': int,
            'task_r': int,
            'task_i': int,
            'queued': int}).\
          reset_index()
      ## scale out and scale in operations
      scaled_df = \
        calculate_scale_out_scale_in_ops(
          input_df=final_agg_df,
          max_items_in_queue=max_items_in_queue,
          total_hpc_jobs=total_hpc_jobs)
      return scaled_df.to_dict(orient='records'), filt_merged_data.to_dict(orient='records')
    except Exception as e:
      raise ValueError(
        f'Failed to combine celery and hpc worker info, error: {e}')


def calculate_scale_out_scale_in_ops(
    input_df: pd.DataFrame,
    max_items_in_queue : int = 3,
    total_hpc_jobs : int = 30) \
        -> pd.DataFrame:
  """
  A function for calculating scale out and scale in operations

  :param input_df: A pandas dataframe with columns queue_name, hpc_r, hpc_q, task_r, task_i, queued
  :param max_items_in_queue: Maximum number of items allowed in the queue
  :param total_hpc_jobs: Total number of hpc jobs allowed
  :returns: A pandas dataframe with scale_out_ops and scale_in_ops columns
  """
  try:
    required_columns = [
      "queue_name",
      "hpc_r",
      "hpc_q",
      "task_r",
      "task_i",
      "queued"]
    # check if all required columns are present
    # copy the input dataframe
    df = input_df.copy()
    for c in required_columns:
      if c not in df.columns:
        raise KeyError(f'Column {c} is missing from input dataframe')
    df['scale_out_ops'] = 0 
    df['scale_in_ops'] = 0
    allowed_tasks = \
      total_hpc_jobs - (df['hpc_r'].sum() + df['hpc_q'].sum())
    ## scale-in - full
    df['scale_in_ops'] = \
      np.where(
        ((df.hpc_r > 0) & (df.task_r == 0) & (df.queued == 0)),
        df.hpc_r,
        df['scale_in_ops'])
    ## scale-in - partial
    df['scale_in_ops'] = \
      np.where(
        ((df.hpc_r > 0) & (df.task_r < df.hpc_r) & (df.queued == 0)),
        df.hpc_r - df.task_r,
        df['scale_in_ops'])
    ## no new task can be added
    if allowed_tasks < 1:
      return df
    ## adding new tasks
    ## scale-out - partial
    df['scale_out_ops'] = \
      np.where(
        ((df.hpc_r > 0) & (df.hpc_q == 0) & (df.queued > max_items_in_queue)),
        df.queued,
        df['scale_out_ops'])
    ## scale-out - full
    df['scale_out_ops'] = \
      np.where(
        ((df.hpc_r == 0) & (df.hpc_q == 0) & (df.queued > 0)),
        df.queued,
        df['scale_out_ops'])
    ## limit new tasks
    if df['scale_out_ops'].sum() > 0 and \
       df['scale_out_ops'].sum() > allowed_tasks:
      requested_tasks = df['scale_out_ops'].values.tolist()
      new_requested_tasks = [0 for i in range(0, len(requested_tasks))]
      while allowed_tasks:
        for i in range(0, len(new_requested_tasks)):
          if new_requested_tasks[i] < requested_tasks[i] and allowed_tasks:
            new_requested_tasks[i] += 1
            allowed_tasks -= 1
      df['scale_out_ops'] = \
        pd.Series(new_requested_tasks, index=df.index)
    return df
  except Exception as e:
    raise ValueError(
      f'Failed to calculate scale out and scale in ops, error: {e}')


def check_celery_workers_are_active(
      flower_config_file: str,
      worker_id_list: List[str],
      flower_url_key: str = 'flower_url',
      flower_user_key: str = 'flower_user',
      flower_pass_key: str = 'flower_pass') \
        -> Tuple[List[str], List[str]]:
  """
  A function for checking if celery workers are active

  :param flower_config_file: A json file containing celery flower config
  :param worker_id_list: A list of worker ids to check
  :returns: A list of active and inactive worker ids
  """
  try:
    flower_config = \
      read_json_data(flower_config_file)
    flower_config = flower_config[0]
    if flower_url_key not in flower_config or \
       flower_user_key not in flower_config or \
       flower_pass_key not in flower_config:
      raise KeyError(
        'Missing flower config in the config file')
    celery_url = \
      f'{flower_config.get(flower_url_key)}/api/workers?refresh=True'
    res = \
      requests.get(
        celery_url,
        auth=HTTPBasicAuth(
          flower_config.get(flower_user_key),
          flower_config.get(flower_pass_key)))
    if res.status_code != 200:
      raise ValueError('Failed to get celery flower workers.')
    data = res.content.decode()
    data = json.loads(data)
    inactive_workers = list()
    active_workers = list()
    for worker_id in worker_id_list:
      if worker_id in data and data[worker_id].get('active'):
        active_workers.append(worker_id)
      else:
        inactive_workers.append(worker_id)
    return active_workers, inactive_workers
  except Exception as e:
    raise ValueError(
      f'Failed to check celery workers are active, error: {e}')


def filter_scale_in_workers(
      scaled_worker_data: List[dict],
      raw_worker_data: List[dict]) -> List[str]:
  """
  A function for filtering scale in workers

  :param scaled_worker_data: A list of dictionaries with queue_name and scale_in_ops
  :param raw_worker_data: A list of dictionaries with job_id, queue_name, hpc_r, task_i, active_jobs, worker_id
  :returns: A list of worker ids for scale in operations
  """
  try:
    scaled_worker_df = \
      pd.DataFrame(scaled_worker_data)
    raw_worker_df = \
      pd.DataFrame(raw_worker_data)
    required_scaled_worker_columns = [
      "queue_name",
      "scale_in_ops"]
    required_raw_worker_columns = [
      "job_id",
      "queue_name",
      "hpc_r",
      "task_i",
      "task_r",
      "worker_id"]
    for c in required_scaled_worker_columns:
      if c not in scaled_worker_df.columns:
        raise KeyError(
          f'Column {c} is missing from input dataframe')
    for c in required_raw_worker_columns:
      if c not in raw_worker_df.columns:
        raise KeyError(
          f'Column {c} is missing from input dataframe')
    queue_list = \
      scaled_worker_df[scaled_worker_df["scale_in_ops"] > 0]["queue_name"].\
        values.tolist()
    scale_in_workers_list = list()
    for queue_name in queue_list:
      scale_in_workers = \
        raw_worker_df[
          (raw_worker_df["queue_name"]==queue_name)&\
          (raw_worker_df["task_r"]==0)&\
          (raw_worker_df["hpc_r"]==1)&\
          (raw_worker_df["task_i"]==1)]["worker_id"].\
          values.tolist()
      scale_in_counts = \
        scaled_worker_df[scaled_worker_df["queue_name"]==queue_name]["scale_in_ops"].values
      if len(scale_in_counts) > 0:
        ## we should have only one row per queue
        scale_in_counts = scale_in_counts[0]
      while scale_in_counts and scale_in_workers:
        scale_in_workers_list.append(scale_in_workers.pop())
        scale_in_counts -= 1
    return scale_in_workers_list
  except Exception as e:
    raise ValueError(
      f'Failed to filter scale in workers, error: {e}')


def terminate_celery_workers(
      flower_config_file: str,
      celery_worker_list: List[str],
      flower_url_key: str = 'flower_url',
      flower_user_key: str = 'flower_user',
      flower_pass_key: str = 'flower_pass') -> List[str]:
  """
  A function for terminating celery workers

  :param flower_config_file: A json file containing celery flower config
  :param celery_worker_list: A list of worker ids to terminate
  :returns: A list of deleted worker ids
  """
  try:
    flower_config = \
      read_json_data(flower_config_file)
    flower_config = flower_config[0]
    if flower_url_key not in flower_config or \
       flower_user_key not in flower_config or \
       flower_pass_key not in flower_config:
      raise KeyError(
        'Missing flower config in the config file')
    celery_url = \
      f'{flower_config.get(flower_url_key)}/api/worker/shutdown'
    ## final checking before termination
    _, inactive_workers = \
      check_celery_workers_are_active(
        flower_config_file=flower_config_file,
        worker_id_list=celery_worker_list)
    deleted_workers = list()
    for worker_id in inactive_workers:
      celery_shutdown_url = f'{celery_url}/{worker_id}'
      res = \
        requests.post(
          celery_shutdown_url,
          auth=HTTPBasicAuth(
            flower_config.get(flower_user_key),
            flower_config.get(flower_pass_key)))
      if res.status_code != 200:
        raise ValueError(
          f'Failed to delete worker {worker_id}')
      deleted_workers.append(worker_id)
    return deleted_workers
  except Exception as e:
    raise ValueError(
      f'Failed to terminate celery workers, error: {e}')


def prepare_scale_out_workers(
      hpc_worker_config: str,
      scaled_worker_data: List[dict]) -> List[dict]:
  """
  A function for preparing scale out workers

  :param hpc_worker_config: A yaml file containing hpc worker config
  :param scaled_worker_data: A list of dictionaries with queue_name and scale_out_ops
  :returns: A list of dictionaries with queue_name, airflow_queue, new_tasks and pbs_resource
  """
  try:
    check_file_path(hpc_worker_config)
    with open(hpc_worker_config, 'r') as fp:
      hpc_queue_data = load(fp, Loader=SafeLoader)
    scaled_worker_df = pd.DataFrame(scaled_worker_data)
    required_scaled_worker_columns = [
      "queue_name",
      "scale_out_ops"]
    for c in required_scaled_worker_columns:
      if c not in scaled_worker_df.columns:
        raise KeyError(
          f'Column {c} is missing from input dataframe')
    scale_out_workers = \
      scaled_worker_df[scaled_worker_df["scale_out_ops"] > 0][["queue_name", "scale_out_ops"]].\
      to_dict(orient='records')
    scale_out_workers_conf = list()
    for entry in scale_out_workers:
      queue_name = entry.get('queue_name')
      scale_out_counts = entry.get('scale_out_ops')
      if queue_name not in hpc_queue_data or \
         "airflow_queue" not in hpc_queue_data.get(queue_name) or \
          "pbs_resource" not in hpc_queue_data.get(queue_name):
        raise ValueError(
          f'Queue {queue_name} not found or not correctly configured in hpc worker config')
      scale_out_workers_conf.append({
        'queue_name': queue_name,
        'airflow_queue': hpc_queue_data.get(queue_name).get("airflow_queue"),
        'new_tasks': scale_out_counts,
        'pbs_resource': hpc_queue_data.get(queue_name).get("pbs_resource")})
    return scale_out_workers_conf
  except Exception as e:
    raise ValueError(
      f'Failed to prepare scale out workers, error: {e}')


## TASK
@task(
  task_id="celery_flower_workers",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=False)
def celery_flower_workers():
  """
  A task for fetching celery flower workers from the celery flower server

  :returns: A list of dictionaries with worker_id, active_jobs and queue_lists
  """
  try:
    worker_list = \
      get_celery_flower_workers(
        celery_flower_config_file=CELERY_FLOWER_CONFIG_FILE)
    return worker_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="redis_queue_workers",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=False)
def redis_queue_workers():
  """
  A task for fetching pending job counts from redis db

  :returns: A list of dictionaries with queue name as key and pending job counts as the value
  """
  try:
    queue_list = \
      fetch_queue_list_from_redis_server(
        redis_conf_file=REDIS_CONF_FILE)
    return queue_list
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="calculate_workers",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=True)
def calculate_workers(
      hpc_worker_info: str,
      celery_flower_worker_info: List[str],
      redis_queue_info: List[dict],
      max_items_in_queue: int = 3,
      generic_queue_name: str = 'generic',
      total_hpc_jobs: int = 50) -> List[dict]:
  """
  A task for combining celery and hpc worker info for scaling operations

  :param hpc_worker_info: A string containing hpc worker info
  :param celery_flower_worker_info: A list of dictionaries with worker_id, active_jobs and queue_lists
  :param redis_queue_info: A list of dictionaries with queue name as key and pending job counts as the value
  :param generic_queue_name: A string for generic queue name
  :param max_items_in_queue: Maximum number of items allowed in the queue
  :param total_hpc_jobs: Total number of hpc jobs allowed
  :returns: A list of dictionaries with scaled_worker_data and raw_worker_data
  """
  try:
    scaled_worker_data, raw_worker_data = \
      combine_celery_and_hpc_worker_info(
        hpc_worker_info=hpc_worker_info,
        celery_flower_worker_info=celery_flower_worker_info,
        redis_queue_info=redis_queue_info,
        generic_queue_name=generic_queue_name,
        max_items_in_queue=max_items_in_queue,
        total_hpc_jobs=total_hpc_jobs)
    return {"scaled_worker_data": scaled_worker_data, "raw_worker_data": raw_worker_data}
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)



@task.branch(
  task_id="decide_scale_out_scale_in_ops",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=False)
def decide_scale_out_scale_in_ops(
      scaled_workers_data: List[dict],
      scale_in_task: str = 'scale_in_hpc_workers',
      scale_out_task: str = 'prep_scale_out_hpc_workers',
      scale_in_ops_key: str = 'scale_in_ops',
      scale_out_ops_key: str = 'scale_out_ops') -> List[str]:
  """
  A function for deciding scale out and scale in operations

  :param scaled_workers_data: A list of dictionaries with queue_name, hpc_r, hpc_q, task_r, task_i, queued
  :param scale_in_task: A string for scale in task name
  :param scale_out_task: A string for scale out task name
  :param scale_in_ops_key: A string for scale in ops key
  :param scale_out_ops_key: A string for scale out ops key
  :returns: A list of tasks to be executed
  """
  try:
    if scaled_workers_data is None or \
      len(scaled_workers_data) == 0:
      return []
    df = pd.DataFrame(scaled_workers_data)
    if scale_in_ops_key not in df.columns or \
       scale_out_ops_key not in df.columns:
      raise KeyError(
        f'Missing {scale_in_ops_key} or {scale_out_ops_key} \
          in the input dataframe')
    df_scale_in = \
      df[df[scale_in_ops_key]>0].copy()
    df_scale_out = \
      df[df[scale_out_ops_key]>0].copy()
    if len(df_scale_in.index) > 0 and \
      len(df_scale_out.index) > 0 :
      return [scale_in_task, scale_out_task]
    elif len(df_scale_in.index) > 0 and \
      len(df_scale_out.index) == 0 :
      return [scale_in_task]
    elif len(df_scale_in.index) == 0 and \
      len(df_scale_out.index) > 0 :
      return [scale_out_task]
    else:
      return []
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="scale_in_hpc_workers",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=False)
def scale_in_hpc_workers(
      scaled_worker_data: List[dict],
      raw_worker_data: List[dict]) -> List[str]:
  """
  A function for scaling in hpc workers

  :param scaled_worker_data: A list of dictionaries with queue_name and scale_in_ops
  :param raw_worker_data: A list of dictionaries with job_id, queue_name, hpc_r, task_i, active_jobs, worker_id
  :returns: A list of deleted worker ids
  """
  try:
    scale_in_workers_list = \
      filter_scale_in_workers(
        scaled_worker_data=scaled_worker_data,
        raw_worker_data=raw_worker_data)
    deleted_workers = \
      terminate_celery_workers(
        flower_config_file=CELERY_FLOWER_CONFIG_FILE,
        celery_worker_list=scale_in_workers_list)
    return deleted_workers
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)

## TASK
@task(
  task_id="prep_scale_out_hpc_workers",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='generic',
  pool='generic_pool',
  multiple_outputs=False)
def prep_scale_out_hpc_workers(
      scaled_worker_data: List[dict]) -> List[dict]:
  """
  A function for preparing scale out workers

  :param scaled_worker_data: A list of dictionaries with queue_name and scale_out_ops
  :returns: A list of dictionaries with queue_name, airflow_queue, new_tasks and pbs_resource
  """
  try:
    scale_out_workers_conf = \
      prepare_scale_out_workers(
        hpc_worker_config=HPC_QUEUE_LIST,
        scaled_worker_data=scaled_worker_data)
    return scale_out_workers_conf
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=e)
    raise ValueError(e)