import pandas as pd
import numpy as np
from io import StringIO
import requests, json, redis
from typing import List
from igf_data.utils.dbutils import read_json_data
from requests.auth import HTTPBasicAuth

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
    flower_user = flower_config.get(flower_user_key)
    flower_pass = flower_config.get(flower_pass_key)
    flower_url = flower_config.get(flower_url_key)
    celery_url = f'{flower_url}/api/workers?refresh=True'
    res = \
      requests.get(
        celery_url,
        auth=HTTPBasicAuth(flower_user, flower_pass))
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
      url: str) -> List[dict]:
  """
  A function for fetching pending job counts from redis db

  :param url: A redis db connection URL
  :returns: A list of dictionaries with queue name as key and pending job counts as the value
  """
  try:
    queue_list = list()
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


def get_redis_queue_tasks(redis_conf_file: str) -> List[dict]:
  try:
    redis_conf = read_json_data(redis_conf_file)
    redis_conf = redis_conf[0]
    url = redis_conf.get('redis_db')
    queue_list = fetch_queue_list_from_redis_server(url=url)
    return queue_list
  except Exception as e:
    raise ValueError(
      f'Failed to get redis queue tasks, error: {e}')


def merge_worker_info_and_scale_workers(
    hpc_worker_info: str,
    celery_flower_worker_info: List[str],
    redis_queue_info: List[dict],
    max_items_in_queue: int = 3,
    total_hpc_jobs: int = 30) -> List[dict]:
  try:
    pass
  except Exception as e:
    raise ValueError(f'Failed to merge worker info and scale workers, error: {e}')


def combine_celery_and_hpc_worker_info(
    hpc_worker_info: str,
    celery_flower_worker_info: List[dict],
    redis_queue_info: List[dict],
    generic_queue_name: str = 'generic',
    max_items_in_queue: int = 3,
    total_hpc_jobs: int = 30)-> List[dict]:
    try:
      ## process hpc output
      hpc_jobs_df = pd.DataFrame()
      if hpc_worker_info != "":
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
      merged_data = pd.DataFrame()
      if len(hpc_jobs_df.index) > 0 and len(filt_worker_df.index) > 0:
        merged_data = \
          hpc_jobs_df.set_index(['job_id','queue_name']).\
            join(
              filt_worker_df.set_index(['job_id','queue_name']),
              how='outer').\
            reset_index()
        merged_data = \
          merged_data.fillna({"worker_id":"U"})
        merged_data['job_status'] = \
          merged_data['job_status'].fillna('U')
        merged_data['active_jobs'] = \
          merged_data['active_jobs'].fillna(0)
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
            'queue_name',
            'hpc_r',
            'hpc_q',
            'task_r',
            'task_i']].copy()
        ## count the number of jobs per queue
        agg_df = \
          filt_merged_data.\
            groupby('queue_name').agg("sum")
        ## add the queue info from redis
        if len(redis_queue_info) > 0:
          queue_data = [
            {'queue_name': queue_name, 'queued': queued_jobs}
              for entry in redis_queue_info
                for queue_name, queued_jobs in entry.items()]
          queue_df = pd.DataFrame(queue_data)
          final_agg_df = \
            agg_df.join(
              queue_df.set_index("queue_name"),
              how="left")
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
        return scaled_df.to_dict(orient='records')
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
    if df['scale_out_ops'].sum() > allowed_tasks:
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

def scale_hpc_workers():
    pass

def filter_scale_in_workers():
    pass

def check_celery_worker_status():
    pass

def prepare_scale_out_workers():
    pass
