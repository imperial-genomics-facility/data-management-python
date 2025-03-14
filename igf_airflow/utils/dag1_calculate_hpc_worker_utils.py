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
    raise ValueError(f'Failed to get redis queue tasks, error: {e}')

def scale_hpc_workers():
    pass

def filter_scale_in_workers():
    pass

def check_celery_worker_status():
    pass

def prepare_scale_out_workers():
    pass
