import requests, json
from typing import List
from igf_data.utils.dbutils import read_json_data
from requests.auth import HTTPBasicAuth

def get_celery_flower_workers(
      celery_flower_config_file: str,
      flower_url_key: str = 'flower_url',
      flower_user_key: str = 'flower_user',
      flower_pass_key: str = 'flower_pass') -> List[str]:
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

def get_redis_queue_tasks():
    pass

def scale_hpc_workers():
    pass

def filter_scale_in_workers():
    pass

def check_celery_worker_status():
    pass

def prepare_scale_out_workers():
    pass
