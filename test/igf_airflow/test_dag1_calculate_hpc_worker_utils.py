import os
import json
import yaml
import subprocess
import responses
import requests
import unittest
import pandas as pd
from unittest.mock import patch
from yaml import load, dump, SafeLoader, Dumper
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag1_calculate_hpc_worker_utils import (
  get_celery_flower_workers,
  get_redis_queue_tasks,
  scale_hpc_workers,
  filter_scale_in_workers,
  check_celery_worker_status,
  prepare_scale_out_workers)

class Test_dag1_calculate_hpc_worker_utils(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.config_file = os.path.join(self.temp_dir,'config.yaml')

  def tearDown(self):
    remove_dir(self.temp_dir)


  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.read_json_data',
         return_value=[{'flower_url':'http://hostname','flower_user':'B','flower_pass':'C'}])
  @responses.activate
  def test_get_celery_flower_workers(self,*args):
    responses.add(
      responses.GET,
      'http://hostname/api/workers?refresh=True',
      status=200,
      json={})
    workers = get_celery_flower_workers('A')
    self.assertEqual(len(workers), 0)
    responses.add(
      responses.GET,
      'http://hostname/api/workers?refresh=True',
      status=200,
      json={'worker1':{'active':[],'active_queues': []}})
    workers = get_celery_flower_workers('A')
    self.assertEqual(len(workers), 1)
    self.assertEqual(workers[0]['worker_id'],'worker1')
    self.assertEqual(workers[0]['active_jobs'], 0)
    self.assertEqual(workers[0]['queue_lists'], [])
    responses.add(
      responses.GET,
      'http://hostname/api/workers?refresh=True',
      status=200,
      json={'worker1':{'active':[1,],'active_queues': [{'name':'A'}]}})
    workers = get_celery_flower_workers('A')
    self.assertEqual(len(workers), 1)
    self.assertEqual(workers[0]['worker_id'],'worker1')
    self.assertEqual(workers[0]['active_jobs'], 1)
    self.assertEqual(workers[0]['queue_lists'], ['A'])

if __name__=='__main__':
  unittest.main()