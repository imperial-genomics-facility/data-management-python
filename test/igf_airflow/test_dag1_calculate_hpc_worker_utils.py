import os
import json
import yaml
import subprocess
import responses
import requests
import unittest
import pandas as pd
from io import StringIO
from unittest.mock import patch
from yaml import load, dump, SafeLoader, Dumper
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag1_calculate_hpc_worker_utils import (
  get_celery_flower_workers,
  get_redis_queue_tasks,
  combine_celery_and_hpc_worker_info,
  calculate_scale_out_scale_in_ops,
  fetch_queue_list_from_redis_server,
  check_celery_workers_are_active)

class Test_dag1_calculate_hpc_worker_utils(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.config_file = os.path.join(self.temp_dir,'config.yaml')

  def tearDown(self):
    remove_dir(self.temp_dir)


  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.read_json_data',
         return_value=[{'flower_url':'http://hostname','flower_user':'B','flower_pass':'C'}])
  @responses.activate
  def test_get_celery_flower_workers(self, *args):
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

  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.redis')
  def test_fetch_queue_list_from_redis_server(self,redis_mock):
    r = redis_mock.from_url.return_value
    r.keys.return_value = {'A': 'a', 'B': 'b', 'unacked1': 'c', '_unacked2': 'd'}
    r.llen.side_effect = [1,2]
    queue_list = fetch_queue_list_from_redis_server('A')
    self.assertEqual(len(queue_list), 2)
    self.assertEqual(queue_list[0], {'A':1})
    self.assertEqual(queue_list[1], {'B':2})

  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.read_json_data',
         return_value=[{'redis_db':'A'}])
  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.fetch_queue_list_from_redis_server',
         return_value=[{'A':1}])
  def test_get_redis_queue_tasks(self, *args):
    queue_list = get_redis_queue_tasks('A')
    self.assertEqual(len(queue_list), 1)
    self.assertEqual(queue_list[0], {'A':1})


  def test_calculate_scale_out_scale_in_ops(self):
    input_data = [ {'queue_name':'hpc_4G','hpc_r':9,'hpc_q':0,'task_r':9,'task_i':0,'queued':7},
                   {'queue_name':'hpc_64G16t','hpc_r':2,'hpc_q':0,'task_r':2,'task_i':0,'queued':0},
                   {'queue_name':'hpc_8G8t','hpc_r':1,'hpc_q':0,'task_r':0,'task_i':1,'queued':0} ]
    input_df = pd.DataFrame(input_data)
    scaled_df = \
      calculate_scale_out_scale_in_ops(
        input_df=input_df,
        max_items_in_queue=3,
        total_hpc_jobs=30)
    self.assertTrue('scale_out_ops' in scaled_df.columns)
    self.assertTrue('scale_in_ops' in scaled_df.columns)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertTrue('hpc_64G16t' in scaled_df['queue_name'].values.tolist())
    self.assertTrue('hpc_8G8t' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 7)
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_64G16t"]["scale_out_ops"].values[0], 0)
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_8G8t"]["scale_in_ops"].values[0], 1)
    input_data = [ {'queue_name':'hpc_4G','hpc_r':10,'hpc_q':0,'task_r':10,'task_i':0,'queued':7}]
    input_df = pd.DataFrame(input_data)
    scaled_df = \
      calculate_scale_out_scale_in_ops(
        input_df=input_df,
        max_items_in_queue=3,
        total_hpc_jobs=10)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 0)


  def test_combine_celery_and_hpc_worker_info(self):
    hpc_worker_info = \
      """834752.pbs,hpc_8G8t,R
      834801.pbs,hpc_4G,R
      834869.pbs,hpc_4G,R
      834876.pbs,hpc_64G16t,R
      834878.pbs,hpc_64G16t,R
      834889.pbs,hpc_4G,R
      834890.pbs,hpc_4G,R
      834891.pbs,hpc_4G,R
      834892.pbs,hpc_4G,R
      834894.pbs,hpc_4G,R
      834895.pbs,hpc_4G,R
      834896.pbs,hpc_4G,R"""
    celery_flower_worker_info = [
      {'worker_id':'celery@834752.pbs-hpc_8G8t','active_jobs':1,'queue_lists':['hpc_8G8t']},
      {'worker_id':'celery@834801.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834869.pbs-hpc_4G ','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834876.pbs-hpc_64G16t','active_jobs':1,'queue_lists':['hpc_64G16t']},
      {'worker_id':'celery@834878.pbs-hpc_64G16t','active_jobs':1,'queue_lists':['hpc_64G16t']},
      {'worker_id':'celery@834889.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834890.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834891.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834892.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834894.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834895.pbs-hpc_4G ','active_jobs':1,'queue_lists':['hpc_4G']},
      {'worker_id':'celery@834896.pbs-hpc_4G','active_jobs':1,'queue_lists':['hpc_4G']}]
    redis_queue_info = [{'hpc_4G':7}]
    scaled_workers = \
      combine_celery_and_hpc_worker_info(
        hpc_worker_info=hpc_worker_info,
        celery_flower_worker_info=celery_flower_worker_info,
        redis_queue_info=redis_queue_info,
        max_items_in_queue=3,
        total_hpc_jobs=30)
    scaled_df = pd.DataFrame(scaled_workers)
    self.assertTrue('queue_name' in scaled_df.columns)
    self.assertTrue('scale_out_ops' in scaled_df.columns)
    self.assertTrue('scale_in_ops' in scaled_df.columns)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertTrue('hpc_64G16t' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 7)
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_64G16t"]["scale_out_ops"].values[0], 0)


  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.read_json_data',
         return_value=[{'flower_url':'http://hostname','flower_user':'B','flower_pass':'C'}])
  @responses.activate
  def test_check_celery_workers_are_active(self, *args):
    responses.add(
      responses.GET,
      'http://hostname/api/workers?refresh=True',
      status=200,
      json={
        'worker1': {'active':[1,], 'active_queues': [{'name': 'A'}]},
        'worker2': {'active':[], 'active_queues': []}})
    active_workers, inactive_workers = \
      check_celery_workers_are_active(
        flower_config_file='A',
        worker_id_list=['worker1', 'worker2'])
    self.assertEqual(len(active_workers), 1)
    self.assertEqual(len(inactive_workers), 1)
    self.assertEqual(active_workers[0], 'worker1')
    self.assertEqual(inactive_workers[0], 'worker2')



if __name__=='__main__':
  unittest.main()