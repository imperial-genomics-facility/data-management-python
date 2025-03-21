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
  # get_redis_queue_tasks,
  combine_celery_and_hpc_worker_info,
  calculate_scale_out_scale_in_ops,
  fetch_queue_list_from_redis_server,
  check_celery_workers_are_active,
  filter_scale_in_workers,
  terminate_celery_workers,
  prepare_scale_out_workers,
  celery_flower_workers,
  redis_queue_workers,
  calculate_workers,
  decide_scale_out_scale_in_ops,
  scale_in_hpc_workers,
  prep_scale_out_hpc_workers)

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
    temp_dir = get_temp_dir()
    redis_config_file = os.path.join(temp_dir, 'redis_config.json')
    with open(redis_config_file, 'w') as json_data:
      json.dump({'redis_db': 'A'}, json_data)
    queue_list = fetch_queue_list_from_redis_server(redis_config_file)
    self.assertEqual(len(queue_list), 2)
    self.assertEqual(queue_list[0], {'A':1})
    self.assertEqual(queue_list[1], {'B':2})

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
    # hpc_worker_info = \
    #   """834752.pbs,hpc_8G8t,R
    #   834801.pbs,hpc_4G,R
    #   834869.pbs,hpc_4G,R
    #   834876.pbs,hpc_64G16t,R
    #   834878.pbs,hpc_64G16t,R
    #   834889.pbs,hpc_4G,R
    #   834890.pbs,hpc_4G,R
    #   834891.pbs,hpc_4G,R
    #   834892.pbs,hpc_4G,R
    #   834894.pbs,hpc_4G,R
    #   834895.pbs,hpc_4G,R
    #   834896.pbs,hpc_4G,R"""
    hpc_worker_info = \
      "ODM0NzUyLnBicyxocGNfOEc4dCxSCjgzND\
      gwMS5wYnMsaHBjXzRHLFIKODM0ODY5LnBicy\
      xocGNfNEcsUgo4MzQ4NzYucGJzLGhwY182NEc\
      xNnQsUgo4MzQ4NzgucGJzLGhwY182NEcxNnQsU\
      go4MzQ4ODkucGJzLGhwY180RyxSCjgzNDg5MC5\
      wYnMsaHBjXzRHLFIKODM0ODkxLnBicyxocGNfNE\
      csUgo4MzQ4OTIucGJzLGhwY180RyxSCjgzNDg5NC\
      5wYnMsaHBjXzRHLFIKODM0ODk1LnBicyxocGNfNEc\
      sUgo4MzQ4OTYucGJzLGhwY180RyxS"
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
    scaled_workers, raw_workers = \
      combine_celery_and_hpc_worker_info(
        hpc_worker_info=hpc_worker_info,
        celery_flower_worker_info=celery_flower_worker_info,
        redis_queue_info=redis_queue_info,
        max_items_in_queue=3,
        total_hpc_jobs=30)
    scaled_df  = pd.DataFrame(scaled_workers)
    self.assertTrue('queue_name' in scaled_df.columns)
    self.assertTrue('scale_out_ops' in scaled_df.columns)
    self.assertTrue('scale_in_ops' in scaled_df.columns)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertTrue('hpc_64G16t' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 7)
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_64G16t"]["scale_out_ops"].values[0], 0)
    raw_df = pd.DataFrame(raw_workers)
    self.assertTrue('job_id' in raw_df.columns)
    self.assertTrue('worker_id' in raw_df.columns)
    hpc_worker_info = "ODc0Mzk0LnBicyxocGNfYWlyZmxvdzMsUQo="
    celery_flower_worker_info = [
      {'worker_id':'generic','active_jobs':1,'queue_lists':['generic']}]
    redis_queue_info = [{'hpc_4G':1}]
    scaled_workers, raw_workers = \
      combine_celery_and_hpc_worker_info(
        hpc_worker_info=hpc_worker_info,
        celery_flower_worker_info=celery_flower_worker_info,
        redis_queue_info=redis_queue_info,
        max_items_in_queue=3,
        total_hpc_jobs=30)
    scaled_df  = pd.DataFrame(scaled_workers)
    self.assertTrue('queue_name' in scaled_df.columns)
    self.assertTrue('scale_out_ops' in scaled_df.columns)
    self.assertTrue('scale_in_ops' in scaled_df.columns)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 1)
    hpc_worker_info = ""
    celery_flower_worker_info = [
      {'worker_id':'generic','active_jobs':1,'queue_lists':['generic']}]
    redis_queue_info = [{'hpc_4G':1}]
    scaled_workers, raw_workers = \
      combine_celery_and_hpc_worker_info(
        hpc_worker_info=hpc_worker_info,
        celery_flower_worker_info=celery_flower_worker_info,
        redis_queue_info=redis_queue_info,
        max_items_in_queue=3,
        total_hpc_jobs=30)
    scaled_df  = pd.DataFrame(scaled_workers)
    self.assertTrue('queue_name' in scaled_df.columns)
    self.assertTrue('scale_out_ops' in scaled_df.columns)
    self.assertTrue('scale_in_ops' in scaled_df.columns)
    self.assertTrue('hpc_4G' in scaled_df['queue_name'].values.tolist())
    self.assertEqual(scaled_df[scaled_df["queue_name"] == "hpc_4G"]["scale_out_ops"].values[0], 1)


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


  def test_filter_scale_in_workers(self):
    scaled_worker_data = [
      {'queue_name':'hpc_64G16t','scale_in_ops':0},
      {'queue_name':'hpc_8G8t','scale_in_ops':1}]
    raw_worker_data = [
      {
        'job_id': '834752.pbs',
        'queue_name': 'hpc_8G8t',
        'hpc_r': 1,
        'task_i': 0,
        'task_r': 1,
        'worker_id': 'celery@834752.pbs-hpc_8G8t'},{
        'job_id': '834801.pbs',
        'queue_name': 'hpc_8G8t',
        'hpc_r': 1,
        'task_i': 1,
        'task_r': 0,
        'worker_id': 'celery@834801.pbs-hpc_8G8t'},{
        'job_id': '834876.pbs',
        'queue_name': 'hpc_64G16t',
        'hpc_r': 1,
        'task_i': 0,
        'active_jobs': 1,
        'worker_id': 'celery@834876.pbs-hpc_64G16t'}]
    filter_scale_in_workers_list = \
      filter_scale_in_workers(
        scaled_worker_data=scaled_worker_data,
        raw_worker_data=raw_worker_data)
    self.assertEqual(len(filter_scale_in_workers_list), 1)
    self.assertEqual(filter_scale_in_workers_list[0], 'celery@834801.pbs-hpc_8G8t')


  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.read_json_data',
         return_value=[{'flower_url':'http://hostname','flower_user':'B','flower_pass':'C'}])
  @responses.activate
  def test_terminate_celery_workers(self, *args):
    responses.add(
      responses.GET,
      'http://hostname/api/workers?refresh=True',
      status=200,
      json={
        'worker1': {'active':[1,], 'active_queues': [{'name': 'A'}]},
        'worker2': {'active':[], 'active_queues': []}})
    responses.add(
      responses.POST,
      'http://hostname/api/worker/shutdown/worker2',
      status=200)
    deleted_workers = \
      terminate_celery_workers(
        flower_config_file='A',
        celery_worker_list=['worker1', 'worker2'])
    self.assertEqual(len(deleted_workers), 1)
    self.assertEqual(deleted_workers[0], 'worker2')

  def test_prepare_scale_out_workers(self):
    temp_dir = get_temp_dir()
    hpc_queue_data = {
      'hpc_4G': {
        'pbs_resource': '-lselect=1:ncpus=1:mem=4gb -lwalltime=12:00:00',
        'airflow_queue': 'hpc_4G'},
      'hpc_8G8t': {
        'pbs_resource': '-lselect=1:ncpus=8:mem=8gb -lwalltime=12:00:00',
        'airflow_queue': 'hpc_4G,hpc_8G8t'}}
    hpc_worker_config = \
      os.path.join(self.temp_dir, 'hpc_worker_config.yaml')
    with open(hpc_worker_config, 'w') as hpc_config:
      yaml.dump(hpc_queue_data, hpc_config)
    scaled_worker_data = [
      {'queue_name':'hpc_64G16t','scale_out_ops':0},
      {'queue_name':'hpc_8G8t','scale_out_ops':2},
      {'queue_name':'hpc_4G','scale_out_ops':1}]
    scale_out_workers_conf = \
      prepare_scale_out_workers(
        hpc_worker_config=hpc_worker_config,
        scaled_worker_data=scaled_worker_data)
    self.assertEqual(len(scale_out_workers_conf), 2)
    df = pd.DataFrame(scale_out_workers_conf)
    self.assertTrue('queue_name' in df.columns)
    self.assertTrue('pbs_resource' in df.columns)
    self.assertTrue('airflow_queue' in df.columns)
    self.assertTrue('new_tasks' in df.columns)
    self.assertTrue('hpc_8G8t' in df['queue_name'].values.tolist())
    self.assertTrue('hpc_4G' in df['queue_name'].values.tolist())
    self.assertEqual(df[df["queue_name"] == "hpc_8G8t"]["new_tasks"].values[0], 2)
    self.assertEqual(
      df[df["queue_name"] == "hpc_8G8t"]["pbs_resource"].values[0],
      '-lselect=1:ncpus=8:mem=8gb -lwalltime=12:00:00')
    self.assertEqual(
      df[df["queue_name"] == "hpc_8G8t"]["airflow_queue"].values[0],
      'hpc_4G,hpc_8G8t')
    self.assertEqual(df[df["queue_name"] == "hpc_4G"]["new_tasks"].values[0], 1)


  @patch("igf_airflow.utils.dag1_calculate_hpc_worker_utils.get_current_context")
  @patch("igf_airflow.utils.dag1_calculate_hpc_worker_utils.Variable")
  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.get_celery_flower_workers',
        return_value=[{'worker_id':'worker1','active_jobs':1,'queue_lists':['A']}])
  def test_celery_flower_workers(
        self,
        get_celery_flower_workers,
        *args):
    celery_worker_config = os.path.join(self.temp_dir, 'celery_worker_config.json')
    with open(celery_worker_config, 'w') as json_data:
      json.dump({'flower_url':'A', 'flower_user':'B', 'flower_pass':'C'}, json_data)
    with patch("igf_airflow.utils.dag1_calculate_hpc_worker_utils.CELERY_FLOWER_CONFIG_FILE", celery_worker_config):
      worker_list = celery_flower_workers.function()
      get_celery_flower_workers.assert_called_once()
      self.assertEqual(len(worker_list), 1)
      self.assertEqual(worker_list[0]['worker_id'], 'worker1')

  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.fetch_queue_list_from_redis_server',
         return_value=[{'A':1}])
  def test_redis_queue_workers(self, *args):
    redis_db_conf = os.path.join(self.temp_dir, 'redis_db_conf.json')
    with open(redis_db_conf, 'w') as json_data:
      json.dump({'A': 'B'}, json_data)
    with patch("igf_airflow.utils.dag1_calculate_hpc_worker_utils.REDIS_CONF_FILE", redis_db_conf):
      queue_list = redis_queue_workers.function()
      self.assertEqual(len(queue_list), 1)
      self.assertEqual(queue_list[0], {'A':1})

  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.combine_celery_and_hpc_worker_info',
         return_value=['A', 'B'])
  def test_calculate_workers(self, mock_combine_celery_and_hpc_worker_info):
    workers = \
      calculate_workers.function(
        hpc_worker_info='a',
        celery_flower_worker_info='b',
        redis_queue_info='c')
    mock_combine_celery_and_hpc_worker_info.assert_called_once()
    self.assertEqual(workers["scaled_worker_data"], 'A')
    self.assertEqual(workers["raw_worker_data"], 'B')


  def test_decide_scale_out_scale_in_ops(self):
    scaled_workers_data = [
      {'queue_name':'hpc_4G','scale_out_ops':0, 'scale_in_ops':0}]
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, [])
    scaled_workers_data = [
      {'queue_name':'hpc_8G','scale_out_ops':1, 'scale_in_ops': 0}]
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, ['scale_out_task'])
    scaled_workers_data = [
      {'queue_name':'hpc_16G','scale_out_ops':0, 'scale_in_ops': 1}]
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, ['scale_in_task'])
    scaled_workers_data = []
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, [])
    scaled_workers_data = None
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, [])
    scaled_workers_data = [{
      "hpc_q":0,
      "hpc_r":0,
      "queue_name":"hpc_4G",
      "queued":5,
      "scale_in_ops":0,
      "scale_out_ops":5,
      "task_i":0,
      "task_r":0}]
    next_tasks = \
      decide_scale_out_scale_in_ops.function(
        scaled_workers_data,
        scale_in_task='scale_in_task',
        scale_out_task='scale_out_task')
    self.assertEqual(next_tasks, ['scale_out_task'])



  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.filter_scale_in_workers',
          return_value=['A'])
  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.terminate_celery_workers',
          return_value=['B'])
  def test_scale_in_hpc_workers(
        self,
        terminate_celery_workers,
        filter_scale_in_workers):
    with patch("igf_airflow.utils.dag1_calculate_hpc_worker_utils.CELERY_FLOWER_CONFIG_FILE",
               'C'):
      deleted_workers = \
        scale_in_hpc_workers.function(
          scaled_worker_data=[{'queue_name':'hpc_4G','scale_in_ops':1}],
          raw_worker_data=[{'worker_id':'worker1','active_jobs':1,'queue_lists':['A']}])
      filter_scale_in_workers.assert_called_once()
      terminate_celery_workers.assert_called_once()
      self.assertEqual(deleted_workers, ['B'])


  @patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.prepare_scale_out_workers',
          return_value='A')
  def test_prep_scale_out_hpc_workers(
        self,
        prepare_scale_out_workers):
    with patch('igf_airflow.utils.dag1_calculate_hpc_worker_utils.HPC_QUEUE_LIST',
               'B'):
      scale_out_workers_conf = \
        prep_scale_out_hpc_workers.function(
          scaled_worker_data='C')
      self.assertEqual(scale_out_workers_conf, 'A')
      prepare_scale_out_workers.assert_called_once()


if __name__=='__main__':
  unittest.main()