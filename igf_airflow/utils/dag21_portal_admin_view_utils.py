
from igf_data.igfdb.igfTables import Seqrun, Platform, Pipeline_seed
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
import os, re, logging, json, base64
import pandas as pd
from datetime import datetime, timedelta
from airflow.models import Variable
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import copy_local_file, check_file_path
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_portal.api_utils import upload_files_to_portal

DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)
IGF_PORTAL_CONF = \
  Variable.get('igf_portal_conf', default_var=None)


def _get_seqrun_plot_data(db_config_file: str) -> dict:
  try:
    backgroundColors = [
      "rgba(75, 192, 192, 0.8)",
      "rgba(255, 99, 132, 0.8)",
      "rgba(54, 162, 235, 0.8)",
      "rgba(255, 159, 64, 0.8)",
      "rgb(216, 149, 252, 0.8)"]
    borderColors = [
      "rgba(75, 192, 192, 1)",
      "rgba(255, 99, 132, 1)",
      "rgba(54, 162, 235, 1)",
      "rgba(255, 159, 64, 1)",
      "rgb(216, 149, 252, 1)"]
    dbparams = read_dbconf_json(db_config_file)
    base = BaseAdaptor(**dbparams)
    base.start_session()
    query = \
      base.session.\
        query(
          Seqrun.date_created,
          Platform.model_name).\
        join(Platform, Platform.platform_id==Seqrun.platform_id).\
        filter(Seqrun.reject_run=='N')
    results = \
      base.fetch_records(
          query=query)
    base.close_session()
    results['date_stamp'] = \
      results['date_created'].\
        map(lambda x: datetime.strftime(x, '%Y-%m'))
    data_list = list()
    for ds, ds_data in results.groupby('date_stamp'):
      ds_dict = dict(date_stamp=ds)
      for m, m_data in ds_data.groupby('model_name'):
        ds_dict.update({m: len(m_data.index)})
      data_list.append(ds_dict)
    df = pd.DataFrame(data_list)
    df.fillna(0, inplace=True)
    df.sort_values('date_stamp', inplace=True, ascending=True)
    df.set_index('date_stamp', inplace=True)
    dataset = list()
    counter = 0
    for i in results['model_name'].drop_duplicates().values.tolist():
      df[i] = df[i].astype(int)
      dataset.\
        append({
          "label": i,
          "data": df[i].values.tolist(),
          "backgroundColor": backgroundColors[counter],
          "borderColor":  borderColors[counter]})
      counter += 1
    return {"labels": df.index.to_list(), "datasets": dataset}
  except Exception as e:
    raise ValueError("Failed to get plot data, error: {0}".format(e))


def get_seqrun_counts_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('json_dump_xcom_key')
    seqrun_plot_data = \
      _get_seqrun_plot_data(
        db_config_file=DATABASE_CONFIG_FILE)
    temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    seqrun_stats_json = \
      os.path.join(temp_dir, 'seqrun_stats.json')
    with open(seqrun_stats_json, 'w') as fp:
      json.dump(seqrun_plot_data, fp)
    ti.xcom_push(
      key=xcom_key,
      value=seqrun_stats_json)
  except Exception as e:
    logging.error(e)
    message = \
      'Failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def prepare_storage_plot_func(**context):
  try:
    ti = context.get('ti')
    orwell_home = \
      context['params'].get('orwell_home')
    wells_home = \
      context['params'].get('wells_home')
    wells_data = \
      context['params'].get('wells_data')
    eliot_root = \
      context['params'].get('eliot_root')
    eliot_data = \
      context['params'].get('eliot_data')
    eliot_data2 = \
      context['params'].get('eliot_data2')
    igf_lims_root = \
      context['params'].get('igf_lims_root')
    woolf_root = \
      context['params'].get('woolf_root')
    woolf_data1 = \
      context['params'].get('woolf_data1')
    woolf_data2 = \
      context['params'].get('woolf_data2')
    nextseq1_data = \
      context['params'].get('nextseq1_data')
    igfportal_data = \
      context['params'].get('igfportal_data')
    hpc_rds = \
      context['params'].get('hpc_rds')
    xcom_key = \
      context['params'].get('xcom_key')
    orwell_home_data = \
      ti.xcom_pull(
        task_ids=orwell_home)
    orwell_home_data = \
      _convert_base64_to_ascii(orwell_home_data)
    wells_home_data = \
      ti.xcom_pull(task_ids=wells_home)
    wells_home_data = \
      _convert_base64_to_ascii(wells_home_data)
    wells_data_data = \
      ti.xcom_pull(task_ids=wells_data)
    wells_data_data = \
      _convert_base64_to_ascii(wells_data_data)
    nextseq1_data_data = \
      ti.xcom_pull(task_ids=nextseq1_data)
    nextseq1_data_data = \
      _convert_base64_to_ascii(nextseq1_data_data)
    eliot_root_data = \
      ti.xcom_pull(task_ids=eliot_root)
    eliot_root_data = \
      _convert_base64_to_ascii(eliot_root_data)
    eliot_data_data = \
      ti.xcom_pull(task_ids=eliot_data)
    eliot_data_data = \
      _convert_base64_to_ascii(eliot_data_data)
    eliot_data2_data = \
      ti.xcom_pull(task_ids=eliot_data2)
    eliot_data2_data = \
      _convert_base64_to_ascii(eliot_data2_data)
    igf_lims_root_data = \
      ti.xcom_pull(task_ids=igf_lims_root)
    igf_lims_root_data = \
      _convert_base64_to_ascii(igf_lims_root_data)
    woolf_root_data = \
      ti.xcom_pull(task_ids=woolf_root)
    woolf_root_data = \
      _convert_base64_to_ascii(woolf_root_data)
    woolf_data1_data = \
      ti.xcom_pull(task_ids=woolf_data1)
    woolf_data1_data = \
      _convert_base64_to_ascii(woolf_data1_data)
    woolf_data2_data = \
      ti.xcom_pull(task_ids=woolf_data2)
    woolf_data2_data = \
      _convert_base64_to_ascii(woolf_data2_data)
    igfportal_root_data = \
      ti.xcom_pull(task_ids=igfportal_data)
    igfportal_root_data = \
      _convert_base64_to_ascii(igfportal_root_data)
    hpc_rds_data = \
      ti.xcom_pull(task_ids=hpc_rds)
    data_dict = {
      'orwell_home': orwell_home_data,
      'wells_home': wells_home_data,
      'wells_data': wells_data_data,
      'eliot_root': eliot_root_data,
      'eliot_data': eliot_data_data,
      'eliot_data2': eliot_data2_data,
      'igf_lims_root': igf_lims_root_data,
      'woolf_root': woolf_root_data,
      'woolf_data1': woolf_data1_data,
      'woolf_data2': woolf_data2_data,
      'hpc_rds': hpc_rds_data,
      'igfportal_root': igfportal_root_data,
      'NextSeq_1': nextseq1_data_data
    }
    plot_data = \
      _get_storage_plot(data_dict=data_dict)
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    json_dump = os.path.join(temp_dir, 'storage_stat.json')
    with open(json_dump, 'w') as fp:
      json.dump(plot_data, fp)
    ti.xcom_push(
      key=xcom_key,
      value=json_dump)
  except Exception as e:
    logging.error(e)
    message = \
      'Failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def _get_storage_plot(data_dict: dict) -> dict:
  try:
    storage_data = list()
    storage_pattern = re.compile('(\d+)\s+(\d+)\s+(\S+)')
    hpc_storage_pattern = re.compile('(\d+)\.(\d+)TB\s+(\d+)\.(\d+)TB')         # expecting used data in TBs
    for key, value in data_dict.items():
      if re.match(storage_pattern, value):
        used, available, name = \
          re.match(storage_pattern, value).groups()
        total = int(used) + int(available)
        used_pct = (int(used) / int(total)) * 100
        used_pct = int(used_pct)
        available_pct = 100 - used_pct
        storage_data.\
          append({
            'storage': key,
            'used': used_pct,
            'free': available_pct})
      elif re.match(hpc_storage_pattern, value):
        used_int, _, total_int, _ = \
          re.match(hpc_storage_pattern, value).groups()
        used_pct = (int(used_int) / int(total_int)) * 100
        used_pct = int(used_pct)
        available_pct = 100 - used_pct
        storage_data.\
          append({
            'storage': key,
            'used': used_pct,
            'free': available_pct})
    storage_df = pd.DataFrame(storage_data)
    storage_df.set_index('storage', inplace=True)
    labels = storage_df.index.tolist()
    datasets = [{
      "label": "Used",
      "data": storage_df["used"].astype(int).values.tolist(),
      "backgroundColor": "rgba(255, 99, 132, 0.8)",
      "borderColor": "rgba(255, 99, 132, 1)"
    },{
      "label": "Free",
      "data": storage_df["free"].astype(int).values.tolist(),
      "backgroundColor": "rgba(54, 162, 235, 0.8)",
      "borderColor": "rgba(54, 162, 235, 1)"
    }]
    plot_data = {
      "labels": labels,
      "datasets": datasets}
    return plot_data
  except Exception as e:
    raise ValueError("Failed to get storage plot, error: {0}".format(e))


def _convert_base64_to_ascii(base64_str: str) -> str:
  try:
    ascii_str = \
      base64.b64decode(
        base64_str.encode('ascii')).\
      decode('utf-8').\
      strip()
    return ascii_str
  except Exception as e:
    raise ValueError("Failed to convert base64 to ascii, error: {0}".format(e))


def _get_pipeline_counts(
    seed_table: str, status_list: list,
    db_config_file: str, days_count: int=30) -> int:
  try:
    if seed_table not in ("seqrun", "analysis"):
      raise KeyError("seed table {0} not supported".format(seed_table))
    if len(status_list) == 0:
      raise ValueError("No status list provided for db lookup")
    dbparams = read_dbconf_json(db_config_file)
    base = BaseAdaptor(**dbparams)
    base.start_session()
    query = \
      base.session.\
        query(Pipeline_seed).\
        filter(Pipeline_seed.seed_table==seed_table).\
        filter(Pipeline_seed.status.in_(status_list)).\
        filter(Pipeline_seed.date_stamp > datetime.now() - timedelta(days=days_count))
    results = \
      base.fetch_records(query=query)
    base.close_session()
    return len(results.index)
  except Exception as e:
    raise ValueError("Failed to get pipeline counts, error: {0}".format(e))


def get_pipeline_stats_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    ongoing_runs = \
      _get_pipeline_counts(
        seed_table='seqrun',
        status_list=['SEEDED', 'RUNNING'],
        db_config_file=DATABASE_CONFIG_FILE)
    finished_runs = \
      _get_pipeline_counts(
        seed_table='seqrun',
        status_list=['FINISHED'],
        db_config_file=DATABASE_CONFIG_FILE)
    ongoing_analysis = \
      _get_pipeline_counts(
        seed_table='analysis',
        status_list=['SEEDED', 'RUNNING'],
        db_config_file=DATABASE_CONFIG_FILE)
    finished_analysis = \
      _get_pipeline_counts(
        seed_table='analysis',
        status_list=['FINISHED'],
        db_config_file=DATABASE_CONFIG_FILE)
    pipeline_stats = {
      'ongoing_runs': ongoing_runs,
      'recent_finished_runs': finished_runs,
      'ongoing_analysis': ongoing_analysis,
      'recent_finished_analysis': finished_analysis
    }
    ti.xcom_push(
      key=xcom_key,
      value=pipeline_stats)
  except Exception as e:
    logging.error(e)
    message = \
      'Failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def combine_all_admin_view_data_to_json(
    seqrun_json: str, storage_stat_json: str, pipeline_stats: dict,
    tag: str="production_data") -> str:
  try:
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    temp_json = os.path.join(temp_dir, "admin_data.json")
    with open(seqrun_json, 'r') as fp:
      seqrun_json_data = json.load(fp)
    with open(storage_stat_json, 'r') as fp:
      storage_stat_json_data = json.load(fp)
    final_data = {
      "admin_data_tag": tag,
      "sequence_counts_plot": seqrun_json_data,
      "storage_stat_plot": storage_stat_json_data}
    final_data.update(pipeline_stats)
    with open(temp_json, 'w') as fp:
      json.dump(final_data, fp)
    return temp_json
  except Exception as e:
    raise ValueError("Failed to get combined json, error: {0}".format(e))


def create_merged_json_and_upload_to_portal_func(**context):
  try:
    ti = context.get('ti')
    seqrun_json_xcom_task = \
      context['params'].get('seqrun_json_xcom_task')
    seqrun_json_xcom_key = \
      context['params'].get('seqrun_json_xcom_key')
    storage_stat_xcom_key = \
      context['params'].get('storage_stat_xcom_key')
    storage_stat_xcom_task = \
      context['params'].get('storage_stat_xcom_task')
    pipeline_stats_xcom_task = \
      context['params'].get('pipeline_stats_xcom_task')
    pipeline_stats_xcom_key = \
      context['params'].get('pipeline_stats_xcom_key')
    seqrun_json = \
      ti.xcom_pull(
        task_ids=seqrun_json_xcom_task,
        key=seqrun_json_xcom_key)
    storage_stat_json = \
      ti.xcom_pull(
        task_ids=storage_stat_xcom_task,
        key=storage_stat_xcom_key)
    pipeline_stats = \
      ti.xcom_pull(
        task_ids=pipeline_stats_xcom_task,
        key=pipeline_stats_xcom_key)
    if isinstance(pipeline_stats, str):
      pipeline_stats = json.loads(pipeline_stats)
    json_file = \
      combine_all_admin_view_data_to_json(
        seqrun_json=seqrun_json,
        storage_stat_json=storage_stat_json,
        pipeline_stats=pipeline_stats)
    _ = \
      upload_files_to_portal(
        portal_config_file=IGF_PORTAL_CONF,
        file_path=json_file,
        url_suffix='/api/v1/admin_home/update_admin_view_data')
  except Exception as e:
    logging.error(e)
    message = \
      'Failed metadata dump, error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise