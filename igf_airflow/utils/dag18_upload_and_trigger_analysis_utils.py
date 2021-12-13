import yaml
import pandas as pd
from airflow.models import Variable
import logging, os, requests, subprocess, re, shutil, gzip
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import check_file_path, get_temp_dir, copy_local_file, get_datestamp_label
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor

DATABASE_CONFIG_FILE = \
  Variable.get('database_config_file', default_var=None)
SLACK_CONF = \
  Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = \
  Variable.get('ms_teams_conf', default_var=None)
ANALYSIS_LOOKUP_DIR = \
  Variable.get("analysis_lookup_dir", default_var=None)
ANALYSIS_TRIGGER_FILE = \
  Variable.get("analysis_triger_file", default_var=None)
ANALYSIS_LIST = \
    Variable.get("analysis_dag_list", default_var={})

def send_log_and_reset_trigger_file_func(**context):
  try:
    ti = context.get('ti')
    xcom_key = \
      context['params'].get('xcom_key')
    xcom_task = \
      context['params'].get('xcom_task')
    analysis_list = \
      ti.xcom_pull(
        task_ids=xcom_task,
        key=xcom_key)
    df = pd.DataFrame(analysis_list)
    if len(df.index)>0:
      analysis_counts = \
        df.\
          groupby("analysis_type").\
          size().\
          to_dict()
      message = \
        "Triggred following analysis: {0}".format(analysis_counts)
      send_log_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment=message,
        reaction='pass')
    # reset analysis_trigger_file
    pd.DataFrame([]).to_csv(ANALYSIS_TRIGGER_FILE)
  except Exception as e:
    logging.error(e)
    message = \
      'analysis input finding error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def trigger_dag_func(context, dag_run_obj):
    try:
        ti = context.get('ti')
        xcom_key = \
            context['params'].get('xcom_key')
        xcom_task = \
            context['params'].get('xcom_task')
        analysis_name = \
            context['params'].get('analysis_name')
        index = \
            context['params'].get('index')
        analysis_list = \
            ti.xcom_pull(
                task_ids=xcom_task,
                key=xcom_key)
        analysis_detail = \
           get_dag_conf_for_analysis(
               analysis_list=analysis_list,
               analysis_name=analysis_name,
               index=index)
        dag_run_obj.payload = analysis_detail
        return dag_run_obj
        ## FIX for v2
        # trigger_dag = \
        #    TriggerDagRunOperator(
        #        task_id="trigger_dag_{0}_{1}".format(analysis_name, index),
        #        trigger_dag_id=analysis_name,
        #        conf=analysis_detail)
        #return trigger_dag.execute(context=context)
    except Exception as e:
        logging.error(e)
        message = \
        'analysis input finding error: {0}'.\
            format(e)
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            comment=message,
            reaction='fail')
        raise


def get_dag_conf_for_analysis(analysis_list, analysis_name, index):
  try:
    df = pd.DataFrame(analysis_list)
    filter_df = df[df['analysis_name']==analysis_name]
    if index >= len(filter_df.index):
      raise KeyError(
              "Missing key {0} for analysis {1}".\
                format(index, analysis_name))
    if 'analysis_id' not in filter_df.columns:
      raise KeyError("Missing key analysis_id in the analysis_list")
    analysis_detail = \
      filter_df.\
        sort_values('analysis_id').\
        to_dict(orient="records")[index]
    return analysis_detail
  except Exception as e:
    raise ValueError(
            "Failed to fetch analysis details for trigger, error: {0}".\
              format(e))

def get_analysis_ids_from_csv(analysis_trigger_file):
  try:
    check_file_path(analysis_trigger_file)
    data = \
      pd.read_csv(
        analysis_trigger_file,
        header=None,
        sep=",")
    data.columns = [
      "project_igf_id",
      "analysis_name"]
    return data.to_dict(orient="records")
  except Exception as e:
    raise ValueError(
            "Failed to get analysis id from {0}, error {1}".\
              format(analysis_trigger_file, e))


def fetch_analysis_records(analysis_records_list, db_config_file):
  try:
    updated_analysis_records = list()
    errors_list = list()
    db_params = \
      read_dbconf_json(db_config_file)
    aa = AnalysisAdaptor(**db_params)
    aa.start_session()
    for analysis_entry in analysis_records_list:
      project_igf_id = analysis_entry.get("project_igf_id")
      analysis_name = analysis_entry.get("analysis_name")
      if project_igf_id is None or \
         analysis_name is None:
        errors_list.append(
          "No project_igf_id or analysis_name found in list: {0}".\
            format(analysis_entry) )
      else:
        analysis_records = \
          aa.fetch_analysis_record_by_analysis_name_and_project_igf_id(
            analysis_name=analysis_name,
            project_igf_id=project_igf_id,
            output_mode='dataframe')
        if len(analysis_records.index) > 0:
          updated_analysis_records.append({
            "analysis_name": analysis_name,
            "analysis_id": analysis_records["analysis_id"].values[0],
            "analysis_type": analysis_records["analysis_type"].values[0],
            "analysis_description": analysis_records["analysis_description"].values[0] })
        else:
          errors_list.\
            append(
              "No analysis entry found with name: {0} and project: {1}".\
                format(analysis_name, project_igf_id))
    aa.close_session()
    return updated_analysis_records, errors_list
  except Exception as e:
    raise ValueError(
            "Failed to fetch analysis records, error: {0}".\
              format(e))


def find_analysis_to_trigger_dags_func(**context):
  try:
    analysis_limit = \
      context['params'].get('analysis_limit')
    no_trigger_task = \
      context['params'].get('no_trigger_task')
    trigger_task_prefix = \
      context['params'].get('trigger_task_prefix')
    xcom_key = \
      context['params'].get('xcom_key')
    ti = context.get('ti')
    task_list = [no_trigger_task]
    analysis_list = \
      get_analysis_ids_from_csv(
        ANALYSIS_TRIGGER_FILE)
    updated_analysis_records, errors_list = \
      fetch_analysis_records(
        analysis_list,
        DATABASE_CONFIG_FILE)
    if len(errors_list) > 0:
      message = \
        "Foung {0} errors while seeding analysis. Ignoring errors: {1}".\
          format(len(errors_list), " \n".join(errors_list))
      send_log_to_channels(
        slack_conf=SLACK_CONF,
        ms_teams_conf=MS_TEAMS_CONF,
        task_id=context['task'].task_id,
        dag_id=context['task'].dag_id,
        comment=message,
        reaction='fail')
    if len(updated_analysis_records) > 0:
      task_df = \
        pd.DataFrame(
          updated_analysis_records)
      if "analysis_type" not in task_df.columns:
        raise KeyError("analysis_type coulmn not found in updated analysis records")
      for analysis_type, t_data in task_df.groupby("analysis_type"):
        if analysis_type not in ANALYSIS_LIST.keys():
          raise KeyError(
            "Missing analysis type {0} in the variable list: {1}".\
              format(analysis_type, ANALYSIS_LIST.keys()))
        analysis_count = len(t_data.index)
        if analysis_count >= analysis_limit:
          raise ValueError(
            "Need to increase analysis_limit from {0} to {1}".\
              format(analysis_limit, analysis_count))
        for i in range(0, analysis_count):
          task_list.\
            append(
              "{0}_{1}_{2}".\
                format(
                  trigger_task_prefix,
                  analysis_type,
                  i))
      ti.xcom_push(
        key=xcom_key,
        value=updated_analysis_records)                                         # xcom push only if analysis is present
    return task_list
  except Exception as e:
    logging.error(e)
    message = \
      'analysis input finding error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise


def parse_analysis_yaml_and_load_to_db(analysis_yaml_file, db_config_file):
  try:
    check_file_path(analysis_yaml_file)
    with open(analysis_yaml_file, 'r') as fp:
      yaml_data = yaml.safe_load(fp)
    df = pd.DataFrame(yaml_data)
    if "project_igf_id" not in df.columns or \
       "analysis_name" not in df.columns or \
       "analysis_type" not in df.columns or \
       "analysis_description" not in df.columns:
      raise ValueError(
              "Missing required column in {0}".\
                format(df.columns))
    if len(df.index) > 0:
      db_params = \
        read_dbconf_json(db_config_file)
      aa = AnalysisAdaptor(**db_params)
      aa.start_session()
      try:
        aa.store_analysis_data(df, autosave=False)
        fa = FileAdaptor(**{'session': aa.session})
        fa.store_file_data(
          data=[{'file_path': analysis_yaml_file}],
          autosave=False)
        aa.commit_session()
        aa.close_session()
      except:
        aa.rollback_session()
        aa.close_session()
        raise
  except Exception as e:
    raise ValueError(
            "Failed to load analysis {0}, error: {1}".\
              format(analysis_yaml_file, e))


def load_analysis_design_func(**context):
  try:
    task_index = \
      context['params'].get('task_index')
    load_design_xcom_key = \
      context['params'].get('load_design_xcom_key')
    load_design_xcom_task = \
      context['params'].get('load_design_xcom_task')
    ti = context.get('ti')
    analysis_files = \
      ti.xcom_pull(
        task_ids=load_design_xcom_task,
        key=load_design_xcom_key)
    analysis_file = \
      analysis_files.get(task_index)
    if analysis_file is None:
      raise ValueError("No analysis file list found")
    check_file_path(analysis_file)
    parse_analysis_yaml_and_load_to_db(
      analysis_file,
      DATABASE_CONFIG_FILE)
  except Exception as e:
    logging.error(e)
    message = \
      'analysis input loading error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise

def find_all_analysis_yaml_files(analysis_design_path):
  try:
    check_file_path(analysis_design_path)
    all_yaml_files = list()
    for root, _, files in os.walk(analysis_design_path):
      for f in files:
        if f.endswith(".yaml") or \
           f.endswith(".yml"):
          file_path = \
            os.path.join(root, f)
          all_yaml_files.\
            append(file_path)
    return all_yaml_files
  except Exception as e:
    raise ValueError(
            "Failed to list analysis files in {0}, error: {1}".\
              format(analysis_design_path, e))

def get_new_file_list(all_files, db_config_file):
  try:
    filtered_list = list()
    if isinstance(all_files, list) and \
       len(all_files) > 0:
      db_params = \
        read_dbconf_json(db_config_file)
      fa = FileAdaptor(**db_params)
      fa.start_session()
      for f in all_files:
        file_exists = \
          fa.check_file_records_file_path(f)
        if not file_exists:
          filtered_list.append(f)
    return filtered_list
  except Exception as e:
    raise ValueError(
            "Failed to check db for existing file, error: {0}".format(e))

def find_analysis_designs_func(**context):
  try:
    load_analysis_task_prefix = \
      context['params'].get('load_analysis_task_prefix')
    load_task_limit = \
      context['params'].get('load_task_limit')
    load_design_xcom_key = \
      context['params'].get('load_design_xcom_key')
    no_task_name = \
      context['params'].get('no_task_name')
    ti = context.get('ti')
    all_files = \
      find_all_analysis_yaml_files(
          ANALYSIS_LOOKUP_DIR)
    new_files = \
      get_new_file_list(
          all_files,
          DATABASE_CONFIG_FILE)
    if len(new_files) > load_task_limit:
      new_files = new_files[0: load_task_limit]                                 # loading only 20 files, its ok as we never going to get that many
    if len(new_files) > 0:
      task_list = [
        "{0}_{1}".format(load_analysis_task_prefix, i)
          for i in range(0, len(new_files))]
      ti.xcom_push(
        key=load_design_xcom_key,
        value=new_files)
    else:
      task_list = [no_task_name]
    return task_list
  except Exception as e:
    logging.error(e)
    message = \
      'analysis input finding error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')
    raise