import os
import json
import logging
from airflow.models import Variable
import pandas as pd
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.igfTables import Base
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.igfTables import Collection
from igf_data.igfdb.igfTables import File
from igf_data.igfdb.igfTables import Collection_group
from igf_data.igfdb.igfTables import Collection_attribute
from igf_portal.api_utils import upload_files_to_portal
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.illumina.samplesheet import SampleSheet
from igf_airflow.logging.upload_log_msg import send_log_to_channels

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)

def cleanup_fastq_for_seqrun_func(**context):
  try:
    ## dag_run.conf should have seqrun_id
    dag_run = context.get('dag_run')
    seqrun_id = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
    if seqrun_id is None:
      raise ValueError('seqrun_id not found in dag_run.conf')
    ## get samplesheet from portal
    temp_dir = \
      get_temp_dir()
    samplesheet_file = \
      fetch_samplesheet_for_seqrun_from_portal(
        seqrun_igf_id=seqrun_id,
        igfportal_config=IGF_PORTAL_CONF,
        output_dir=temp_dir)
    ## get list of projects from samplesheet
    project_id_list = \
      get_list_of_projects_from_samplesheet(
        samplesheet_file=samplesheet_file)
    ## cleanup data for the run
    cleanup_all_projects(
      dbconfig_file=DATABASE_CONFIG_FILE,
      seqrun_igf_id=seqrun_id,
      project_id_list=project_id_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def fetch_samplesheet_for_seqrun_from_portal(
      seqrun_igf_id: str,
      igfportal_config: str,
      output_dir: str) \
        -> str:
  try:
    check_file_path(output_dir)
    temp_dir = \
      get_temp_dir()
    seqrun_id_json = \
      os.path.join(
        temp_dir,
        "seqrun_id.json")
    json_data = \
      {"seqrun_id": seqrun_igf_id}
    with open(seqrun_id_json, "w") as fp:
      json.dump(json_data, fp)
    res = \
      upload_files_to_portal(
        url_suffix="/api/v1/raw_seqrun/search_run_samplesheet",
        portal_config_file=igfportal_config,
        file_path=seqrun_id_json,
        verify=False,
        jsonify=False)
    if res.status_code != 200:
      raise ValueError('Failed to get samplesheet from portal')
    data = res.content.decode('utf-8')
    # deal with runs without valid samplesheets
    if "No samplesheet found" in data:
      raise ValueError(
        f"No samplesheet found for seqrun_id: {seqrun_igf_id}")
    data = \
      res.content.decode('utf-8')
    samplesheet_file = \
      os.path.join(
        output_dir,
        'SampleSheet.csv')
    with open(samplesheet_file, 'w') as fp:
      fp.write(data)
    return samplesheet_file
  except Exception as e:
    raise ValueError(
      f"Failed to fetch samplesheet for {seqrun_igf_id} from portal, error: {e}")


def get_list_of_projects_from_samplesheet(
      samplesheet_file: str) \
        -> list:
  try:
    check_file_path(samplesheet_file)
    sa = SampleSheet(samplesheet_file)
    list_of_projects = sa.get_project_names()
    return list_of_projects
  except Exception as e:
    raise ValueError(
      f"Failed to get project info from samplesheet f{samplesheet_file}, error: {e}")


def cleanup_all_projects(
      dbconfig_file: str,
      seqrun_igf_id: str,
      project_id_list: list) \
        -> None:
  try:
    for project_igf_id in project_id_list:
      _ = \
        cleanup_existing_data_for_flowcell_and_project(
          dbconfig_file=dbconfig_file,
          seqrun_igf_id=seqrun_igf_id,
          project_igf_id=project_igf_id)
  except Exception as e:
    raise ValueError(
      f"Failed to cleanup, error: {e}")


def cleanup_existing_data_for_flowcell_and_project(
      dbconfig_file: str,
      seqrun_igf_id: str,
      project_igf_id: str) \
        -> bool:
  try:
    dbparams = \
      read_dbconf_json(dbconfig_file)
    ## get all runs for the project + flowcell
    ra = RunAdaptor(**dbparams)
    ra.start_session()
    records = \
      ra.get_all_run_for_seqrun_igf_id(
        seqrun_igf_id=seqrun_igf_id,
        project_igf_id=project_igf_id)
    ## get flowcell id
    sra = SeqrunAdaptor(**{'session': ra.session})
    flowcell_id = \
      sra.get_flowcell_id_for_seqrun_id(
        seqrun_igf_id=seqrun_igf_id)
    if flowcell_id is None:
      raise ValueError(
        f'Failed to get flowcell for seqrun : {seqrun_igf_id}')
    ## project report partial collection name
    partial_collection_name = \
      f'{project_igf_id}_{flowcell_id}'
    ca = CollectionAdaptor(**{'session': ra.session})
    try:
      # remove collection and files
      # 'demultiplexed_fastq',
      # 'FASTQC_HTML_REPORT',
      # 'FASTQSCREEN_HTML_REPORT'
      collection_list = [
        'demultiplexed_fastq',
        'FASTQC_HTML_REPORT',
        'FASTQSCREEN_HTML_REPORT']
      for entry in records:
        run_igf_id = entry.get('run_igf_id')
        if run_igf_id is None:
          raise ValueError('No run id found')
        for collection_type in collection_list:
          ca.cleanup_collection_and_file_for_name_and_type(
            collection_name=run_igf_id,
            collection_type=collection_type,
            remove_files_on_disk=True,
            autosave=False)
      ## clean up ftp html report entries, not removing files
      collection_list = [
        'FTP_FASTQC_HTML_REPORT',
        'FTP_FASTQSCREEN_HTML_REPORT']
      for entry in records:
        run_igf_id = entry.get('run_igf_id')
        if run_igf_id is None:
          raise ValueError('No run id found')
        for collection_type in collection_list:
          ca.cleanup_collection_and_file_for_name_and_type(
            collection_name=run_igf_id,
            collection_type=collection_type,
            remove_files_on_disk=False,
            autosave=False)
      ## remove demult and multiqc reports on disk and db
      ## using partial collection name
      collection_list = [
        'DEMULTIPLEXING_REPORT_HTML',
        'MULTIQC_HTML_REPORT_KNOWN',
        'MULTIQC_HTML_REPORT_UNDETERMINED']
      for collection_type in collection_list:
        ca.cleanup_collection_and_file_for_name_and_type(
          collection_name=partial_collection_name,
          collection_type=collection_type,
          remove_files_on_disk=True,
          partial_collection_name=True,
          autosave=False)
      ## removing demult reports dir and collection
      collection_list = [
        'DEMULTIPLEXING_REPORT_DIR']
      for collection_type in collection_list:
        ca.cleanup_collection_and_file_for_name_and_type(
          collection_name=partial_collection_name,
          collection_type=collection_type,
          remove_files_on_disk=True,
          partial_collection_name=True,
          cleanup_dirs=True,
          autosave=False)
      ## removing ftp reports files from db, not deleting files
      collection_list = [
        'FTP_DEMULTIPLEXING_REPORT_HTML',
        'FTP_MULTIQC_HTML_REPORT_KNOWN',
        'FTP_MULTIQC_HTML_REPORT_UNDETERMINED']
      for collection_type in collection_list:
        ca.cleanup_collection_and_file_for_name_and_type(
          collection_name=partial_collection_name,
          collection_type=collection_type,
          remove_files_on_disk=False,
          partial_collection_name=True,
          autosave=False)
      ## remove runs
      run_igf_id_list = [
        entry.get('run_igf_id')
          for entry in records
            if entry.get('run_igf_id') is not None]
      ra.delete_runs_from_db(
        run_igf_id_list=run_igf_id_list,
        autosave=False)
      ## commit session
      ra.commit_session()
    except Exception as e:
      ra.rollback_session()
      ra.close_session()
      raise ValueError(
        f'Aborted db and disk clean up. File deletion interrupted due to error: {e}')
    ra.close_session()
    return True
  except Exception as e:
    raise ValueError(f"Failed to cleanup, error: {e}")