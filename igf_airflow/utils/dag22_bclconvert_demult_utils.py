import os, logging, subprocess
from airflow.models import Variable
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, copy_remote_file, check_file_path, read_json_data, get_date_stamp_for_file_name
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file',default_var=None)

log = logging.getLogger(__name__)

def find_seqrun_func(**context):
  try:
    dag_run = context.get('dag_run')
    task_list = ['mark_run_finished',]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      run_status = \
        _check_for_required_files(
          seqrun_path,
          file_list=[
            'RunInfo.xml',
            'RunParameters.xml',
            'SampleSheet.csv',
            'Data/Intensities/BaseCalls',
            'InterOp'])
      if run_status:
        _check_and_load_seqrun_to_db(
          seqrun_id=seqrun_id,
          seqrun_path=seqrun_path,
          dbconf_json_path=DATABASE_CONFIG_FILE)
        seed_status = \
          _check_and_seed_seqrun_pipeline(
            seqrun_id=seqrun_id,
            pipeline_name=context['task'].dag_id,
            dbconf_json_path=DATABASE_CONFIG_FILE)
        if seed_status:
          task_list = ['format_and_split_samplesheet',]
    return task_list
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


def _check_for_required_files(seqrun_path: str, file_list: list) -> bool:
  try:
    for file_name in file_list:
      file_path = os.path.join(seqrun_path, file_name)
      if not os.path.exists(file_path):
        return False
    return True
  except:
    raise


def _check_and_load_seqrun_to_db(
    seqrun_id: str,
    seqrun_path: str,
    dbconf_json_path: str,
    runinfo_file_name: str = 'RunInfo.xml') \
    -> None:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    sra = SeqrunAdaptor(**dbconf)
    sra.start_session()
    run_exists = \
      sra.check_seqrun_exists(seqrun_id)
    if not run_exists:
      runinfo_file = os.path.join(seqrun_path, runinfo_file_name)
      runinfo_data = RunInfo_xml(xml_file=runinfo_file)
      platform_name = runinfo_data.get_platform_number()
      flowcell_id = runinfo_data.get_flowcell_name()
      seqrun_data = [{
        'seqrun_igf_id': seqrun_id,
        'platform_igf_id': platform_name,
        'flowcell_id': flowcell_id }]
      sra.store_seqrun_and_attribute_data(
        data=seqrun_data,
        autosave=True)
    sra.close_session()
  except:
    raise


def _check_and_seed_seqrun_pipeline(
    seqrun_id: str,
    pipeline_name: str,
    dbconf_json_path: str,
    seed_status: str = 'SEEDED',
    seed_table: str ='seqrun',
    no_change_status: str = 'RUNNING') -> bool:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    base = BaseAdaptor(**dbconf)
    base.start_session()
    sra = SeqrunAdaptor(**{'session': base.session})
    seqrun_entry = \
      sra.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_id)
    pa = PipelineAdaptor(**{'session': base.session})
    pa.start_session()
    seed_status = \
      pa.create_or_update_pipeline_seed(
        seed_id=seqrun_entry.seqrun_id,
        pipeline_name=pipeline_name,
        new_status=seed_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    return seed_status
  except:
    raise