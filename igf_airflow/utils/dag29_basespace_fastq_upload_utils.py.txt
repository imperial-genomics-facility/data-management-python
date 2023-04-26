import os
import re
import sys
import logging
import pandas as pd
from typing import Tuple
from airflow.models import Variable
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Project
from igf_data.igfdb.igfTables import Sample
from igf_data.igfdb.igfTables import Experiment
from igf_data.igfdb.igfTables import Collection
from igf_data.igfdb.igfTables import Collection_group
from igf_data.igfdb.igfTables import File
from igf_data.igfdb.igfTables import Pipeline
from igf_data.igfdb.igfTables import Pipeline_seed
from igf_data.igfdb.igfTables import Seqrun
from igf_data.igfdb.igfTables import Run
from igf_data.igfdb.igfTables import Run_attribute
from igf_airflow.logging.upload_log_msg import send_log_to_channels

log = logging.getLogger(__name__)
## import Basespace API
## don't change it for now

sys.path.append('/project/tgu/software/basespace-python-sdk/src')
sys.path.append('/project/tgu/software/igf-basespace/')
try:
  from BaseSpacePy.api.BaseSpaceAPI import BaseSpaceAPI
except:
  log.error("Failed to load basespace library")
  raise

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)

def upload_project_fastq_to_basespace_func(**context):
  try:
    dag_run = context.get('dag_run')
    project_name = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('project_name') is not None:
      project_name = \
        dag_run.conf.get('project_name')
    if project_name is None:
      raise ValueError('project_name not found in dag_run.conf')
    upload_project_data_to_basespace(
      dbconf_file=DATABASE_CONFIG_FILE,
      project_name=project_name)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=f"finished uploading data for project {project_name} to Basespace",
      reaction='pass')
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

def upload_project_data_to_basespace(
      dbconf_file: str,
      project_name: str) -> None:
  try:
    sample_info = \
      collect_all_data_for_project_and_process_for_basespace_upload(
        dbconf_file=dbconf_file,
        project_name=project_name)
    create_new_basespace_project_and_upload_fastq(
      project_name=project_name,
      sample_data_list=sample_info)
  except Exception as e:
    raise ValueError(
      f"Failed Basespace upload for project {project_name}, error: {e}")


def collect_all_data_for_project_and_process_for_basespace_upload(
      dbconf_file: str,
      project_name: str,
      read_length: int = 150)-> list:
  '''
  A function for fetching fastq data from db and upload to basespace

  :param dbconf_file: A dbconfig file
  :param project_name: A project name string
  :returns: A list of sample info with fastq files for basespace upload
  '''
  try:
    dbparam = \
      read_dbconf_json(dbconf_file)
    base = \
      BaseAdaptor(**dbparam)
    base.start_session()
    query = \
    base.\
      session.\
      query(\
        Project.project_igf_id,
        Sample.sample_igf_id,
        Experiment.experiment_igf_id,
        Run.run_igf_id,
        Run_attribute.attribute_name,
        Run_attribute.attribute_value,
        Seqrun.flowcell_id,
        Collection.name,
        Collection.type,
        File.file_path).\
      join(Sample, Project.project_id==Sample.project_id).\
      join(Experiment, Sample.sample_id==Experiment.sample_id).\
      join(Run, Experiment.experiment_id==Run.experiment_id).\
      join(Run_attribute, Run.run_id==Run_attribute.run_id).\
      join(Seqrun, Seqrun.seqrun_id==Run.seqrun_id).\
      join(Collection, Collection.name==Run.run_igf_id).\
      join(Collection_group, Collection.collection_id==Collection_group.collection_id).\
      join(File, File.file_id==Collection_group.file_id).\
      filter(Run_attribute.attribute_name=='R1_READ_COUNT').\
      filter(Collection.type=='demultiplexed_fastq').\
      filter(Collection.table=='run').\
      filter(Project.project_igf_id==project_name)
    records = \
      base.fetch_records(
        query=query,
        output_mode='dataframe')
    base.close_session()
    if len(records.index) == 0:
      raise ValueError(
        f"No Fastq records found in db for {project_name} and {flowcell_id}")
    sample_info = list()
    for sample_id, s_data in records.groupby('sample_igf_id'):
      for flowcell_id, f_data in s_data.groupby('flowcell_id'):
        sample_entry = \
          dict()
        fastq_paths = \
          f_data['file_path'].values.tolist()
        ## sorry, can't upload Index fastqs
        fastq_pattern = \
          re.compile('\S+_R[1,2]_001.fastq.gz')
        fastq_paths = [
          f for f in fastq_paths
            if re.match(fastq_pattern, f)]
        sample_entry = {
          'sample_name': str(sample_id),
          'flowcell_id': str(flowcell_id),
          'fastq_path': fastq_paths,
          'read_count': str(f_data['attribute_value'].values[0]),
          'read_length': read_length}
        sample_info.append(sample_entry)
    if len(sample_info) == 0:
      raise ValueError(
        f"No sample info records found for {project_name}")
    return sample_info
  except Exception as e:
    raise ValueError(
      f"Failed to get fastqs for Basespace upload for ptoject {project_name}, error: {e}")


def create_new_basespace_project_and_upload_fastq(
      project_name: str,
      sample_data_list: list) -> None:
  '''
  A function for uploading afstq files to basespace after creating a new project

  :param project_name: A project name
  :param sample_data_list: Sample data list containing following information
    * sample_name (str)
    * read_count  (str)
    * read_length (str)
    * flow_cell_id (str)
    * fastq_path (list)
  :retruns: None
  '''
  try:
    myAPI = BaseSpaceAPI()
    project = myAPI.createProject(project_name)                                 # create new project
    appResults = \
      project.\
        createAppResult(
          myAPI,
          "FastqUpload",
          "uploading project data",
          appSessionId='')                                                      # create new app results for project
    myAppSession = appResults.AppSession                                        # get app session
    __create_sample_and_upload_data(
      api=myAPI,
      appSessionId=myAppSession.Id,
      appSession=myAppSession,
      project_id=project.Id,
      sample_data_list=sample_data_list)                                                                           # upload fastq
    myAppSession.setStatus(myAPI,'complete',"finished file upload")             # mark app session a complete
  except Exception as e:
    if myAppSession and \
       len(project.getAppResults(myAPI,statuses=['Running']))>0:
      myAppSession.setStatus(myAPI,'complete',"failed file upload")             # comment for failed jobs
    raise ValueError(
      f"Failed to upload files to Basespace, error: {e}")


def __create_sample_and_upload_data(api,appSessionId,appSession,project_id,sample_data_list):
  '''
  An internal function for file upload

  :param api: An api instance
  :param appSessionId: An active appSessionId
  :param appSession: An active app session
  :param project_id: An existing project_id
  :param sample_data_list: Sample data information
  :returns: None
  '''
  try:
    flowcell_count = \
      pd.DataFrame(sample_data_list)['flowcell_id'].drop_duplicates().count()
    sample_number = 0
    read_pattern = re.compile(r'(\S+)_(S\d+_.*\.fastq.gz)')
    for entry in sample_data_list:
      sample_number += 1
      sample_name = entry.get('sample_name')
      read_count = entry.get('read_count')
      files = entry.get('fastq_path')
      read_length = entry.get('read_length')
      flowcell_id = entry.get('flowcell_id')
      appSession.\
        setStatus(
        api,
        'running',
        'uploading {0} of {1} samples'.\
          format(
            sample_number,
            len(sample_data_list))
        )                                                                       # comment on running app session
      sample = \
        api.createSample(
          Id=project_id,
          name=sample_name,
          experimentName=flowcell_id,
          sampleNumber=sample_number,
          sampleTitle=sample_name,
          readLengths=[read_length,read_length],
          countRaw=read_count,
          countPF=read_count,
          appSessionId=appSessionId)                                           # create sample
      for local_path in files:
        fileName = os.path.basename(local_path)
        if flowcell_count > 1:                                                 # change file name if more than one flowcells are present
          if not re.match(read_pattern,fileName):
            raise ValueError('Incorrect fastq name: {0}'.format(fileName))
          (s_id,s_body) = re.match(read_pattern,fileName).groups()
          remote_file_name = '{0}-{1}_{2}'.format(s_id,flowcell_id,s_body)
        else:
           remote_file_name = fileName
        file = \
          api.sampleFileUpload(
            Id=sample.Id,
            localPath=local_path,
            fileName=remote_file_name,
            directory='/FastqUpload/{0}/'.format(sample_name),
            contentType='application/x-fastq')                                      # upload file to sample
  except:
    raise