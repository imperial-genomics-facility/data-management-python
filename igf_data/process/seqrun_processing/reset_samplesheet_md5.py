import os
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana


class Reset_samplesheet_md5:
  '''
  A class for modifying samplesheet md5 for seqrun data processing
  '''
  def __init__(self,seqrun_path,seqrun_igf_list,dbconfig_file,
               json_collection_type='ILLUMINA_BCL_MD5',log_slack=True,
               log_asana=True,slack_config=None,asana_project_id=None,
               asana_config=None):
    '''
    :param seqrun_path: A directory path for sequencing run home
    :param seqrun_igf_list: A file path listing sequencing runs to reset
    :param dbconfig_file: A file containing the database configuration
    :param json_collection_type: A collection type for md5 json file lookup, default ILLUMINA_BCL_MD5
    :param log_slack: A boolean flag for toggling Slack messages, default True
    :param log_asana: Aboolean flag for toggling Asana message, default True
    :param slack_config: A file containing Slack tokens, default None
    :param asana_config: A file containing Asana tokens, default None
    :param asana_project_id: A numeric Asana project id, default is None
    '''
    try:
      self.seqrun_path=seqrun_path
      self.seqrun_igf_list=self._read_seqrun_list(seqrun_igf_list)
      self.json_collection_type=json_collection_type
      self.log_slack=log_slack
      self.log_asana=log_asana
      dbparams = read_dbconf_json(dbconfig_file)
      base=BaseAdaptor(**dbparams)
      self.session_class = base.get_session_class()                             # add session class to instance
      if log_slack and slack_config is None:
        raise ValueError('Missing slack config file')
      elif log_slack and slack_config:
        self.igf_slack = IGF_slack(slack_config)                                # add slack object

      if log_asana and \
         (asana_config is None or \
          asana_project_id is None):
        raise ValueError('Missing asana config file or asana project id')
      elif log_asana and asana_config and asana_project_id:
        self.igf_asana=IGF_asana(asana_config,asana_project_id)                 # add asana object
    except:
      raise

  @staticmethod
  def _read_seqrun_list(seqrun_igf_list):
    '''
    A static method for reading list of sequencing run ids from a n input file 
    to a list
    :param seqrun_igf_list: A file containing the sequencing run ids
    :return list: A list of seqrun ids from the input file
    '''
    try:
      if not os.path.exists(seqrun_igf_list):
        raise IOError('File {0} not found'.format(seqrun_igf_list))

      seqrun_ids=list()                                                         # define an empty list of seqrun ids
      with open(seqrun_igf_list,'r') as fp:
        seqrun_ids=[i.strip() for i in fp]                                      # add seqrun ids to the list
      return seqrun_ids
    except:
      raise