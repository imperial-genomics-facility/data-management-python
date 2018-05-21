import os, warnings, json
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.fileutils import calculate_file_checksum,move_file
from igf_data.igfdb.igfTables import File

class Reset_samplesheet_md5:
  '''
  A class for modifying samplesheet md5 for seqrun data processing
  '''
  def __init__(self,seqrun_path,seqrun_igf_list,dbconfig_file,clean_up=True,
               json_collection_type='ILLUMINA_BCL_MD5',log_slack=True,
               log_asana=True,slack_config=None,asana_project_id=None,
               asana_config=None,samplesheet_name='SampleSheet.csv'):
    '''
    :param seqrun_path: A directory path for sequencing run home
    :param seqrun_igf_list: A file path listing sequencing runs to reset
    :param dbconfig_file: A file containing the database configuration
    :param clean_up: Clean up input file once its processed, default True
    :param json_collection_type: A collection type for md5 json file lookup, default ILLUMINA_BCL_MD5
    :param log_slack: A boolean flag for toggling Slack messages, default True
    :param log_asana: Aboolean flag for toggling Asana message, default True
    :param slack_config: A file containing Slack tokens, default None
    :param asana_config: A file containing Asana tokens, default None
    :param asana_project_id: A numeric Asana project id, default is None
    :param samplesheet_name: Name of the samplesheet file, default SampleSheet.csv
    '''
    try:
      self.seqrun_path=seqrun_path
      self.seqrun_igf_list=seqrun_igf_list
      self.json_collection_type=json_collection_type
      self.log_slack=log_slack
      self.log_asana=log_asana
      self.clean_up=clean_up
      self.samplesheet_name=samplesheet_name
      dbparams = read_dbconf_json(dbconfig_file)
      self.base_adaptor=BaseAdaptor(**dbparams)
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

  def _get_samplesheet_md5(self,seqrun_igf_id):
    '''
    An internal method for calculating md5 value for updated samplesheet file
    :param seqrun_igf_id: A string of seqrun_igf_id
    :return string: MD5 value of the samplesheet.csv file
    '''
    try:
      samplesheet_path=os.path.join(self.seqrun_path,
                                    seqrun_igf_id,
                                    self.samplesheet_name)
      if not os.path.exists(samplesheet_path):
        raise IOError('Samplesheet not found for seqrun {0}'.\
                      format(seqrun_igf_id))
      return calculate_file_checksum(filepath=samplesheet_path,
                                     hasher='md5')
    except:
      raise

  @staticmethod
  def _get_updated_json_file(json_file_path,samplesheet_md5,samplesheet_name,
                             file_field='seqrun_file_name',md5_field='file_md5'):
    '''
    A static method for checking samplesheet md5 value in json file and create
    a new copy of json file with updated md5, if samplesheet has changed
    :param json_file_path: A file path for seqrun md5 json file
    :param samplesheet_md5: A md5 value for samplesheet file
    :param samplesheet_name: Name of the samplesheet file
    :param file_field: A keyword for filename loop up in json file, default seqrun_file_name
    :param md5_field: A keyword for md5 value look up in json file, default file_md5
    :returns A string filepath if samplesheet has been updated or None
    '''
    try:
      if not os.path.exists(json_file_path):
        raise IOError('Json md5 file {0} not found'.format(json_file_path))

      create_new_file=False                                                     # don't create new json by default
      json_data=list()
      with open(json_file_path,'r') as jp:
        json_data=json.load(jp)                                                 # load data from json file

      for json_row in json_data:
        if json_row[file_field]==samplesheet_name and \
           json_row[md5_field]!=samplesheet_md5:
          json_row[md5_field]=samplesheet_md5                                   # update json data with new md5
          create_new_file=True                                                  # create new json if md5 values are not matching
          break;                                                                # stop file look up

      if create_new_file:
        temp_dir=get_temp_dir()
        json_file_name=os.path.basename(json_file_path)                         # get original json filename
        temp_json_file=os.path.join(temp_dir,
                                    json_file_name)                             # get temp file path
        with open(temp_json_file,'w') as jwp:
          json.dump(json_data,jwp,indent=4)                                     # write data to temp file
        return temp_json_file                                                   # return file path
      else:
        return None                                                             # return none
    except:
      raise

  def run(self):
    '''
    A method for resetting md5 values in the samplesheet json files for all seqrun ids
    '''
    try:
      db_connected=False
      seqrun_list=self._read_seqrun_list(self.seqrun_igf_list)                  # fetch list of seqrun ids from input file
      if len(seqrun_list)>0:
        base=self.base_adaptor
        base.start_session()                                                    # connect to database
        db_connected=True
        ca=CollectionAdaptor(**{'session':base.session})                        # connect to collection table
        fa=FileAdaptor(**{'session':base.session})                              # connect to file table
        for seqrun_id in seqrun_list:
          try:
            files_data=ca.get_collection_files(collection_name=seqrun_id,
                                               collection_type=self.json_collection_type,
                                               output_mode='one_or_none')       # check for existing md5 json file in db
            # TO DO: skip seqrun_id if pipeline is still running
            if files_data is not None:
              json_file_path=[element.file_path 
                                for element in files_data 
                                  if isinstance(element, File)][0]              # get md5 json file path from sqlalchemy collection results
              samplesheet_md5=self._get_samplesheet_md5(seqrun_id)              # get md5 value for new samplesheet file
              new_json_path=self._get_updated_json_file(json_file_path,
                                                        samplesheet_md5,
                                                        self.samplesheet_name)  # get updated md5 json file if samplesheet has been changed
              if new_json_path is not None:
                new_json_file_md5=calculate_file_checksum(filepath=new_json_path,
                                                          hasher='md5')
                fa.update_file_table_for_file_path(file_path=json_file_path,
                                                   tag='md5',
                                                   value=new_json_file_md5,
                                                   autosave=False)              # update json file md5 in db, don't commit yet
                move_file(source_path=new_json_path,
                          destinationa_path=json_file_path,
                          force=True)                                           # overwrite json file
                base.commit_session()                                           # save changes in db
                message='Setting new Samplesheet infor for run {0}'.\
                        format(seqrun_id)
                if self.log_slack:
                  self.igf_slack.post_message_to_channel(message, 
                                                         reaction='pass')       # send log to slack
                if self.log_asana:
                  self.igf_asana.comment_asana_task(task_name=seqrun_id,
                                                    notes=message)              # send log to asana
              else:
                message='no change in samplesheet for seqrun {0}'.format(seqrun_id)
                warnings.warn(message)
                if self.log_slack:
                  self.igf_slack.post_message_to_channel(message, reaction='pass')
            else:
              message='No md5 json file found for seqrun_igf_id: {0}'.\
                      format(seqrun_id)
              warnings.warn(message)                                            # not raising any exception if seqrun id is not found
              if self.log_slack:
                self.igf_slack.post_message_to_channel(message, reaction='fail')
          except Exception as e:
            base.rollback_session()
            message='Failed to update  json file for seqrun id {0}, error : {1}'.\
                    format(seqrun_id,e)
            warnings.warn(message)
            if self.log_slack:
              self.igf_slack.post_message_to_channel(message, reaction='fail')
        base.close_session()                                                    # close db connection
        if self.clean_up:
          self._clear_seqrun_list(self.seqrun_igf_list)                         # clear input file
      else:
        if self.log_slack:
          message='No new seqrun id found for changing samplesheet md5'
          warnings.warn(message)
          if self.log_slack:
            self.igf_slack.post_message_to_channel(message, reaction='sleep')
    except:
      if db_connected:
        base.rollback_session()
        base.close_session()
      raise


  @staticmethod
  def _clear_seqrun_list(seqrun_igf_list):
    '''
    A static method for clearing the seqrun list file
    :param seqrun_igf_list: A file containing the sequencing run ids
    '''
    try:
      if not os.path.exists(seqrun_igf_list):
        raise IOError('File {0} not found'.format(seqrun_igf_list))

      with open(seqrun_igf_list,'w') as fwp:
        fwp.write('')                                                           # over write seqrun list file
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
