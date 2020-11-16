import pandas as pd
import os,subprocess,json
from igf_data.utils.fileutils import copy_remote_file,get_temp_dir,remove_dir,copy_local_file
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

class SeqrunFileFactory(IGFBaseJobFactory):
  def param_defaults(self):
    params_dict=super(SeqrunFileFactory,self).param_defaults()
    params_dict.update({ 
             'seqrun_md5_type':'ILLUMINA_BCL_MD5',
             'seqrun_server':None,
             'hpc_location':'HPC_PROJECT',
             'db_file_location_label':'location',
             'db_file_path_label':'file_path',
             'seqrun_user':None,
             'seqrun_source':None,
             'seqrun_local_dir':None,
           })
    return params_dict


  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      seqrun_local_dir = self.param_required('seqrun_local_dir')
      seqrun_source = self.param_required('seqrun_source')
      seqrun_server = self.param_required('seqrun_server')
      seqrun_user = self.param_required('seqrun_user')
      igf_session_class = self.param_required('igf_session_class')
      seqrun_md5_type = self.param_required('seqrun_md5_type')
      hpc_location = self.param_required('hpc_location')
      db_file_location_label = self.param_required('db_file_location_label')
      db_file_path_label = self.param_required('db_file_path_label')

      seqrun_path = os.path.join(seqrun_source,seqrun_igf_id)                   # get new seqrun path
      seqrun_server_login = \
        '{0}@{1}'.format(seqrun_user, seqrun_server)                            # get destination path
      subprocess.\
        check_call([
          'ssh', 
          seqrun_server_login,
          'ls', 
          seqrun_path])                                                         # check remote file
      ca = \
        CollectionAdaptor(**{'session_class':igf_session_class})                # get the md5 list from db
      ca.start_session()
      files = \
        ca.get_collection_files(
          collection_name=seqrun_igf_id,
          collection_type=seqrun_md5_type)                                      # fetch file collection
      files=files.to_dict(orient='records')
      ca.close_session()
      if len(files)>1:
        raise ValueError(
                'sequencing run {0} has more than one md5 json file'.\
                  format(seqrun_igf_id))
      if len(files)==0:
        raise ValueError(
                'sequencing run {0} does not have any md5 json file'.\
                  format(seqrun_igf_id))
      md5_json_location = \
        files[0][db_file_location_label]
      md5_json_path = \
        files[0][db_file_path_label]
      if md5_json_location !=hpc_location:
        temp_dir = \
          get_temp_dir(work_dir=os.getcwd())                                    # create a temp directory
        destination_path = \
          os.path.join(
            temp_dir,
            os.path.basename(md5_json_path))                                    # get destination path for md5 file
        copy_remote_file(
          source_path=md5_json_path,
          destinationa_path=destination_path,
          source_address=seqrun_server_login)                                   # copy remote file to local disk
        md5_json_path = destination_path                                        # set md5 json filepath
      ## TO DO: check and reset json file removing existing files with same size
      ## also add samplesheet in the output file, if its not present
      local_seqrun_path = \
        os.path.join(
          seqrun_local_dir,
          seqrun_igf_id)
      self.compare_and_reset_seqrun_list(
        json_path=md5_json_path,
        local_seqrun_dir=local_seqrun_path)                                     # reset json file, if existing files have same size
      with open(md5_json_path) as json_data:
        md5_json = json.load(json_data)                                         # read json data, get all file and md5 from json file
      self.param('sub_tasks',md5_json)                                          # seed dataflow
      remove_dir(temp_dir)                                                      # remove temp dir when its not required
      
      message = \
        'seqrun: {0}, seeded {1} files for copy'.\
          format(seqrun_igf_id,len(md5_json))
      self.warning(message)
      self.post_message_to_slack(message,reaction='pass')
      self.comment_asana_task(
            task_name=seqrun_igf_id,
            comment=message)
      self.post_message_to_ms_team(
             message=message,
             reaction='pass')
    except Exception as e:
      message = \
        'Error in {0}: {1}, seqrun: {2}'.\
          format(
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      self.comment_asana_task(
             task_name=seqrun_igf_id,
             comment=message)
      raise

  @staticmethod
  def compare_and_reset_seqrun_list(
        json_path,local_seqrun_dir,samplesheet_name="SampleSheet.csv"):
    """
    A function for comparing the size of existing files and
    overwriting the json list only with non-existing files and
    samplesheet 
    """
    try:
      if os.path.exists(local_seqrun_dir):
        with open(json_path,'r') as jp:
          json_data = json.load(jp)
        samplesheet_record = \
          [i for i in json_data
            if i.get('seqrun_file_name') == (samplesheet_name)]
        if len(samplesheet_record) == 0:
          raise ValueError(
                  'No samplesheet found in {0}'.\
                    format(json_path))
        file_size_list = list()
        for root_path,_,files in os.walk(local_seqrun_dir):
          for file_name in files:
            file_path = os.path.join(root_path,file_name)
            file_size = os.path.getsize(file_path)
            file_rel_path = \
              os.path.relpath(file_path,local_seqrun_dir)
            file_size_list.\
              append({
                "seqrun_file_name":file_rel_path,
                "file_size":file_size})
        if len(file_size_list) > 0:
          remote_files = \
            pd.read_json(json_path).\
              set_index('seqrun_file_name')
          local_files = \
            pd.DataFrame(file_size_list).\
              set_index('seqrun_file_name')
          merged_data = \
            remote_files.\
              join(
                local_files,
                how='left',
                rsuffix='_local')                                               # left df join
          merged_data.\
            fillna(0,inplace=True)                                              # replace NAN with 0
          merged_data['t'] = \
            pd.np.where(
              merged_data['file_size'] != merged_data['file_size_local'],
              'DIFF','')
          target_files = \
            merged_data[merged_data['t']=='DIFF'].\
            reset_index()[[
              'seqrun_file_name',
              'file_size',
              'file_md5']]
          if len(target_files.index) == 0:
            target_files = \
              pd.DataFrame(samplesheet_record)                                  # add samplesheet if no records
          tmp_dir = get_temp_dir()
          tmp_json = \
            os.path.join(
              tmp_dir,
              os.path.basename(json_path))
          with open(tmp_json,'w') as jp:
            json.dump(target_files.to_dict(orient='records'),jp)
          copy_local_file(
            tmp_json,
            json_path,
            force=True)
    except Exception as e:
      raise ValueError(
        'Failed to compare the existing file for json: {0}, error: {1}'.\
          format(json_path,e))