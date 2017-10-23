import os, subprocess, json
from igf_data.utils.fileutils import copy_remote_file, get_temp_dir, remove_dir
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

class SeqrunFileFactory(IGFBaseJobFactory):
  def param_defaults(self):
    params_dict=super(IGFBaseJobFactory,self).param_defaults()
    params_dict.update({ 
             'seqrun_md5_type':'ILLUMINA_BCL_MD5',
             'seqrun_server':None,
             'hpc_location':'HPC_PROJECT',
             'db_file_location_label':'location',
             'db_file_path_label':'file_path',
             'seqrun_user':None,
             'seqrun_source':None,
           })
    return params_dict


  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_source=self.param_required('seqrun_source')
      seqrun_server=self.param_required('seqrun_server')
      seqrun_user=self.param_required('seqrun_user')
      igf_session_class=self.param_required('igf_session_class')
      seqrun_md5_type=self.param_required('seqrun_md5_type')
      hpc_location=self.param_required('hpc_location')
      db_file_location_label=self.param_required('db_file_location_label')
      db_file_path_label=self.param_required('db_file_path_label')

      seqrun_path=os.path.join(seqrun_source,seqrun_igf_id)                     # get new seqrun path
      seqrun_server_login='{0}@{1}'.format(seqrun_user, seqrun_server)          # get destination path
      subprocess.check_call(['ssh', 
                             seqrun_server_login,
                             'ls', 
                             seqrun_path])                                      # check remote file
      ca=CollectionAdaptor(**{'session_class':igf_session_class})               # get the md5 list from db
      ca.start_session()
      files=ca.get_collection_files(collection_name=seqrun_igf_id,
                                    collection_type=seqrun_md5_type)            # fetch file collection
      files=files.to_dict(orient='records')
      ca.close_session()

      if len(files)>1:
        raise ValueError('sequencing run {0} has more than one md5 json file'.\
                         format(seqrun_igf_id))

      if len(files)==0:
        raise ValueError('sequencing run {0} does not have any md5 json file'.\
                         format(seqrun_igf_id))
      
      md5_json_location=files[0][db_file_location_label]
      md5_json_path=files[0][db_file_path_label]
      if md5_json_location !=hpc_location:
        temp_dir=get_temp_dir(work_dir=os.getcwd())                             # create a temp directory
        destination_path=os.path.join(temp_dir,os.path.basename(md5_json_path)) # get destination path for md5 file
        copy_remote_file(source_path=md5_json_path,
                         destinationa_path=destination_path,
                         source_address=seqrun_server_login)                    # copy remote file to local disk
        md5_json_path=destination_path                                          # set md5 json filepath

      with open(md5_json_path) as json_data:
            md5_json=json.load(json_data)                                       # read json data, get all file and md5 from json file
      self.param('sub_tasks',md5_json)                                          # seed dataflow
      remove_dir(temp_dir)                                                      # remove temp dir when its not required
      
      message='seqrun: {1}, seeded {0} files for copy'.format(seqrun_igf_id, \
                                                              len(md5_json))
      self.warning(message)
      self.post_message_to_slack(message,reaction='pass')
      self.comment_asana_task(task_name=seqrun_igf_id, \
                              comment=message)

    except Exception as e:
      message='Error in {0}: {1}, seqrun: {2}'.format(self.__class__.__name__,\
                                                      e,\
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      self.comment_asana_task(task_name=seqrun_igf_id, \
                              comment=message)
      raise