import os,subprocess
from ehive.runnable.IGFBaseJobFactory import IGFBaseJobFactory
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

class SeqrunFileFactory(IGFBaseJobFactory):
  def param_defaults(seld):
    return { 'log_slack':True, 
             'log_asana':True,
             'seqrun_md5_type':'ILLUMINA_BCL_MD5'
           } 


  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_source=self.param_required('seqrun_source')
      seqrun_server=self.param_required('seqrun_server')
      igf_session_class=self.param_required('igf_session_class')
      seqrun_md5_type=self.param_required('seqrun_md5_type')

      seqrun_path=os.path.join(seqrun_source,seqrun_igf_id) # get new seqrun path
      # check for remote dir
      subprocess.check_call(['ssh', '{0}@{1}'.format(seqrun_user, seqrun_server), 'ls', seqrun_path], stderror=None, stdout=None)
      # get the md5 list from db
      ca=CollectionAdaptor(**{'session_class':igf_session_class})
      ca.start_session()
      files=ca.get_collection_files(collection_name=seqrun_igf_id,collection_type=seqrun_md5_type)
      files=files.to_dict(orient='records')

      if len(files)>1:
        raise ValueError('sequencing run {0} has more than one md5 json file'.format(seqrun_igf_id))
      if len(files)==0:
        raise ValueError('sequencing run {0} doesn't have any md5 json file'.format(seqrun_igf_id))
      
      md5_json_location=files[0]['location']
      md5_json_path=files[0]['file_path']
      
      # get all file and md5 from json file
      # seed dataflow

    
    except Exceptiona as e:
      message='Error in {0}: {1}'.format(self.__class__.__name__, e)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')
      raise
        
