from igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db import Collect_seqrun_fastq_to_db
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectFastqToDbCollection(IGFBaseProcess):
  '''
  A ehive runnable class for adding fastq files to database
  as the experiment and runs
  '''
  def param_defaults(self):
    params_dict=super(CollectFastqToDbCollection,self).param_defaults()
    params_dict.\
      update({'file_location':'HPC_PROJECT',
              'samplesheet_filename':'SampleSheet.csv',
              'manifest_name': 'file_manifest.csv',
              'singlecell_tag':'10X',
             })
    return params_dict

  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      fastq_dir = self.param_required('fastq_dir')
      igf_session_class = self.param_required('igf_session_class')
      model_name = self.param_required('model_name')
      flowcell_id = self.param_required('flowcell_id')
      file_location = self.param('file_location')
      samplesheet_filename = self.param('samplesheet_filename')
      manifest_name = self.param_required('manifest_name')
      singlecell_tag = self.param('singlecell_tag')
      collect_instance = \
        Collect_seqrun_fastq_to_db(\
          fastq_dir=fastq_dir,
          session_class=igf_session_class,
          model_name=model_name,
          seqrun_igf_id=seqrun_igf_id,
          flowcell_id=flowcell_id,
          file_location=file_location,
          samplesheet_filename=samplesheet_filename,
          manifest_name=manifest_name,
          singlecell_tag=singlecell_tag)
      collect_instance.\
        find_fastq_and_build_db_collection()
      self.param('dataflow_params',
                 {'fastq_dir':fastq_dir,
                  'manifest_name':manifest_name})
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
        format(\
          self.__class__.__name__,
          e,
          seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      self.post_message_to_ms_team(
          message=message,
          reaction='fail')
      raise