from igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db import Collect_seqrun_fastq_to_db
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CollectFastqToDbCollection(IGFBaseProcess):
  '''
  A ehive runnable class for adding fastq files to database
  as the experiment and runs
  '''
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({'file_location':'HPC_PROJECT',
                        'samplesheet_filename':'SampleSheet.csv',
                      })
    
  def run(self):
    try:
      fastq_dir=self.param_required('fastq_dir')
      igf_session_class=self.param_required('igf_session_class')
      model_name=self.param_required('model_name')
      flowcell_id=self.param_required('flowcell_id')
      file_location=self.param('file_location')
      samplesheet_filename=self.param('samplesheet_filename')
      collect_instance=Collect_seqrun_fastq_to_db(fastq_dir=fastq_dir,
                                                  igf_session_class=igf_session_class,
                                                  model_name=model_name,
                                                  flowcell_id=flowcell_id,
                                                  file_location=file_location,
                                                  samplesheet_filename=samplesheet_filename,
                                                  )
      collect_instance.find_fastq_and_build_db_collection()
      self.param('dataflow_params',{'fastq_dir':fastq_dir})
    except:
      raise