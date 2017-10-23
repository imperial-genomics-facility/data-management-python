#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.igfdb.platformadaptor import PlatformAdaptor

class CheckAndProcessSampleSheet(IGFBaseProcess):
  '''
  A class for checking and processing samplesheet
  creates a re-formated samplesheet removing the 10X samples
  and converting index barcodes based on flowcell rules table
  '''
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
        'index2_label':'index_2',
        'revcomp_label':'REVCOMP',
        '10X_label':'10X'
      })
    return params_dict
  
  
  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_local_dir=self.param_required('seqrun_local_dir')
      base_work_dir=self.param_required('base_work_dir')
      samplesheet_filename=self.param('samplesheet_filename')
      index2_label=self.param('index2_label')
      revcomp_label=self.param('revcomp_label')
      tenX_label=self.param('10X_label')
      
      job_name=self.job_name()
      work_dir=os.path.join(base_work_dir,seqrun_igf_id,job_name)               # get work directory name
      if not os.path.exists(work_dir):
        os.makedirs(work_dir,mode=0o770)                                        # create work directory
      
      output_file=os.path.join(work_dir,samplesheet_filename)                   # get name of the output file
      if os.path.exists(output_file):
        raise IOError('seqrun: {0}, reformatted samplesheet {1} already present'.\
                      format(seqrun_igf_id,output_file))
      
      samplesheet_file=os.path.join(seqrun_local_dir, 
                                    seqrun_igf_id, 
                                    samplesheet_filename)
      if not os.path.exists(samplesheet_file):
        raise IOError('seqrun: {0}, samplesheet file {1} not found'.\
                      format(seqrun_igf_id,samplesheet_file))
    
      samplesheet=SampleSheet(infile=samplesheet_file)                          # read samplesheet
      samplesheet.filter_sample_data(condition_key='Description', 
                                     condition_value=tenX_label, 
                                     method='exclude')                          # separate 10X samplesheet
      sa=SeqrunAdaptor(**{'session_class':igf_session_class})
      sa.start_session()
      rules_data=sa.fetch_flowcell_barcode_rules_for_seqrun(seqrun_igf_id)      # convert index based on barcode rules
      sa.close_session()
      
      rules_data=rules_data.to_dict(orient='records')[0]                        # convert dataframe to dictionary
      
      if rules_data[index2_label]==revcomp_label:
        samplesheet.get_reverse_complement_index(index_field='index2')          # reverse complement index2 based on the rules
                                                                                # no need to add revcomp method for index1
      samplesheet_data.print_sampleSheet(outfile=output_file)
      self.param('dataflow_params',{'samplesheet':output_file})
      
      message='seqrun: {0}, reformatted samplesheet:{1}'.format(seqrun_igf_id,\
                                                               output_file)
      self.post_message_to_slack(message,reaction='pass')
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise