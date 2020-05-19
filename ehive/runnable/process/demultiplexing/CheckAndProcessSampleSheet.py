#!/usr/bin/env python
import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor

class CheckAndProcessSampleSheet(IGFBaseProcess):
  '''
  A class for checking and processing samplesheet
  creates a re-formated samplesheet removing the 10X samples
  and converting index barcodes based on flowcell rules table
  '''
  def param_defaults(self):
    params_dict=super(CheckAndProcessSampleSheet,self).param_defaults()
    params_dict.update({
        'samplesheet_filename':'SampleSheet.csv',
        'index2_label_in_db':'index_2',
        'revcomp_label':'REVCOMP',
        'singlecell_tag':'10X',
        'adapter_trim_check':True,
        'adapter_section':'Settings',
        'read1_adapter_label':'Adapter',
        'read2_adapter_label':'AdapterRead2',
        'project_type':None,
      })
    return params_dict


  def run(self):
    try:
      igf_session_class = self.param_required('igf_session_class')
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      seqrun_local_dir = self.param_required('seqrun_local_dir')
      base_work_dir = self.param_required('base_work_dir')
      samplesheet_filename = self.param('samplesheet_filename')
      index2_label = self.param('index2_label_in_db')
      revcomp_label = self.param('revcomp_label')
      singlecell_tag = self.param('singlecell_tag')
      adapter_trim_check = self.param('adapter_trim_check')
      adapter_section = self.param('adapter_section')
      read1_adapter_label = self.param('read1_adapter_label')
      read2_adapter_label = self.param('read2_adapter_label')
      project_type = self.param('project_type')

      job_name = self.job_name()
      work_dir = \
        os.path.join(\
          base_work_dir,
          seqrun_igf_id,
          job_name)                                                             # get work directory name
      if not os.path.exists(work_dir):
        os.makedirs(work_dir,mode=0o770)                                        # create work directory

      output_file = \
        os.path.join(\
          work_dir,
          samplesheet_filename)                                                 # get name of the output file
      if os.path.exists(output_file):
        raise IOError('seqrun: {0}, reformatted samplesheet {1} already present'.\
                      format(seqrun_igf_id,output_file))

      samplesheet_file = \
        os.path.join(\
          seqrun_local_dir,
          seqrun_igf_id,
          samplesheet_filename)
      if not os.path.exists(samplesheet_file):
        raise IOError('seqrun: {0}, samplesheet file {1} not found'.\
                      format(seqrun_igf_id,samplesheet_file))

      samplesheet_sc = \
        SampleSheet(infile=samplesheet_file)                                    # read samplesheet for single cell check
      samplesheet_sc.\
        filter_sample_data(\
          condition_key='Description',
          condition_value=singlecell_tag,
          method='include')                                                     # get 10X samplesheet
      if len(samplesheet_sc._data) > 0:
        project_type=singlecell_tag                                             # check if 10x samples are present in samplesheet

      samplesheet = \
        SampleSheet(infile=samplesheet_file)                                    # read samplesheet
      samplesheet.\
        filter_sample_data(\
          condition_key='Description',
          condition_value=singlecell_tag,
          method='exclude')                                                     # filter 10X samplesheet

      if adapter_trim_check:
        read1_val = \
          samplesheet.\
            check_sample_header(\
              section=adapter_section,
              condition_key=read1_adapter_label)
        read2_val = \
          samplesheet.\
            check_sample_header(\
              section=adapter_section,
              condition_key=read2_adapter_label)
        if read1_val==0 or read2_val==0:
          message = \
            'seqrun {0} samplesheet does not have adapter trim option, read1: {1}, read2:{2}'.\
            format(seqrun_igf_id,read1_val,read2_val)
          self.post_message_to_slack(message,reaction='pass')
          self.comment_asana_task(task_name=seqrun_igf_id, comment=message)     # send info about adapter trip to slack and asana
          self.post_message_to_ms_team(
            message=message,
            reaction='pass')
      sa = SeqrunAdaptor(**{'session_class':igf_session_class})
      sa.start_session()
      rules_data = \
        sa.fetch_flowcell_barcode_rules_for_seqrun(seqrun_igf_id)               # convert index based on barcode rules
      sa.close_session()

      rules_data_set = rules_data.to_dict(orient='records')                     # convert dataframe to dictionary
      if len(rules_data_set) > 0:
        rules_data=rules_data_set[0]                                            # consider only the first rule
        if rules_data[index2_label]==revcomp_label:
          samplesheet.\
            get_reverse_complement_index(index_field='index2')                  # reverse complement index2 based on the rules
      else:
        message = \
          'no rules found for seqrun {0}, using original samplesheet'.\
          format(seqrun_igf_id)
        self.post_message_to_slack(message,reaction='pass')
        self.comment_asana_task(task_name=seqrun_igf_id, comment=message)
        self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      if len(samplesheet_sc._data) > 0:                                         # merge 10x samplesheet
        samplesheet._data.extend(samplesheet_sc._data)
        message = \
          'seqrun: {0}, merging 10X samples with reformatted samplesheet'.\
          format(seqrun_igf_id)
        self.post_message_to_slack(message,reaction='pass')
        self.comment_asana_task(task_name=seqrun_igf_id, comment=message)
        self.post_message_to_ms_team(
          message=message,
          reaction='pass')
      samplesheet.print_sampleSheet(outfile=output_file)
      self.param('dataflow_params',
                 {'samplesheet':output_file,
                  'project_type':project_type})
      message = \
        'seqrun: {0}, reformatted samplesheet:{1}'.\
          format(\
            seqrun_igf_id,
            output_file)
      self.post_message_to_slack(message,reaction='pass')
      self.comment_asana_task(task_name=seqrun_igf_id, comment=message)
      self.post_message_to_ms_team(
          message=message,
          reaction='pass')
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