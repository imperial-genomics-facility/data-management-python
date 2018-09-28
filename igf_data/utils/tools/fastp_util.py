import os,subprocess

class Fastp_utils:
  '''
  A class for running fastp tool for a list of input fastq files
  
  '''
  def __init__(self,fastp_exe,input_fastq_list,output_dir,
               split_by_lines_count=5000000,fastp_options={}):
    self.fastp_exe=fastp_exe
    self.input_fastq_list=input_fastq_list
    self.output_dir=output_dir
    self.split_by_lines_count=split_by_lines_count
    self.fastp_options=fastp_options

  def run_adapter_trimming(self):
    '''
    A method for running fastp adapter trimming
    
    :returns: A list of output fastq files, a html report path
    '''
    try:
      pass
    except:
      raise

  def run_adapter_trimming_and_splitting(self):
    '''
    '''
    try:
      pass
    except:
      raise