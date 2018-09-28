import os,subprocess

class Fastp_utils:
  '''
  A class for running fastp tool for a list of input fastq files
  
  :param fastp_exe: A fastp executable path
  :param input_fastq_list: A list of input files
  :param output_dir: A output directory path
  :param split_by_lines_count: Number of entries for splitted fastq files, default 5000000
  :param fastp_options_list: A list of options for running fastp, default  empty list
  '''
  def __init__(self,fastp_exe,input_fastq_list,output_dir,
               split_by_lines_count=5000000,fastp_options_list=[]):
    self.fastp_exe=fastp_exe
    self.input_fastq_list=input_fastq_list
    self.output_dir=output_dir
    self.split_by_lines_count=split_by_lines_count
    self.fastp_options=fastp_options

  def _run_checks(self):
    '''
    An internal method for running initial checks before fastp run
    '''
    try:
      if not os.path.exists(self.fastp_exe):
        raise IOError('Fastp executable {0} not found'.\
                      format(self.fastp_exe))

      file_check=0
      for file in self.input_fastq_list:
        if not os.path.exists(file):
          file_check += 1

      if file_check > 0:
        raise IOError('Fastq input files not found: {0}'.\
                      format(self.input_fastq_list))

      if not os.path.exists(self.output_dir):
        raise IOError('Output directory path {0} not found'.\
                      format(self.output_dir))

    except:
      raise
    
  def run_adapter_trimming(self):
    '''
    A method for running fastp adapter trimming
    
    :returns: A list of output fastq files and a html report path
    '''
    try:
      
    except:
      raise

  def run_adapter_trimming_and_splitting(self):
    '''
    A method for running fastp adapter trimming and fastq splitting
    
    :returns: A list of list for output fastq files and a html report path
    '''
    try:
      pass
    except:
      raise