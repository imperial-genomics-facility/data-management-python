import os,subprocess,re

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

      if len(self.input_fastq_list) > 2:
        raise ValueError('Expecting max 2 fastq files, got {0}'.\
                         format(len(self.input_fastq_list)))

    except:
      raise

  @staticmethod
  def _identify_fastq_pair(input_list):
    '''
    An internal static method for fastq pair identification
    
    :patam input_list:
    :returns: A list for read1 and another list for read2
    '''
    try:
      read1_list=list()
      read2_list=list()
      read1_pattern=re.compile(r'\S+_R1_\d+\.fastq(\.\gz)?')
      read2_pattern=re.compile(r'\S+_R2_\d+\.fastq?(\.\gz)?')
      for file in input_list:
        if re.match(read1_pattern,file):
          read1_list.append(file)

        if re.match(read2_pattern,file):
          read2_list.append(file)

      if len(read1_list) == 0:
        raise ValueError('No fastq file found for read 1')

      if len(read1_list) != len(read2_list) and \
         len(read2_list) > 0:
        raise ValueError('Number of fastq files are not same for read 1 :{0} and read2:{1}'.\
                         format(len(read1_list),len(read2_list)))

      if len(read1_list)==len(read2_list):                                      # if fastq input list is not properly sorted
        sorted_read2_list=list()
        for file1 in read1_list:
          temp_file2=file1.replace('R1','R2')
          if temp_file2 not in read2_list:
            raise ValueError('No read2 found for file {0}'.format(file1))
          else:
            sorted_read2_list.append(temp_file2)
        read2_list=sorted_read2_list                                            # reset read2 list

      return read1_list, read2_list
    except:
      raise

  def run_adapter_trimming(self):
    '''
    A method for running fastp adapter trimming
    
    :returns: A list of output fastq files and a html report path
    '''
    try:
      self._run_checks()
    except:
      raise

  def run_adapter_trimming_and_splitting(self):
    '''
    A method for running fastp adapter trimming and fastq splitting
    
    :returns: A list of list for output fastq files and a html report path
    '''
    try:
      self._run_checks()
    except:
      raise