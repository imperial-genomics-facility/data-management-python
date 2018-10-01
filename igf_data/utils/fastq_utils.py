import os,re

def identify_fastq_pair(input_list,sort_output=True):
  '''
  A method for fastq read pair identification

  :param input_list: A list of input fastq files
  :param sort_output: Sort output list, default true
  :returns: A list for read1 files and another list of read2 files
  '''
  try:
    read1_list=list()
    read2_list=list()
    read1_pattern=re.compile(r'\S+_R1_\d+\.fastq(\.gz)?')
    read2_pattern=re.compile(r'\S+_R2_\d+\.fastq?(\.gz)?')
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

    if sort_output and \
       len(read1_list)==len(read2_list):                                        # if fastq input list is not properly sorted
      sorted_read2_list=list()
      for file1 in read1_list:
        temp_file2=file1.replace('R1','R2')
        if temp_file2 not in read2_list:
          raise ValueError('No read2 found for file {0}'.format(file1))
        else:
          sorted_read2_list.append(temp_file2)
      read2_list=sorted_read2_list                                              # reset read2 list

    return read1_list, read2_list
  except:
    raise

  def detect_non_fastq_in_file_list(input_list):
    '''
    A method for detecting non fastq file within a list of input fastq
    
    :param input_list: A list of filepath to check
    :returns: True in non fastq files are present or else False
    '''
    try:
      fastq_pattern=re.compile(r'\S+\.fastq(\.gz)?')
      non_fastq_found=False
      for file in input_list:
        if not re.match(fastq_pattern,os.path.basename(file)):
          non_fastq_found=True

      return non_fastq_found
    except:
      raise