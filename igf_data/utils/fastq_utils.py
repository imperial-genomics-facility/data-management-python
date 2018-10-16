import os,re,gzip

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

def count_fastq_lines(fastq_file):
  '''
  A method for counting fastq lines
  
  :param fastq_file: A gzipped or unzipped fastq file
  :returns: Fastq line count
  '''
  try:
    gzipped_pattern=re.compile(r'\S+\.fastq\.gz$')
    unzipped_pattern=re.compile(r'\S+\.fastq$')
    lines=0
    if not os.path.exists(fastq_file):
      raise IOError('Fastq file {0} not found'.\
                    format(fastq_file))

    if re.match(gzipped_pattern,fastq_file):                                    # read gzipped file
      with gzip.open(fastq_file, 'rb') as f:
        buf_size = 1024 * 1024
        read_f = f.read
        buf = read_f(buf_size)
        while buf:
          lines += buf.count(b'\n')
          buf = read_f(buf_size)

    elif re.match(unzipped_pattern,fastq_file):                                 # read unzipped file
      with open(filename, 'rb') as f:
        buf_size = 1024 * 1024
        read_f = f.raw.read
        buf = read_f(buf_size)
        while buf:
          lines += buf.count(b'\n')
          buf = read_f(buf_size)

    else:
      raise ValueError('Failed to detect read mode for fastq file {0}'.\
                       format(fastq_file))

    if lines >= 4 :
      if lines % 4 != 0:
        raise ValueError('Fastq file missing have block of 4 lines:{0}'.\
                         format(fastq_file))

      lines = int(lines/4)

    return lines
  except:
    raise
