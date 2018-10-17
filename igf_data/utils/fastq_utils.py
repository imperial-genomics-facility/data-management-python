import pandas as pd
import os,re,gzip
from igf_data.utils.fileutils import check_file_path

def identify_fastq_pair(input_list,sort_output=True,check_count=False):
  '''
  A method for fastq read pair identification

  :param input_list: A list of input fastq files
  :param sort_output: Sort output list, default true
  :param check_count: Check read count for fastq pair, only available if sort_output is True, default False
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

      if check_count and \
         len(read1_list) > 0 and \
         len(read2_list) > 0 :
        fastq_df=pd.DataFrame({'R1_file':read1_list,
                               'R2_file':read2_list})
        for entry in fastq_df.to_dict(orient='records'):
          compare_fastq_files_read_counts(r1_file=entry['R1_file'],
                                          r2_file=entry['R2_file'])

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

def compare_fastq_files_read_counts(r1_file,r2_file):
  '''
  '''
  try:
    check_file_path(r1_file)
    check_file_path(r2_file)
    r1_count=count_fastq_lines(r1_file)
    r2_count=count_fastq_lines(r2_file)
    if r1_count != r2_count:
      raise ValueError('Fastq pair does not have same number of reads: {0} {1}'.\
                       format(r1_file,r2_file))
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
    check_file_path(fastq_file)
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
