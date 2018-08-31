import os,zipfile, fnmatch, re
from igf_data.utils.fileutils import get_temp_dir

def get_fastq_info_from_fastq_zip(fastqc_zip,fastqc_datafile='*/fastqc_data.txt'):
  '''
  A function for retriving total reads and fastq file name from fastqc_zip file
  
  :param fastqc_zip: A zip file containing fastqc results
  :param fastqc_datafile: A pattern f
  :returns: return total read count and fastq filename
  '''
  try:
    zip_obj=zipfile.ZipFile(file=fastqc_zip)
    total_seq_pattern=re.compile(r'Total\sSequences\s+(\d+)\n')
    filename_pattern=re.compile(r'Filename\s(\S+)\n')
    total_reads=None
    fastq_filename=None
    
    fastqc_data=os.path.join(get_temp_dir(),\
                             'fastqc_data.txt')
    for file in zip_obj.namelist():
      if fnmatch.fnmatch(file,fastqc_datafile):
        with open(fastqc_data,'wb') as f1:
          f1.write(zip_obj.read(name=file))                                     # write fastqc data to temp file
          
    if not os.path.exists(fastqc_data):
      raise IOError('file {0} not found'.format(fastqc_data))
    
    with open(fastqc_data,'r') as t1:
      for l in t1:
        m1=re.match(total_seq_pattern,l)
        if m1:
          total_reads=m1.group(1)
          
        m2=re.match(filename_pattern,l)
        if m2:
          fastq_filename=m2.group(1)
    return total_reads, fastq_filename
  except:
    raise