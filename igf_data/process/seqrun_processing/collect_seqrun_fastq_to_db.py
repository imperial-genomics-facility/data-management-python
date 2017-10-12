import os, re

def get_fastq_and_samplesheet(fastq_dir, samplesheet_filename):
  r1_fastq_regex=re.compile(r'\S+_R1_\d+\.fastq(\.gz)?', re.IGNORECASE)
  r2_fastq_regex=re.compile(r'\S+_R2_\d+\.fastq(\.gz)?', re.IGNORECASE)
  samplesheet_list=list()
  r1_fastq_list=list()
  r2_fastq_list=list()
  for root, dirs, files in os.walk(top=fastq_dir, topdown=True):
    if samplesheet_filename in files:
      samplesheet_list.append(os.path.join(root,samplesheet_filename))
      for file in files:
        if r1_fastq_regex.match(file):
          r1_fastq_list.append(os.path.join(root,file))
        elif r2_fastq_regex.match(file):
          r2_fastq_list.append(os.path.join(root,file))
                
  if len(r2_fastq_list) > 0 and len(r1_fastq_list) != len(r2_fastq_list):
    raise ValueError('R1 {0} and R2 {1}'.format(len(r1_fastq_list),len(r2_fastq_list)))
        
  if len(samplesheet_list) > 1:
    raise ValueError('Found more than one samplesheet file for fastq dir {0}'.format(fastq_dir))
        
  return samplesheet_list[0], r1_fastq_list, r2_fastq_list
