import os,subprocess,fnmatch
from shlex import quote
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

def _check_cram_file(cram_path):
  '''
  An internal method for checking cram file
  
  :param cram_path: A cram file path
  :raises Value Error: It raises error if cram file doesn't have '.cram' extension
  :raises IOError: It raises IOError if the cram_path doesn't exists
  '''
  try:
    check_file_path(cram_path)
    if not fnmatch.fnmatch(cram_path, '*.cram'):
      raise ValueError('Cram file extension is not correct: {0}'.\
                       format(cram_path))
  except:
    raise


def _check_bam_file(bam_file):
  '''
  An internal method for checking bam file
  
  :param bam_file: A bam file path
  :raises Value Error: It raises error if bam file doesn't have '.bam' extension
  :raises IOError: It raises IOError if the bam_file doesn't exists
  '''
  try:
    check_file_path(bam_file)
    if not fnmatch.fnmatch(bam_file, '*.bam'):
      raise ValueError('Bam file extension is not correct: {0}'.\
                       format(bam_file))
  except:
    raise

def _check_bam_index(samtools_exe,bam_file):
  '''
  An internal method for checking bam index files. It will generate a new index if its not found.
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam file path
  '''
  try:
    check_file_path(samtools_exe)
    bam_index='{0}.bai'.format(bam_file)
    if not os.path.exists(bam_index):
      index_cmd=[quotes(samtools_exe),
                 'index',
                 quote(bam_file)
                ]                                                               # generate bam index, if its not present
      subprocess.check_call(index_cmd,
                            shell=False)

  except:
    raise


def convert_bam_to_cram(samtools_exe,bam_file,reference_file,cram_path,threads=1,
                        force=False):
  '''
  A function for converting bam files to cram using pysam utility
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param reference_file: Reference genome fasta filepath
  :param cram_path: A cram output file path
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output cram will be overwritten if force is True, default False
  :returns: Nill
  :raises IOError: It raises IOError if no input or reference fasta file found or
                   output file already present and force is not True
  :raises ValueError: It raises ValueError if bam_file doesn't have .bam extension or 
                      cram_path doesn't have .cram extension
  '''
  try:
    check_file_path(samtools_exe)
    check_file_path(reference_file)
    _check_bam_file(bam_file=bam_file)                                          # check bam file
    _check_bam_index(samtools_exe=samtools_exe,
                     bam_file=bam_file)                                         # check bam index
    temp_file=os.path.join(get_temp_dir(),
                           os.path.basename(cram_path))                         # get temp cram file path
    view_cmd=[quotes(samtools_exe),
              'view',
              '-C',
              '-OCRAM',
              '-@{0}'.format(quotes(threads)),
              '-T{0}'.format(quote(reference_file)),
              '-o{0}'.format(quote(temp_file)),
              quotes(bam_file)
             ]                                                                  # convert bam to cram using pysam view
    subprocess.check_call(view_cmd)
    _check_cram_file(cram_path=temp_file)                                       # check cram output
    copy_local_file(\
      source_path=temp_file,
      destinationa_path=cram_path,
      force=force)                                                              # move cram file to original path
    remove_dir(dir_path=os.path.dirname(temp_file))                             # remove temp directory
  except:
    raise


def run_bam_flagstat(samtools_exe,bam_file,output_dir,threads=1,force=False):
  '''
  A method for generating bam flagstat output
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam flagstat output directory path
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output flagstat file will be overwritten if force is True, default False
  :returns: Output file path
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=bam_file)                                          # check bam file
    _check_bam_index(samtools_exe=samtools_exe,
                     bam_file=bam_file)                                         # generate bam index
    output_path='{0}.{1}.{2}'.\
                format(os.path.basename(bam_file),
                       'flagstat',
                       'txt')                                                   # get output filename
    output_path=os.path.join(output_dir,
                             output_path)                                       # get complete output path
    if not os.path.exists(output_dir):
      raise IOError('Output path {0} not found'.format(output_dir))

    if os.path.exists(output_path) and not force:
      raise ValueError('Output file {0} already present, use force to overwrite'.\
                       format(output_path))

    flagstat_cmd=[quotes(samtools_exe),
                  'flagstat',
                  '-@{0}'.format(quotes(threads)),
                  quotes(bam_file)
                 ]
    with open(output_path,'w') as fp:
      with subprocess.Popen(flagstat_cmd, stdout=PIPE) as proc:
        fp.write(proc.stdout.read())                                        # write bam flagstat output

    return output_path
  except:
    raise


def run_bam_idxstat(samtools_exe,bam_file,output_dir,force=False):
  '''
  A function for running samtools index stats generation
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam idxstats output directory path
  :param force: Output idxstats file will be overwritten if force is True, default False
  :returns: Output file path
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=bam_file)                                          # check bam file
    _check_bam_index(samtools_exe=samtools_exe,
                     bam_file=bam_file)                                         # generate bam index
    output_path='{0}.{1}.{2}'.\
                format(os.path.basename(bam_file),
                       'idxstats',
                       'txt')                                                   # get output filename
    output_path=os.path.join(output_dir,
                             output_path)                                       # get complete output path
    if not os.path.exists(output_dir):
      raise IOError('Output path {0} not found'.format(output_dir))

    if os.path.exists(output_path) and not force:
      raise ValueError('Output file {0} already present, use force to overwrite'.\
                       format(output_path))

    idxstat_cmd=[quotes(samtools_exe),
                  'idxstats',
                  quotes(bam_file)
                ]
    with open(output_path,'w') as fp:
      with subprocess.Popen(idxstat_cmd, stdout=PIPE) as proc:
        fp.write(proc.stdout.read())                                            # write bam flagstat output

    return output_path
  except:
    raise
