import os
import pysam
import fnmatch
from shlex import quote
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file

def _check_cram_file(cram_path):
  '''
  An internal method for checking cram file
  
  :param cram_path: A cram file path
  :raises Value Error: It raises error if cram file doesn't have '.cram' extension
  :raises IOError: It raises IOError if the cram_path doesn't exists
  '''
  try:
    if not os.path.exists(cram_path):
      raise IOError('Output cram not present {0}'.\
                    format(cram_path))

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
    if not os.path.exists(bam_file):
      raise IOError('Input bam not found {0}'.format(bam_file))

    if not fnmatch.fnmatch(bam_file, '*.bam'):
      raise ValueError('Bam file extension is not correct: {0}'.\
                       format(bam_file))
  except:
    raise

def _check_bam_index(bam_file):
  '''
  An internal method for checking bam index files. It will generate a new index if its not found.
  
  :param bam_file: A bam file path
  '''
  try:
    bam_index='{0}.bai'.format(bam_file)
    if not os.path.exists(bam_index):
      pysam.index(quote(bam_file))                                              # generate bam index, if its not present

  except:
    raise


def convert_bam_to_cram(bam_file,reference_file,cram_path,threads=1,force=False):
  '''
  A function for converting bam files to cram using pysam utility
  
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
    if not os.path.exists(reference_file):
      raise IOError('Input reference fasta not found {0}'.format(reference_file))

    _check_bam_file(bam_file=bam_file)                                          # check bam file
    _check_bam_index(bam_file=bam_file)                                         # check bam index

    temp_file=os.path.join(get_temp_dir(),
                           os.path.basename(cram_path))                         # get temp cram file path
    pysam.samtools.view(bam_file,
                        '-C',
                        '-OCRAM',
                        '-@{0}'.format(threads),
                        '-T{0}'.format(quote(reference_file)),
                        '-o{0}'.format(quote(temp_file)),
                        catch_stdout=False
                        )                                                       # convert bam to cram using pysam view
    _check_cram_file(cram_path=temp_file)                                       # check cram output
    move_file(source_path=temp_file,
              destinationa_path=cram_path,
              force=force)                                                      # move cram file to original path
    remove_dir(dir_path=os.path.dirname(temp_file))                             # remove temp directory
  except:
    raise


  def run_bam_flagstat(bam_file,output_path,threads=1,force=False):
    '''
    A method for generating bam flagstat output
    
    :param bam_file: A bam filepath with / without index. Index file will be created if its missing
    :param output_path: Bam flagstat output file path
    :param threads: Number of threads to use for conversion, default 1
    :param force: Output flagstat file will be overwritten if force is True, default False
    '''
    try:
      _check_bam_file(bam_file=bam_file)                                        # check bam file
      _check_bam_index(bam_file=bam_file)                                       # generate bam index
      if os.path.exists(output_path) and not force:
        raise ValueError('Output file {0} already present, use force to overwrite'.\
                         format(output_path))

      with open(output_path,'w') as fp:
        fp.write(pysam.flagstat(quote(bam_file),
                                '-@{0}'.format(threads)))                       # write bam flagstat output

    except:
      raise


  def run_bam_idxstat(bam_file,output_path,force=False):
    '''
    A function for running samtools index stats generation
    
    :param bam_file: A bam filepath with / without index. Index file will be created if its missing
    :param output_path: Bam idxstats output file path
    :param force: Output idxstats file will be overwritten if force is True, default False
    '''
    try:
      _check_bam_file(bam_file=bam_file)                                        # check bam file
      _check_bam_index(bam_file=bam_file)                                       # generate bam index
      if os.path.exists(output_path) and not force:
        raise ValueError('Output file {0} already present, use force to overwrite'.\
                         format(output_path))

      with open(output_path,'w') as fp:
        fp.write(pysam.idxstats(quote(bam_file)))                               # write bam flagstat output

    except:
      raise
