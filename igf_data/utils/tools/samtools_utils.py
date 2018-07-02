import os
import pysam
import fnmatch
from shlex import quote
from igf_data.utils.fileutils import get_temp_dir,remove_dir,move_file

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
    if not os.path.exists(bam_file):
      raise IOError('Input bam not found {0}'.format(bam_file))

    if not os.path.exists(reference_file):
      raise IOError('Input reference fasta not found {0}'.format(reference_file))

    if os.path.exists(cram_path) and not force:
      raise IOError('Output cram already present {0}, and force is not enabled'.\
                    format(cram_path))

    if not fnmatch.fnmatch(bam_file, '*.bam'):
      raise ValueError('Bam file extension is not correct: {0}'.\
                       format(bam_file))

    if not fnmatch.fnmatch(cram_path, '*.cram'):
      raise ValueError('Cram file extension is not correct: {0}'.\
                       format(cram_path))

    if not os.path.exists(os.path.join(bam_file,'.bai')):
      pysam.samtools.index(bam_file)                                            # generate bam index if its not present

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
    move_file(source_path=temp_file,
              destinationa_path=cram_path,
              force=force)                                                      # move cram file to original path
    remove_dir(dir_path=os.path.dirname(temp_file))                             # remove temp directory
  except:
    raise