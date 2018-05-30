import os
import pysam
import fnmatch

def convert_bam_to_cram(bam_file,reference_file,cram_path,force=False):
  '''
  A function for converting bam files to cram using pysam utility
  
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param reference_file: Reference genome fasta filepath
  :param cram_path: A cram output file path
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

    bam_data=pysam.AlignmentFile(bam_file,
                                 mode='rb',
                                 check_sq=True,
                                 require_index=True)                            # read bam file
    bam_header=bam_data.header.as_dict()                                        # get bam header info
    with pysam.AlignmentFile(cram_path,
                             mode='wb',
                             reference_filename=reference_file,
                             header=bam_header) as outf:
      for read in bam_data.fetch():
        outf.write(read)                                                        # write entries to cram format
  except:
    raise