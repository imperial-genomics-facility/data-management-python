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

def _check_bam_index(samtools_exe,bam_file,dry_run=False):
  '''
  An internal method for checking bam index files. It will generate a new index if its not found.
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam file path
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  '''
  try:
    check_file_path(samtools_exe)
    bam_index='{0}.bai'.format(bam_file)
    if not os.path.exists(bam_index):
      index_cmd=[quote(samtools_exe),
                 'index',
                 quote(bam_file)
                ]                                                               # generate bam index, if its not present
      if dry_run:
        return index_cmd

      subprocess.check_call(index_cmd,
                            shell=False)

  except:
    raise


def convert_bam_to_cram(samtools_exe,bam_file,reference_file,cram_path,threads=1,
                        force=False,dry_run=False):
  '''
  A function for converting bam files to cram using pysam utility
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param reference_file: Reference genome fasta filepath
  :param cram_path: A cram output file path
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output cram will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
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
    view_cmd=[quote(samtools_exe),
              'view',
              '-C',
              '-OCRAM',
              '-@{0}'.format(quote(str(threads))),
              '-T{0}'.format(quote(reference_file)),
              '-o{0}'.format(quote(temp_file)),
              quote(bam_file)
             ]                                                                  # convert bam to cram using samtools
    if dry_run:
      return view_cmd

    subprocess.check_call(view_cmd,
                          shell=False)
    _check_cram_file(cram_path=temp_file)                                       # check cram output
    copy_local_file(\
      source_path=temp_file,
      destinationa_path=cram_path,
      force=force)                                                              # move cram file to original path
    remove_dir(dir_path=os.path.dirname(temp_file))                             # remove temp directory
  except:
    raise


def run_bam_flagstat(samtools_exe,bam_file,output_dir,threads=1,force=False,
                     dry_run=False):
  '''
  A method for generating bam flagstat output
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam flagstat output directory path
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output flagstat file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
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

    flagstat_cmd=[quote(samtools_exe),
                  'flagstat',
                  '-@{0}'.format(quote(str(threads))),
                  quote(bam_file)
                 ]
    if dry_run:
      return flagstat_cmd

    with open(output_path,'w') as fp:
      with subprocess.Popen(flagstat_cmd, stdout=PIPE) as proc:
        fp.write(proc.stdout.read())                                        # write bam flagstat output

    return output_path
  except:
    raise


def run_bam_idxstat(samtools_exe,bam_file,output_dir,force=False,dry_run=False):
  '''
  A function for running samtools index stats generation
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam idxstats output directory path
  :param force: Output idxstats file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
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

    idxstat_cmd=[quote(samtools_exe),
                  'idxstats',
                  quote(bam_file)
                ]
    if dry_run:
      return idxstat_cmd

    with open(output_path,'w') as fp:
      with subprocess.Popen(idxstat_cmd, stdout=PIPE) as proc:
        fp.write(proc.stdout.read())                                            # write bam flagstat output

    return output_path
  except:
    raise


def run_sort_bam(samtools_exe,input_bam_path,output_bam_path,sort_by_name=False,
                 threads=1,force=False,dry_run=False):
  '''
  A function for sorting input bam file and generate a output bam
  
  :param samtools_exe: samtools executable path
  :param input_bam_path: A bam filepath
  :param output_bam_path: A bam output filepath
  :param sort_by_name: Sort bam file by read_name, default False (for coordinate sorting)
  :param threads: Number of threads to use for sorting, default 1
  :param force: Output bam file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :return: None
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=input_bam_path)
    sort_cmd=[quote(samtools_exe),
              'sort',
              '--output-fmt','BAM',
              '--threads',quote(str(threads))
              ]
    if sort_by_name:
      sort_cmd.append('-n')                                                     # sorting by read name

    sort_cmd.append(quote(input_bam_path))

    temp_dir=get_temp_dir()
    temp_bam=os.path.join(temp_dir,
                          os.path.basename(output_bam_path))

    if dry_run:
      return sort_cmd

    with open(temp_bam,'w') as bam:
      with subprocess.Popen(sort_cmd, stdout=PIPE) as proc:
        bam.write(proc.stdout.read())                                           # write temp bam files

    copy_local_file(source_path=temp_bam,
                    destinationa_path=output_bam_path,
                    force=force)                                                # copy output bam
    remove_dir(temp_dir)                                                        # remove temp dir
    _check_bam_file(output_bam_path)
  except:
    raise


def merge_multiple_bam(samtools_exe,input_bam_list,output_bam_path,sorted_by_name=False,
                       threads=1,force=False,dry_run=False):
  '''
  A function for merging multiple input bams to a single output bam
  
  :param samtools_exe: samtools executable path
  :param input_bam_list: A file containing list of bam filepath
  :param output_bam_path: A bam output filepath
  :param sorted_by_name: Sort bam file by read_name, default False (for coordinate sorted bams)
  :param threads: Number of threads to use for merging, default 1
  :param force: Output bam file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :return: None
  '''
  try:
    check_file_path(samtools_exe)
    check_file_path(input_bam_list)
    temp_dir=get_temp_dir()
    temp_bam=os.path.join(temp_dir,
                          os.path.basename(output_bam_path))
    merge_cmd=[quote(samtools_exe),
               'merge',
               '--output-fmt','BAM',
               '--threads',quote(str(threads)),
               '-b',quote(input_bam_list)
              ]
    if sorted_by_name:
      merge_cmd.append('-n')                                                    # Input files are sorted by read name

    merge_cmd.append(quote(temp_bam))
    if dry_run:
      return merge_cmd

    subprocess.check_call(merge_cmd)                                            # run samtools merge
    copy_local_file(source_path=temp_bam,
                    destinationa_path=output_bam_path,
                    force=force)                                                # copy bamfile
    remove_dir(temp_dir)                                                        # remove temp dir
    _check_bam_file(output_bam_path)
  except:
    raise

