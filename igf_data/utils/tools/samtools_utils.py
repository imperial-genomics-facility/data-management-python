import os,subprocess,fnmatch
import pandas as pd
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
      index_bam_or_cram(\
        samtools_exe=samtools_exe,
        input_path=bam_file,
        dry_run=dry_run
      )
  except:
    raise


def index_bam_or_cram(samtools_exe,input_path,threads=1,
                      dry_run=False):
  '''
  A method for running samtools index

  :param samtools_exe: samtools executable path
  :param input_path: Alignment filepath
  :param threads: Number of threads to use for conversion, default 1
  :param dry_run: A toggle for returning the samtools command
                  without actually running it, default False
  :returns: samtools cmd list
  '''
  try:
    index_cmd=[quote(samtools_exe),
               'index',
              ]
    if threads is not None:
      index_cmd.append('-@{0}'.format(str(threads)))

    index_cmd.append(quote(input_path))
    if dry_run:
        return index_cmd

    subprocess.\
      check_call(\
        ' '.join(index_cmd),
        shell=True)
    return index_cmd
  except:
    raise


def run_samtools_view(samtools_exe,input_file,output_file,reference_file=None,
                      force=True,cram_out=False,threads=1,samtools_params=None,
                      index_output=True,dry_run=False):
  '''
  A function for running samtools view command

  :param samtools_exe: samtools executable path
  :param input_file: An input bam filepath with / without index. Index file will be created if its missing
  :param output_file: An output file path
  :param reference_file: Reference genome fasta filepath, default None
  :param force: Output file will be overwritten if force is True, default True
  :param threads: Number of threads to use for conversion, default 1
  :param samtools_params: List of samtools param, default None
  :param index_output: Index output file, default True
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :returns: Samtools command as list
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=input_file)                                        # check bam file
    if not dry_run:
      _check_bam_index(\
        samtools_exe=samtools_exe,
        bam_file=input_file)                                                    # check bam index

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_file = \
      os.path.join(\
        temp_dir,
        os.path.basename(output_file))                                          # get temp output file path
    view_cmd = \
      [quote(samtools_exe),
       'view',
       '-o',quote(temp_file)
      ]                                                                         # convert bam to cram using samtools
    if reference_file is not None:
      check_file_path(reference_file)
      view_cmd.extend(['-T',quote(reference_file)])

    if threads is not None:
      view_cmd.append('-@{0}'.format(quote(str(threads))))

    if cram_out:
      view_cmd.append('-C')
      if reference_file is None:
        raise ValueError('Reference file is required for cram output')
    else:
      view_cmd.append('-b')

    if samtools_params is not None and \
       isinstance(samtools_params, list) and \
       len(samtools_params) > 0:
      view_cmd.extend(\
        [quote(i) for i in samtools_params])                                    # add additional params

    view_cmd.append(quote(input_file))
    if dry_run:
      return view_cmd

    subprocess.check_call(\
      ' '.join(view_cmd),
      shell=True)
    if cram_out:
      _check_cram_file(cram_path=temp_file)                                     # check cram output

    copy_local_file(\
      source_path=temp_file,
      destinationa_path=output_file,
      force=force)                                                              # move cram file to original path
    remove_dir(temp_dir)                                                        # remove temp directory
    if index_output:
      index_bam_or_cram(\
        samtools_exe=samtools_exe,
        input_path=output_file,
        threads=threads)

    return view_cmd
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
    view_cmd = \
      run_samtools_view(
        samtools_exe=samtools_exe,
        input_file=bam_file,
        output_file=cram_path,
        reference_file=reference_file,
        force=force,
        cram_out=True,
        threads=threads,
        index_output=False,
        dry_run=dry_run)
    return view_cmd
  except:
    raise


def filter_bam_file(samtools_exe,input_bam,output_bam,samFlagInclude=None,reference_file=None,
                    samFlagExclude=None,threads=1,mapq_threshold=20,cram_out=False,
                    index_output=True,dry_run=False):
  '''
  A function for filtering bam file using samtools view

  :param samtools_exe: Samtools path
  :param input_bam: Input bamfile path
  :param output_bam: Output bamfile path
  :param samFlagInclude: Sam flags to keep, default None
  :param reference_file: Reference genome fasta filepath
  :param samFlagExclude: Sam flags to exclude, default None
  :param threads: Number of threads to use, default 1
  :param mapq_threshold: Skip alignments with MAPQ smaller than this value, default None
  :param index_output: Index output bam, default True
  :param cram_out: Output cram file, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :returns: Samtools command
  '''
  try:
    samtools_params = list()
    if mapq_threshold is not None:
      samtools_params.\
        append("-q{0}".format(quote(str(mapq_threshold))))

    if samFlagInclude is not None:
      samtools_params.\
        append("-f{0}".format(quote(str(samFlagInclude))))

    if samFlagExclude is not None:
      samtools_params.\
        append("-F{0}".format(quote(str(samFlagExclude))))

    view_cmd = \
      run_samtools_view(\
        samtools_exe=samtools_exe,
        input_file=input_bam,
        output_file=output_bam,
        cram_out=cram_out,
        index_output=index_output,
        threads=threads,
        dry_run=dry_run,
        reference_file=reference_file,
        samtools_params=samtools_params)
    return view_cmd
  except:
    raise


def run_bam_stats(samtools_exe,bam_file,output_dir,threads=1,force=False,
                  output_prefix=None,dry_run=False):
  '''
  A method for generating samtools stats output
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam stats output directory path
  :param output_prefix: Output file prefix, default None
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output flagstat file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :returns: Output file path, list containing samtools command and a list containing the SN matrics of report
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=bam_file)
    if not dry_run:
      _check_bam_index(\
        samtools_exe=samtools_exe,
        bam_file=bam_file)

    if output_prefix is None:
      output_prefix = os.path.basename(bam_file)

    output_path = \
      '{0}.{1}.{2}'.\
      format(output_prefix,'stats','txt')
    output_path = \
      os.path.join(\
        output_dir,
        output_path)
    if not os.path.exists(output_dir):
      raise IOError('Output path {0} not found'.format(output_dir))

    if os.path.exists(output_path) and not force:
      raise ValueError('Output file {0} already present, use force to overwrite'.\
                       format(output_path))

    stats_cmd = \
      [quote(samtools_exe),
       'stats',
       '-@{0}'.format(quote(str(threads))),
       quote(bam_file)
      ]
    if dry_run:
      return stats_cmd

    with open(output_path,'w') as fp:
      with subprocess.Popen(stats_cmd, stdout=subprocess.PIPE) as proc:
        fp.write(proc.stdout.read().decode('utf-8'))                            # write bam stats output

    stats_data_list = \
      _parse_samtools_stats_output(stats_file=output_path)                      # parse stats output file

    return output_path,stats_cmd,stats_data_list
  except:
    raise


def _parse_samtools_stats_output(stats_file):
  '''
  An internal static method for parsing samtools stats output

  :param stats_file: Samtools stats output file
  :returns: A list of dictionaries
  '''
  try:
    data = \
      pd.read_csv(\
        stats_file,
        sep='\n',
        header=None,
        engine='python',
        dtype=object,
        comment='#')
    sn_rows = \
      data[data.get(0).map(lambda x: x.startswith('SN'))][0].\
      map(lambda x: x.strip('SN\t')).values                                     # read SN fields from report
    stats_data_list = list()
    for row in sn_rows:
      stats_data_list.\
      append(\
        {'SAMTOOLS_STATS_{0}'.format(item.replace(' ','_')):row.split(':')[index+1].strip()
            for index, item in enumerate(row.split(':'))
              if index % 2 == 0
        })                                                                         # append sn fields with minor formatting
    return stats_data_list
  except Exception as e:
    raise ValueError('Failed to parse file {0}, got error {1}'.\
          format(stats_file,e))


def run_bam_flagstat(samtools_exe,bam_file,output_dir,threads=1,force=False,
                     output_prefix=None,dry_run=False):
  '''
  A method for generating bam flagstat output
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam flagstat output directory path
  :param output_prefix: Output file prefix, default None
  :param threads: Number of threads to use for conversion, default 1
  :param force: Output flagstat file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :returns: Output file path and a list containing samtools command
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=bam_file)                                          # check bam file
    if not dry_run:
      _check_bam_index(\
        samtools_exe=samtools_exe,
        bam_file=bam_file)                                                      # generate bam index
    if output_prefix is None:
      output_prefix=os.path.basename(bam_file)

    output_path = \
      '{0}.{1}.{2}'.\
      format(output_prefix,'flagstat','txt')                                    # get output filename
    output_path = \
      os.path.join(\
        output_dir,
        output_path)                                                            # get complete output path
    if not os.path.exists(output_dir):
      raise IOError('Output path {0} not found'.format(output_dir))

    if os.path.exists(output_path) and not force:
      raise ValueError('Output file {0} already present, use force to overwrite'.\
                       format(output_path))

    flagstat_cmd = \
      [quote(samtools_exe),
       'flagstat',
       '-@{0}'.format(quote(str(threads))),
       quote(bam_file)
      ]
    if dry_run:
      return flagstat_cmd

    with open(output_path,'w') as fp:
      with subprocess.Popen(flagstat_cmd, stdout=subprocess.PIPE) as proc:
        fp.write(proc.stdout.read().decode('utf-8'))                            # write bam flagstat output

    return output_path,flagstat_cmd
  except:
    raise


def run_bam_idxstat(samtools_exe,bam_file,output_dir,output_prefix=None,
                    force=False,dry_run=False):
  '''
  A function for running samtools index stats generation
  
  :param samtools_exe: samtools executable path
  :param bam_file: A bam filepath with / without index. Index file will be created if its missing
  :param output_dir: Bam idxstats output directory path
  :param output_prefix: Output file prefix, default None
  :param force: Output idxstats file will be overwritten if force is True, default False
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :returns: Output file path and a list containing samtools command
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=bam_file)                                          # check bam file
    if not dry_run:
      _check_bam_index(\
        samtools_exe=samtools_exe,
        bam_file=bam_file)                                                      # generate bam index
    if output_prefix is None:
      output_prefix=os.path.basename(bam_file)

    output_path = \
      '{0}.{1}.{2}'.\
      format(output_prefix,'idxstats','txt')                                    # get output filename
    output_path = \
      os.path.join(\
        output_dir,
        output_path)                                                            # get complete output path
    if not os.path.exists(output_dir):
      raise IOError('Output path {0} not found'.format(output_dir))

    if os.path.exists(output_path) and not force:
      raise ValueError('Output file {0} already present, use force to overwrite'.\
                       format(output_path))

    idxstat_cmd = \
      [quote(samtools_exe),
       'idxstats',
       quote(bam_file)
      ]
    if dry_run:
      return idxstat_cmd

    with open(output_path,'w') as fp:
      with subprocess.Popen(idxstat_cmd, stdout=subprocess.PIPE) as proc:
        fp.write(proc.stdout.read().decode('utf-8'))                            # write bam flagstat output

    return output_path,idxstat_cmd
  except:
    raise


def run_sort_bam(samtools_exe,input_bam_path,output_bam_path,sort_by_name=False,
                 threads=1,force=False,dry_run=False,cram_out=False,index_output=True):
  '''
  A function for sorting input bam file and generate a output bam
  
  :param samtools_exe: samtools executable path
  :param input_bam_path: A bam filepath
  :param output_bam_path: A bam output filepath
  :param sort_by_name: Sort bam file by read_name, default False (for coordinate sorting)
  :param threads: Number of threads to use for sorting, default 1
  :param force: Output bam file will be overwritten if force is True, default False
  :param cram_out: Output cram file, default False
  :param index_output: Index output bam, default True
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :return: None
  '''
  try:
    check_file_path(samtools_exe)
    _check_bam_file(bam_file=input_bam_path)
    sort_cmd = \
      [quote(samtools_exe),
       'sort',
       '-@{0}'.format(quote(str(threads)))
      ]
    if sort_by_name:
      sort_cmd.append('-n')                                                     # sorting by read name

    if cram_out:
      sort_cmd.append('--output-fmt CRAM')
    else:
      sort_cmd.append('--output-fmt BAM')

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_bam = \
      os.path.join(\
        temp_dir,
        os.path.basename(output_bam_path))

    sort_cmd.extend(['-o',quote(temp_bam)])
    sort_cmd.append(quote(input_bam_path))
    if dry_run:
      return sort_cmd

    copy_local_file(\
      source_path=temp_bam,
      destinationa_path=output_bam_path,
      force=force)                                                              # copy output bam
    remove_dir(temp_dir)                                                        # remove temp dir
    if cram_out:
      _check_cram_file(output_bam_path)
    else:
      _check_bam_file(output_bam_path)

    if index_output:
      index_bam_or_cram(\
        samtools_exe=samtools_exe,
        input_path=output_bam_path,
        threads=threads)
  except:
    raise


def merge_multiple_bam(samtools_exe,input_bam_list,output_bam_path,sorted_by_name=False,
                       threads=1,force=False,dry_run=False,index_output=True):
  '''
  A function for merging multiple input bams to a single output bam
  
  :param samtools_exe: samtools executable path
  :param input_bam_list: A file containing list of bam filepath
  :param output_bam_path: A bam output filepath
  :param sorted_by_name: Sort bam file by read_name, default False (for coordinate sorted bams)
  :param threads: Number of threads to use for merging, default 1
  :param force: Output bam file will be overwritten if force is True, default False
  :param index_output: Index output bam, default True
  :param dry_run: A toggle for returning the samtools command without actually running it, default False
  :return: samtools command
  '''
  try:
    check_file_path(samtools_exe)
    check_file_path(input_bam_list)
    with open(input_bam_list,'r') as fp:
      for bam in fp:
        check_file_path(bam.strip())

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_bam = \
      os.path.join(\
        temp_dir,
        os.path.basename(output_bam_path))
    merge_cmd = \
      [quote(samtools_exe),
       'merge',
       '--output-fmt','BAM',
       '--threads',quote(str(threads)),
       '-b',quote(input_bam_list)
      ]
    if sorted_by_name:
      merge_cmd.append('-n')                                                    # Input files are sorted by read name

    merge_cmd.append(temp_bam)
    if dry_run:
      return merge_cmd

    subprocess.check_call(merge_cmd)                                            # run samtools merge
    copy_local_file(\
      source_path=temp_bam,
      destinationa_path=output_bam_path,
      force=force)                                                              # copy bamfile
    remove_dir(temp_dir)                                                        # remove temp dir
    _check_bam_file(output_bam_path)
    if index_output:
      index_bam_or_cram(\
        samtools_exe=samtools_exe,
        input_path=output_bam_path,
        threads=threads)
    return merge_cmd
  except:
    raise
