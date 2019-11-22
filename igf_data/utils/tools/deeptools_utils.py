import os,io
import pandas as pd
from shlex import quote
from contextlib import redirect_stdout



from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path,copy_local_file

def run_plotCoverage(bam_files,output_raw_counts,plotcov_stdout,output_plot=None,
                     blacklist_file=None,thread=1,params_list=None,dry_run=False):
  '''
  A function for running Deeptools plotCoverage

  :param bam_files: A list of indexed bam files
  :param output_raw_counts: Output raw count filepath
  :param plotcov_stdout: Output path of plotCoverage stdout logs
  :param output_plot: Output plots filepath, default None
  :param blacklist_file: Input blacklist region filepath, default None
  :param thread: Number of threads to use, default 1
  :param params_list: Additional deeptools plotCoverage params as list, default None
  :param dry_run: Return Deeptools command list without running it
  :returns: Deeptools command list
  '''
  try:
    if len(bam_files)==0:
      raise ValueError('No bamfiles found to generate coverage plot data')

    plotcov_args = ['--bamfiles']                                               # prepare to add input bams to args
    for path in bam_files:
      check_file_path(path)                                                     # check input bams
      plotcov_args.append(quote(path))                                          # adding input bams

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_output_raw_counts = \
      os.path.join(temp_dir,os.path.basename(output_raw_counts))                # path for temp raw counts
    temp_plotcov_stdout = \
      os.path.join(temp_dir,os.path.basename(plotcov_stdout))                   # path for temp raw counts

    plotcov_args.\
    extend(\
      ["--numberOfProcessors",quote(str(thread)),
       "--outRawCounts",temp_output_raw_counts
      ])
    if output_plot is not None:
      temp_output_plot = \
        os.path.join(temp_dir,os.path.basename(output_plot))                    # path for temp raw counts
      plotcov_args.extend(["--plotFile",temp_output_plot])

    if blacklist_file is not None:
      check_file_path(blacklist_file)
      plotcov_args.extend(["--blackListFileName",quote(blacklist_file)])

    if (params_list is not None or \
        params_list !='') and \
       isinstance(params_list,list) and \
       len(params_list) > 0:
      params_list = [quote(param)
                       for param in params_list]
      plotcov_args.extend(params_list)                                          # add additional params to the list

    if dry_run:
      return plotcov_args

    from deeptools.plotCoverage import main as plotCoverage_main
    f = io.StringIO()
    with redirect_stdout(f):
      plotCoverage_main(plotcov_args)

    stdout_logs = f.getvalue()
    with open(temp_plotcov_stdout,'w') as fp:
      fp.write(stdout_logs)

    copy_local_file(\
      source_path=temp_plotcov_stdout,
      destinationa_path=plotcov_stdout)
    copy_local_file(\
      source_path=temp_output_raw_counts,
      destinationa_path=output_raw_counts)
    if output_plot is not None:
      copy_local_file(\
        source_path=temp_output_plot,
        destinationa_path=output_plot)

    remove_dir(temp_dir)                                                        # clean up temp dir
    plotcov_args.insert(0,'plotCoverage')                                       # fix for deeptools commandline
    return plotcov_args
  except:
    raise


def run_bamCoverage(bam_files,output_file,blacklist_file=None,thread=1,dry_run=False,
                    params_list=("--outFileFormat","bigwig")):
  '''
  A function for running Deeptools bamCoverage

  :param bam_files: A list of bam files to run tool,expecting only one file
  :param output_file: Ouput filepath for the coverage plot
  :param blacklist_file: Input blacklist region filepath, default None
  :param thread: Number of threads to use, default 1
  :param dry_run: Return Deeptools command list without running it
  :param params_list: Additional deeptools plotCoverage params as list, default ("--outFileFormat","bigwig")
  :returns: Deeptools command as list
  '''
  try:
    if len(bam_files)==0:
      raise ValueError('No bamfiles found to generate coverage data')

    if len(bam_files)>1:
      raise ValueError('Expecting only one bam for bamCoverage tools, found : {0}'.\
                       format(len(bam_files)))

    bamcov_args = ['--bam']                                                     # prepare to add input bams to args
    for path in bam_files:
      check_file_path(path)                                                     # check input bams
      bamcov_args.append(quote(path))                                           # adding input bams

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_output = \
      os.path.join(temp_dir,os.path.basename(output_file))
    bamcov_args.\
    extend(\
      ["--numberOfProcessors",quote(str(thread)),
       "--outFileName",temp_output])
    if blacklist_file is not None:
      check_file_path(blacklist_file)
      bamcov_args.extend(["--blackListFileName",quote(blacklist_file)])

    
    if (params_list is not None or \
        params_list != '') and \
        (isinstance(params_list,list) or \
         isinstance(params_list,tuple)) and \
        len(params_list)>0:
      params_list = list(params_list)
      if len(params_list) > 0:
        params_list = \
          [quote(param)
             for param in params_list]
        bamcov_args.extend(params_list)                                         # add additional params to the list

    if dry_run:
      return bamcov_args

    from deeptools.bamCoverage import main as bamCoverage_main
    bamCoverage_main(bamcov_args)                                               # generate bam coverage file
    copy_local_file(\
      source_path=temp_output,
      destinationa_path=output_file)                                            # copy output file
    remove_dir(temp_dir)                                                        # clean up temp dir
    bamcov_args.insert(0,'bamCoverage')                                        # fix for deeptools commandline
    return bamcov_args
  except:
    raise


def run_plotFingerprint(bam_files,output_raw_counts,output_matrics,output_plot=None,dry_run=False,
                        blacklist_file=None,thread=1,params_list=None):
  '''
  A function for running Deeptools plotFingerprint

  :param bam_files: A list of indexed bam files
  :param output_raw_counts: Output raw count filepath
  :param output_matrics: Output matrics file
  :param output_plot: Output plots filepath, default None
  :param blacklist_file: Input blacklist region filepath, default None
  :param thread: Number of threads to use, default 1
  :param dry_run: Return Deeptools command list without running it
  :param params_list: Additional deeptools plotCoverage params as list, default None
  :returns: Deeptools command list
  '''
  try:
    if len(bam_files)==0:
      raise ValueError('No bamfiles found to generate coverage plot data')

    plotFgCov_args = ['--bamfiles']                                             # prepare to add input bams to args
    for path in bam_files:
      check_file_path(path)                                                     # check input bams
      plotFgCov_args.append(quote(path))                                        # adding input bams

    temp_dir = get_temp_dir(use_ephemeral_space=False)
    temp_output_raw_counts = \
      os.path.join(temp_dir,os.path.basename(output_raw_counts))                # path for temp raw counts
    temp_output_matrics =\
      os.path.join(temp_dir,os.path.basename(output_matrics))                   # path for temp matrics counts
    plotFgCov_args.\
    extend(\
      ["--numberOfProcessors",quote(str(thread)),
       "--outRawCounts",temp_output_raw_counts,
       "--outQualityMetrics",temp_output_matrics
      ])
    if output_plot is not None:
      temp_output_plot = \
        os.path.join(temp_dir,os.path.basename(output_plot))                    # path for temp raw counts
      plotFgCov_args.extend(["--plotFile",temp_output_plot])

    if blacklist_file is not None:
      check_file_path(blacklist_file)
      plotFgCov_args.extend(["--blackListFileName",quote(blacklist_file)])

    if (params_list is not None or \
        params_list != '') and \
       isinstance(params_list,list) and \
       len(params_list) > 0:
      params_list = [quote(param)
                       for param in params_list]
      plotFgCov_args.extend(params_list)                                        # add additional params to the list

    if dry_run:
      return plotFgCov_args

    from deeptools.plotFingerprint import main as plotFingerprint_main
    plotFingerprint_main(plotFgCov_args)
    copy_local_file(\
      source_path=temp_output_raw_counts,
      destinationa_path=output_raw_counts)
    copy_local_file(\
      source_path=temp_output_matrics,
      destinationa_path=output_matrics)
    if output_plot is not None:
      copy_local_file(\
        source_path=temp_output_plot,
        destinationa_path=output_plot)
    remove_dir(temp_dir)
    plotFgCov_args.insert(0,'plotFingerprint')                                  # fix for deeptools commandline
    return plotFgCov_args
  except:
    raise