import os,subprocess
from igf_data.utils.fileutils import check_file_path,get_temp_dir
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd

def run_bcl2fastq(
      runfolder_dir,output_dir,samplesheet_path=None,bases_mask=None,
      threads=4,dry_run=False,reports_dir=None,bcl2fast1_exe='bcl2fastq',
      singularity_image_path=None,tiles=(),
      options=['--barcode-mismatches 1',
               '--auto-set-to-zero-barcode-mismatches',
               '--create-fastq-for-index-reads']):
  """
  A function for running Bcl2Fastq command

  :param runfolder_dir: Path to sequencing run folder
  :param output_dir: Path to output dir
  :param samplesheet_path: Samplesheet path, default None to use 'SampleSheet.csv'
  :param bases_mask: Bases mask for demultiplexing, default None for using global mask
  :param threads: Number of threads to use, default 4
  :param dry_run: A toggle for dry run to return the command, default False
  :param reports_dir: Path to the output dir for Reports, default None
  :param bcl2fast1_exe: Path to Bcl2Fastq executable, default 'bcl2fastq'
  :param singularity_image_path: Path to Singularity image container, default None
  :param tiles: A list of tiles to demultiplex, default empty list for all the tiles
  :param options: List of options to use for Bcl2Fastq execution, default are the following:

    * --barcode-mismatches 1
    * --auto-set-to-zero-barcode-mismatches
    * --create-fastq-for-index-reads

  :returns: A string of the Bcl2Fastq command
  """
  try:
    check_file_path(runfolder_dir)
    check_file_path(output_dir)
    command = [
        bcl2fast1_exe,
        '--runfolder-dir',runfolder_dir,
        '--output-dir',output_dir,
        '-r',str(threads),
        '-w',str(threads),
        '-p',str(threads)]
    command.\
      extend(options)
    if samplesheet_path is not None:
      check_file_path(samplesheet_path)
      command.\
        extend(['--sample-sheet',samplesheet_path])
    if bases_mask is not None:
      command.\
        extend(['--use-bases-mask',bases_mask])
    if reports_dir is not None:
      command.\
        extend(['--reports-dir',reports_dir])
    if len(tiles) > 0:
      command.\
        extend(['--tiles',','.join(tiles)])
    command = \
      ' '.join(command)
    log_dir = get_temp_dir(use_ephemeral_space=True)
    if dry_run:
      return command
    if singularity_image_path is not None:
      check_file_path(singularity_image_path)
      bind_dir_list = [
        runfolder_dir,
        output_dir]
      if samplesheet_path is not None:
        bind_dir_list.\
          append(samplesheet_path)
      if reports_dir is not None:
        bind_dir_list.\
          append(reports_dir)
      execute_singuarity_cmd(
        image_path=singularity_image_path,
        command_string=command,
        log_dir=log_dir,
        task_id='bcl2fastq',
        bind_dir_list=bind_dir_list)
    else:
      subprocess.check_call(command)
    return command
  except Exception as e:
    raise ValueError(
            'Failed to run bcl2fastqn command: {0}, error: {1}'.\
              format(command,e))