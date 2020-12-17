import os,subprocess
from igf_data.utils.fileutils import check_file_path,get_temp_dir
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd

def run_cutadapt(
      read1_fastq_in,read1_fastq_out,read2_fastq_in=None,read2_fastq_out=None,dry_run=False,
      cutadapt_exe='cutadapt',singularity_image_path=None,cutadapt_options=('--cores=1',)):
  try:
    check_file_path(read1_fastq_in)
    if read2_fastq_in is not None:
      check_file_path(read2_fastq_in)
    if singularity_image_path is not None:
      check_file_path(singularity_image_path)
    cmd = [cutadapt_exe]
    cmd.\
      extend(cutadapt_options)
    if read2_fastq_in is not None:
      if read2_fastq_out is None:
        raise ValueError(
                'Missing R2 output file for input {0}'.\
                  format(read2_fastq_in))
      cmd.\
        append('-p {0}'.format(read2_fastq_out))
    cmd.\
      extend([
        '-o {0}'.format(read1_fastq_out),
        read1_fastq_in])
    if read2_fastq_in is not None:
      cmd.\
        append(read2_fastq_in)
    cmd = \
      ' '.join(cmd)
    if dry_run:
      return cmd
    if singularity_image_path is not None:
      check_file_path(singularity_image_path)
      log_dir = get_temp_dir(use_ephemeral_space=True)
      bind_dir_list = [
        os.path.dirname(read1_fastq_out),
        os.path.dirname(read1_fastq_in)]
      if read2_fastq_in is not None:
        bind_dir_list.\
          extend([
              os.path.dirname(read2_fastq_in),
              os.path.dirname(read2_fastq_out)])
      bind_dir_list = \
        list(set(bind_dir_list))
      execute_singuarity_cmd(
        image_path=singularity_image_path,
        command_string=cmd,
        log_dir=log_dir,
        task_id='cutadapt',
        bind_dir_list=bind_dir_list)
    else:
      subprocess.check_call(cmd,shell=True)
  except Exception as e:
    raise ValueError(
            'Failed to run cutadapt, error: {0}'.\
              format(e))