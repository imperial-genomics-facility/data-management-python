import os,subprocess,fnmatch
from shlex import quote
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

class RSEM_utils:
  '''
  A python wrapper for running RSEM tool

  :param rsem_exe_dir: RSEM executable path
  :param reference_rsem: RSEM reference transcriptome path
  :param input_bam: Input bam file path for RSEM
  :param threads: No. of threads for RSEM run, default 1
  :param memory_limit: Memory usage limit for RSEM, default 4Gb
  '''
  def __init__(self,rsem_exe_dir,reference_rsem,input_bam,threads=1,
               memory_limit=4000):
    self.rsem_exe_dir=rsem_exe_dir
    self.reference_rsem=reference_rsem
    self.input_bam=input_bam
    self.threads=threads
    self.memory_limit=memory_limit

  def run_rsem_calculate_expression(self,output_dir,output_prefix,paired_end=True,
                                    strandedness='reverse',options=None,force=True):
    '''
    A method for running RSEM rsem-calculate-expression tool from alignment file
    
    :param output_dir: A output dir path
    :param output_prefix: A output file prefix
    :param paired_end: A toggle for paired end data, default True
    :param strandedness: RNA strand information, default reverse for Illumina TruSeq
                         allowed values are none, forward and reverse
    :param options: A dictionary for rsem run, default None
    :param force: Overwrite existing data if force is True, default False
    :returns: RSEM commandline, output file list and logfile
    '''
    try:
      rsem_exe=os.path.join(self.rsem_exe_dir,
                            'rsem-calculate-expression')
      check_file_path(rsem_exe)
      check_file_path(self.input_bam)
      temp_dir=get_temp_dir(use_ephemeral_space=True)
      temp_output=os.path.join(temp_dir,output_prefix)
      rsem_cmd=[quote(rsem_exe),
                '--quiet',
                '--no-bam-output',
                '--alignments',
                '--strandedness',quote(strandedness),
                '--num-threads',quote(str(self.threads)),
                '--ci-memory',quote(str(self.memory_limit)),
                '--estimate-rspd'
               ]
      if paired_end:
        rsem_cmd.append('--paired-end')

      if isinstance(options,dict) and \
         len(options)>0:
        rsem_cmd.extend([key if val=='' else '{0} {1}'.format(key,val)
                          for key,val in options.items()])

      rsem_cmd.append(self.input_bam)
      if self.reference_rsem is None:
        raise ValueError('No reference genome found for Rsem')

      rsem_cmd.append(self.reference_rsem)
      rsem_cmd.append(temp_output)
      subprocess.check_call(rsem_cmd)
      rsem_output_list=list()
      rsem_log_file=None
      for file in os.listdir(temp_dir):
        if fnmatch.fnmatch(file,'*.results'):
          rsem_output_list.append(os.path.join(output_dir,file))                # add output files to the list
          copy_local_file(source_path=os.path.join(temp_dir,file),
                          destinationa_path=os.path.join(output_dir,file),
                          force=force)                                          # copy output files to work dir

      for root, dirs, files in os.walk(temp_dir):
        for file in files:
          if fnmatch.fnmatch(file,'*.cnt'):
            rsem_log_file=os.path.join(output_dir,file)
            copy_local_file(source_path=os.path.join(root,file),
                            destinationa_path=rsem_log_file,
                            force=force)

      if len(rsem_output_list)==0 or \
         rsem_log_file is None:
        raise ValueError('Rsem output file not found')

      return rsem_cmd,rsem_output_list,rsem_log_file
    except:
      raise