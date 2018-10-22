import os,subprocess,fnmatch
from shlex import quote
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file

class RSEM_utils:
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
    :returns: RSEM commandline and output file list
    '''
    try:
      rsem_exe=os.path.join(self.rsem_exe_dir,
                            'rsem-calculate-expression')
      check_file_path(rsem_exe)
      check_file_path(self.input_bam)
      temp_dir=get_temp_dir()
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
      for file in os.listdir(temp_dir):
        if fnmatch.fnmatch(file,'*.results'):
          copy_local_file(source_path=os.path.join(temp_dir,file),
                          destinationa_path=os.path.join(output_dir,file),
                          force=force)

      return rsem_cmd,rsem_output_list
    except:
      raise