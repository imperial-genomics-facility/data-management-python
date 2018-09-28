import os,subprocess
from shlex import quote,split
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file
from igf_data.utils.fastq_utils import identify_fastq_pair

class BWA_util:
  def __init__(self,bwa_exe,samtools_exe,ref_genome,input_fastq_list,output_dir,
               output_prefix,bam_output=True,thread=1):
    self.bwa_exe=bwa_exe
    self.samtools_exe=samtools_exe
    self.ref_genome=ref_genome
    self.input_fastq_list=input_fastq_list
    self.output_dir=output_dir
    self.output_prefix=output_prefix
    self.bam_output=bam_output
    self.thread=thread

  def _run_checks(self):
    '''
    An internal method for running initial checks before bwa run
    '''
    try:
      check_file_path(self.bwa_exe)
      if self.bam_output:
        check_file_path(self.samtools_exe)

      for file in self.input_fastq_list:
        check_file_path(file)

      check_file_path(self.output_dir)
      if len(self.input_fastq_list) > 2:
        raise ValueError('Expecting max 2 fastq files, got {0}'.\
                         format(len(self.input_fastq_list)))

    except:
      raise

  def run_mem(self,mem_cmd='mem',option_list=['-M'],samtools_cmd='view'):
    '''
    A method for running Bwa mem and generate output alignment
    
    :param mem_cmd: Bwa mem command, default mem
    :param option_list: List of bwa mem option, default -M
    :param samtools_cmd: Samtools view command, default view
    :return: A alignment file path
    '''
    try:
      self._run_checks()                                                        # check input params
      read1_list,read2_list=identify_fastq_pair(input_list=self.input_fastq_list) # fetch input files
      temp_dir=get_temp_dir()
      bwa_cmd=[
        quote(self.bwa_exe),
        quite(mem_cmd),
        '-t',quote(self.thread),
      ]
      if isinstance(option_list,list) and \
         len(option_list)>0:
        option_list=split(option_list)                                          # split option lists
        option_list=[quote(opt) for opt in option_list]                         # wrap options in quotes
        bwa_cmd.append(option_list)                                             # add mem specific options

      bwa_cmd.append(quote(self.ref_genome))
      bwa_cmd.append(quote(read1_list[0]))                                      # add read 1
      if len(read2_list) > 0:
        bwa_cmd.append(quote(read2_list[0]))                                    # add read 2

      if self.bam_output:
        temp_output_path=os.path.join(temp_dir,
                                      '{0}.bam'.format(self.output_prefix))     # bam output
        samtools_cmd=[
          quote(self.samtools_exe),
          quote(samtools_cmd),
          quote('--threads'),quote(self.thread),
          quote('-bo'),quote(temp_output_path)
          ]
        with subprocess.Popen(bwa_cmd, stdout=PIPE) as proc:
          proc2=subprocess.Popen(samtools_cmd, stdin=proc.stdout)

      else:
        temp_output_path=os.path.join(temp_dir,
                                      '{0}.sam'.format(self.output_prefix))     # sam output
        with open(temp_output_path,'w') as sam:
          with subprocess.Popen(bwa_cmd, stdout=PIPE) as proc:
            sam.write(proc.stdout.read())

      if os.path.exists(temp_output_path):
          final_output_file=os.path.join(self.output_dir,
                                         os.path.basename(temp_output_path))
          copy_local_file(source_path=temp_output_path,
                          destinationa_path=final_output_file)
      else:
        raise IOError('Alignment temp output missing')

      return final_output_file
    except:
      raise
