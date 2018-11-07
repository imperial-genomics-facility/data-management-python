import os,subprocess
from shlex import quote
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

  def run_mem(self,mem_cmd='mem',parameter_options={"-M":""},samtools_cmd='view',
              dry_run=False):
    '''
    A method for running Bwa mem and generate output alignment
    
    :param mem_cmd: Bwa mem command, default mem
    :param option_list: List of bwa mem option, default -M
    :param samtools_cmd: Samtools view command, default view
    :param dry_run: A toggle for returning the bwa cmd without running it, default False
    :return: A alignment file path and bwa run cmd
    '''
    try:
      self._run_checks()                                                        # check input params
      read1_list,read2_list=identify_fastq_pair(input_list=self.input_fastq_list) # fetch input files
      temp_dir=get_temp_dir(use_ephemeral_space=True)
      bwa_cmd=[
        quote(self.bwa_exe),
        quite(mem_cmd),
        '-t',quote(str(self.thread)),
      ]
      if isinstance(parameter_options,dict) and \
         len(parameter_options)>0:
        parameter_options=[quote(str(field))
                           for key,val in parameter_options
                             for field in [key,val]
                               if field != '']                                  # flatten param list
        bwa_cmd.extend(parameter_options)                                       # add mem specific options

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
          quote('--threads'),quote(str(self.thread)),
          quote('-bo'),quote(temp_output_path)
          ]
        if dry_run:
          return bwa_cmd,samtools_cmd                                           # return bwa and samtools cmd

        with subprocess.Popen(bwa_cmd, stdout=subprocess.PIPE) as proc:
          proc2=subprocess.Popen(samtools_cmd, stdin=proc.stdout)

      else:
        temp_output_path=os.path.join(temp_dir,
                                      '{0}.sam'.format(self.output_prefix))     # sam output
        if dry_run:
          return bwa_cmd

        with open(temp_output_path,'w') as sam:
          with subprocess.Popen(bwa_cmd, stdout=subprocess.PIPE) as proc:
            sam.write(proc.stdout.read().decode('utf-8'))                       # writing sam output

      if os.path.exists(temp_output_path):
          final_output_file=os.path.join(self.output_dir,
                                         os.path.basename(temp_output_path))
          copy_local_file(source_path=temp_output_path,
                          destinationa_path=final_output_file)
      else:
        raise IOError('Alignment temp output missing')

      return final_output_file,bwa_cmd
    except:
      raise
