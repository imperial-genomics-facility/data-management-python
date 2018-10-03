import os,subprocess,re
from shlex import quote,split
from igf_data.utils.fastq_utils import identify_fastq_pair
from igf_data.utils.fileutils import get_temp_dir,remove_dir,copy_local_file,check_file_path

class Fastp_utils:
  '''
  A class for running fastp tool for a list of input fastq files
  
  :param fastp_exe: A fastp executable path
  :param input_fastq_list: A list of input files
  :param output_dir: A output directory path
  :param split_by_lines_count: Number of entries for splitted fastq files, default 5000000
  :param run_thread: Number of threads to use, default 1
  :param fastp_options_list: A list of options for running fastp, default
                               --qualified_quality_phred 15
                               --length_required 15
  '''
  def __init__(self,fastp_exe,input_fastq_list,output_dir,run_thread=1,
               split_by_lines_count=5000000,fastp_options_list=['--qualified_quality_phred 15',
                                                                '--length_required 15']):
    self.fastp_exe=fastp_exe
    self.input_fastq_list=input_fastq_list
    self.output_dir=output_dir
    self.run_thread=run_thread
    self.split_by_lines_count=split_by_lines_count
    self.fastp_options=fastp_options

  def _run_checks(self):
    '''
    An internal method for running initial checks before fastp run
    '''
    try:
      check_file_path(self.fastp_exe)
      for file in self.input_fastq_list:
        check_file_path(file)

      check_file_path(self.output_dir)
      if len(self.input_fastq_list) > 2:
        raise ValueError('Expecting max 2 fastq files, got {0}'.\
                         format(len(self.input_fastq_list)))

    except:
      raise

  def run_adapter_trimming(self,split_fastq=False):
    '''
    A method for running fastp adapter trimming
    
    :param split_fastq: Split fastq output files by line counts, default False
    :returns: A list for read1 files, list of read2 files and a html report file
              and the fastp commandline
    '''
    try:
      self._run_checks()
      read1_list,read2_list=identify_fastq_pair(input_list=self.input_fastq_list) # fetch input files
      temp_dir=get_temp_dir()
      cmd=[self.fastp_exe,
           '--in1',quote(read1_list[0]),
           '--out1',quote(os.path.join(temp_dir,
                                       os.path.basename(read1_list[0]))),
           '--html',quote(os.path.join(temp_dir,'{0}.html'.\
                                       format(os.path.basename(read1_list[0])))),
           '--json',quote(os.path.join(temp_dir,'{0}.json'.\
                                       format(os.path.basename(read1_list[0])))),
           '--report_title',quote(os.path.basename(read1_list[0])),
           '--thread',quote(self.run_thread)
          ]
      if len(read2_list) > 0:                                                   # add read 2 options
        cmd.append('--in2',quote(read2_list[0]))
        cmd.append('--out2',
                   quote(os.path.join(temp_dir,
                                      os.path.basename(read2_list[0]))))

      if len(self.fastp_options)>0:
        fastp_options=split(fastp_options)                                      # split options
        fastp_options=[quote(opt) for opt in fastp_options]                     # wrap options in quotes
        cmd.extend(self.fastp_options)                                          # add fastp options

      if split_fastq:
        cmd.append('--split_by_lines',
                   quote(self.split_by_lines_count))

      subprocess.check_call(cmd,shell=False)                                    # run fastp
      fastq_pattern=re.compile(r'\S+\.fastq(\.\gz)?')
      html_pattern=re.compile(r'\S+\.html')
      output_fastq_list=list()
      output_html_file=''
      for out_file in os.listdir(temp_dir):
        output_file=os.path.join(self.output_dir,out_file)
        if re.match(out_file,fastq_pattern):
          output_fastq_list.append(output_file)
          copy_local_file(source_path=os.path.join(temp_dir,out_file),
                          destinationa_path=output_file)                        # copy fastq file to output dir

        if re.match(out_file,html_pattern):
          output_html_file=output_file
          copy_local_file(source_path=os.path.join(temp_dir,out_file),
                          destinationa_path=output_file)                        # copy fastq file to output dir

      if len(output_fastq_list) == 0:
        raise ValueError('No output fastq files found as fastp output')

      if output_html_file == '':
        raise ValueError('No fastp html report found')

      remove_dir(temp_dir)                                                      # clean up temp dir
      output_read1,output_read2=self._identify_fastq_pair(output_fastq_list)
      return output_read1,output_read2,output_html_file,cmd
    except:
      raise