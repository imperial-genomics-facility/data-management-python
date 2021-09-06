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
  :param enable_polyg_trim: Enable poly G trim for NextSeq and NovaSeq, default False
  :param log_output_prefix: Output prefix for log file, default None
  :param use_ephemeral_space: A toggle for temp dir, default 0
  :param fastp_options_list: A list of options for running fastp, default
                               -a auto
                               --qualified_quality_phred 15
                               --length_required=15
  '''
  def __init__(self,fastp_exe,input_fastq_list,output_dir,run_thread=1,enable_polyg_trim=False,
               split_by_lines_count=5000000,log_output_prefix=None,use_ephemeral_space=0,
               fastp_options_list=('-a','auto',
                                   '--qualified_quality_phred=15',
                                   '--length_required=15')):
    self.fastp_exe = fastp_exe
    self.input_fastq_list = input_fastq_list
    self.output_dir = output_dir
    self.run_thread = run_thread
    self.enable_polyg_trim = enable_polyg_trim
    self.split_by_lines_count = split_by_lines_count
    self.log_output_prefix = log_output_prefix
    self.use_ephemeral_space = use_ephemeral_space
    self.fastp_options_list = fastp_options_list

  def _run_checks(self):
    '''
    An internal method for running initial checks before fastp run
    '''
    try:
      check_file_path(self.fastp_exe)
      if not isinstance(self.input_fastq_list, list):
        raise ValueError('No input fastq list found: {0}'.format(self.input_fastq_list))

      if  isinstance(self.run_thread, int):
        self.run_thread = str(self.run_thread)                                  # convert run thread param to str

      for file in self.input_fastq_list:
        check_file_path(file)

      check_file_path(self.output_dir)
      if len(self.input_fastq_list) > 2:
        raise ValueError('Expecting max 2 fastq files, got {0}'.\
                         format(len(self.input_fastq_list)))

    except:
      raise

  def run_adapter_trimming(self,split_fastq=False,force_overwrite=True):
    '''
    A method for running fastp adapter trimming
    
    :param split_fastq: Split fastq output files by line counts, default False
    :pram force_overwrite: A toggle for overwriting existing file, default True
    :returns: A list for read1 files, list of read2 files and a html report file
              and the fastp commandline
    '''
    try:
      temp_dir = \
        get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)              # create temp dir in ephemeral space
      self._run_checks()
      read1_list, read2_list = \
        identify_fastq_pair(input_list=self.input_fastq_list)                   # fetch input files
      log_output_prefix = self.log_output_prefix
      if log_output_prefix is None:
        log_output_prefix = os.path.basename(read1_list[0])

      cmd = \
        [self.fastp_exe,
         '--in1',quote(read1_list[0]),
         '--out1',quote(os.path.join(temp_dir,
                                     os.path.basename(read1_list[0]))),
         '--html',quote(os.path.join(temp_dir,'{0}.html'.\
                                     format(os.path.basename(read1_list[0])))),
         '--json',quote(os.path.join(temp_dir,'{0}.fastp.json'.\
                                     format(log_output_prefix))),
         '--report_title',quote(log_output_prefix),
         '--thread',quote(self.run_thread)
        ]
      if len(read2_list) > 0:                                                   # add read 2 options
        cmd.extend(['--in2',quote(read2_list[0])])
        cmd.extend(['--out2',
                    quote(os.path.join(temp_dir,
                                      os.path.basename(read2_list[0])))])

      if isinstance(self.fastp_options_list,list) and \
         len(self.fastp_options_list)>0:
        fastp_options_list = \
          [quote(line)
            for line in self.fastp_options_list
          ]                                                                     # wrap options in quotes
        cmd.extend(fastp_options_list)                                          # add fastp options

      if self.enable_polyg_trim:
        cmd.append('--trim_poly_g')                                             # turning on polyg trim for NextSeq and NovaSeq data

      if split_fastq:
        cmd.append('--split_by_lines',
                   quote(self.split_by_lines_count))

      subprocess.check_call(' '.join(cmd),shell=True)                           # run fastp
      fastq_pattern = re.compile(r'\S+\.fastq(\.gz)?$')
      html_pattern = re.compile(r'\S+\.html$')
      json_pattern = re.compile(r'\S+\.json$')
      output_fastq_list = list()
      output_html_file = ''
      output_json_file = ''
      for out_file in os.listdir(temp_dir):
        output_file = os.path.join(self.output_dir,out_file)
        if re.match(fastq_pattern,out_file):
          output_fastq_list.append(output_file)
          copy_local_file(\
            source_path=os.path.join(temp_dir,out_file),
            destination_path=output_file,
            force=force_overwrite)                                              # copy fastq file to output dir

        if re.match(html_pattern,out_file):
          output_html_file = output_file
          copy_local_file(\
            source_path=os.path.join(temp_dir,out_file),
            destination_path=output_file,
            force=force_overwrite)                                              # copy html file to output dir

        if re.match(json_pattern,out_file):
          output_json_file = output_file
          copy_local_file(\
            source_path=os.path.join(temp_dir,out_file),
            destination_path=output_file,
            force=force_overwrite)                                              # copy json file to output dir

      if len(output_fastq_list) == 0:
        raise ValueError('No output fastq files found as fastp output')

      if output_html_file == '' or \
         output_json_file == '':
        raise ValueError('No fastp html or json report found')

      remove_dir(temp_dir)                                                      # clean up temp dir
      output_read1, output_read2 = \
          identify_fastq_pair(\
            input_list=output_fastq_list,
            sort_output=True,
            check_count=True)                                                   #identify fastq pairs and validate output fastq pairs
      return output_read1, output_read2, output_html_file, output_json_file, cmd
    except:
      if os.path.exists(temp_dir):
        remove_dir(temp_dir)                                                    # remove temp dir
      raise