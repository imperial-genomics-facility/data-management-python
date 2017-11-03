import os, subprocess,fnmatch
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunFastqc(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'force_overwrite':True,
      'fastqc_dir_label':'fastqc',
      'fastqc_exe':'fastqc',
      'fastqc_options':{'-q':'','--noextract':'','-f':'fastq','-k':'7','-t':'1'},
      'tag':None,
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      fastqc_exe=self.param_required('fastqc_exe')
      tag=self.param_required('tag')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      fastqc_options=self.param('fastqc_options')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      force_overwrite=self.param('force_overwrite')
      fastqc_dir_label=self.param('fastqc_dir_label')
      
      lane_index_info=os.path.basename(os.path.dirname(fastq_file))             # get the lane and index length info
      fastq_file_label=os.path.basename(fastq_file).replace('.fastq.gz','')
      
      fastqc_result_dir=os.path.join(base_results_dir, \
                                     project_name, \
                                     fastqc_dir_label, \
                                     seqrun_date, \
                                     flowcell_id, \
                                     lane_index_info,\
                                     tag,\
                                     fastq_file_label)                          # result dir path is generic
      
      if os.path.exists(fastqc_result_dir) and force_overwrite:
        remove_dir(fastqc_result_dir)                                           # remove existing output dir if force_overwrite is true
        
      if not os.path.exists(fastqc_result_dir):
        os.makedirs(fastqc_result_dir,mode=0o775)                               # create output dir if its not present
        
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      fastqc_output=os.path.join(temp_work_dir,fastq_file_label)
      os.mkdir(fastqc_output)                                                   # create fastqc output dir
      fastqc_param=self.format_tool_options(fastqc_options)                     # format fastqc params
      fastqc_cmd=[fastqc_exe, '-o',fastqc_output, '-d',temp_work_dir ]          # fastqc base parameters
      fastqc_cmd.extend(fastqc_param)                                           # add additional parameters
      fastqc_cmd.append(fastq_file)                                             # fastqc input file
      subprocess.check_call(fastqc_cmd)                                         # run fastqc
      
      fastqc_zip=None
      fastqc_html=None
      
      for root, dirs, files in os.walk(top=fastqc_output):
        for file in files:
          if fnmatch.fnmatch(file, '*.zip'):
            input_fastqc_zip=os.path.join(root,file)
            copy2(input_fastqc_zip,fastqc_result_dir)
            fastqc_zip=os.path.join(fastqc_result_dir,file)
          
          if fnmatch.fnmatch(file, '*.html'):
            input_fastqc_html=os.path.join(root,file)
            copy2(input_fastqc_html,fastqc_result_dir)
            fastqc_html=os.path.join(fastqc_result_dir,file)
          
      if fastqc_html is None or fastqc_zip is None:
        raise ValueError('Missing required values, fastqc zip: {0}, fastqc html: {1}'.\
                         format(fastqc_zip,fastqc_html))
      
      self.param('dataflow_params',{'fastqc_html':fastqc_html, \
                                    'fastqc':{'fastqc_path':fastqc_result_dir,
                                              'fastqc_zip':fastqc_zip,
                                              'fastqc_html':fastqc_html}})      # set dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise