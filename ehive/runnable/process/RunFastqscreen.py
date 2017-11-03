import os, subprocess,fnmatch
from shutil import copytree
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunFastqscreen(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'force_overwrite':True,
      'tag':None,
      'fastqscreen_dir_label':'fastqscreen',
      'fastqscreen_exe':'fastqscreen',
      'fastqscreen_conf':None,
      'fastqscreen_options':{'--aligner':'bowtie2',
                             '--force':'',
                             '--quiet':'',
                             '--subset':'100000',
                             '--threads':'1'},
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      fastqscreen_exe=self.param_required('fastqscreen_exe')
      fastqscreen_conf=self.param_required('fastqscreen_conf')
      tag=self.param_required('tag')
      fastqscreen_options=self.param('fastqscreen_options')
      force_overwrite=self.param('force_overwrite')
      fastqscreen_dir_label=self.param('fastqscreen_dir_label')
      
      lane_index_info=os.path.basename(os.path.dirname(fastq_file))             # get the lane and index length info
      fastq_file_label=os.path.basename(fastq_file).replace('.fastq.gz','')
      
      fastqscreen_result_dir=os.path.join(base_results_dir, \
                                          project_name, \
                                          fastqscreen_dir_label, \
                                          seqrun_date, \
                                          flowcell_id, \
                                          lane_index_info,\
                                          tag,\
                                          fastq_file_label)                     # result dir path is generic
      
      if os.path.exists(fastqscreen_result_dir) and force_overwrite:
        remove_dir(fastqscreen_result_dir)                                      # remove existing output dir if force_overwrite is true
      
      if not os.path.exists(fastqscreen_result_dir):
        os.makedirs(fastqscreen_result_dir,mode=0o775)                          # create output dir if its not present
      
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      filename=os.path.basename(fastq_file).replace('.fastq.gz','')             # get fastqfile base name and remove file ext
      fastqscreen_output=os.path.join(temp_work_dir,filename)
      os.mkdir(fastqscreen_output)                                              # create fastqc output dir
      
      fastqscreen_param=self.format_tool_options(fastqscreen_options)           # format fastqc params
      fastqscreen_cmd=[fastqscreen_exe,
                       '-conf',fastqscreen_conf,
                       '--outdir',fastqscreen_output,
                      ]                                                         # fastqscreen base parameters
      fastqscreen_cmd.extend(fastqscreen_param)                                 # add additional parameters
      fastqscreen_cmd.extend(fastq_file)                                        # fastqscreen input file
      subprocess.check_call(fastqscreen_cmd)                                    # run fastqscreen
      
      copytree(fastqscreen_output,fastqscreen_result_dir)                       # copy fastqscreen output files
      
      fastqscreen_stat=None
      fastqscreen_html=None
      fastqscreen_png=None
      
      for root,dirs,files in os.walk(top=fastqscreen_result_dir):
        for file in files:
          if fnmatch.fnmatch(file, '*.txt'):
            fastqscreen_stat=os.path.join(root,file)
          
          if fnmatch.fnmatch(file, '*.html'):
            fastqscreen_html=os.path.join(root,file)
            
          if fnmatch.fnmatch(file, '*.png'):
            fastqscreen_png=os.path.join(root,file)
            
      self.param('dataflow_params',{'fastqscreen_html':fastqscreen_html, \
                                    'fastqscreen': \
                                    {'fastqscreen_path':fastqscreen_result_dir,
                                     'fastqscreen_stat':fastqscreen_stat,
                                     'fastqscreen_html':fastqscreen_html,
                                     'fastqscreen_png':fastqscreen_png}})       # set dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise