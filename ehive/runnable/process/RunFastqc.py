import os, subprocess,fnmatch
from shutil import copytree
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunFastqc(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'overwrite_output':True,
      'fastqc_dir_label':'fastqc',
      'fastqc_module':'fastqc/0.11.2',
      'fastqc_exe':'fastqc',
      'fastqc_options':{'-q':'','--noextract':'','-f':'fastq','-k':7,'-t':1},
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      fastqc_exe=self.param_required('fastqc_exe')
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      fastqc_module=self.param('fastqc_module')
      fastqc_options=self.param('fastqc_options')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      sample_igf_id=self.param_required('sample_igf_id')
      run_igf_id=self.param_required('run_igf_id')
      overwrite_output=self.param('overwrite_output')
      fastqc_dir_label=self.param('fastqc_dir_label')
      
      fastqc_result_dir=os.path.join(base_results_dir, \
                                     project_name, \
                                     fastqc_dir_label, \
                                     sample_igf_id, \
                                     run_igf_id)
      if not os.path.exists(fastqc_result_dir):
        os.mkdir(fastqc_result_dir)                                             # create output dir if its not present
        
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      filename=os.path.basename(fastq_file)                                     # get fastqfile base name
      filename=filename.replace('.fastq.gz','')                                 # remove file ext
      fastqc_output=os.path.join(temp_work_dir,filename)
      os.mkdir(fastqc_output)                                                   # create fastqc output dir
      
      subprocess.check_call(['module','load',fastqc_module])                    # load fastqc specific settings
      fastqc_param=[[param,value] if value else [param] \
                       for param, value in fastqc_options.items()]              # remove empty values
      fastqc_param=[col for row in param for col in fastqc_param]               # flatten sub lists
      fastqc_cmd=[fastqc_exe,
                     '-o',fastqc_output,
                     '-d',temp_work_dir,
                     ]                                                          # fastqc base parameters
      fastqc_cmd.extend(fastqc_param)                                           # add additional parameters
      fastqc_cmd.extend(fastq_file)                                             # fastqc input file
      subprocess.check_call(fastqc_cmd)                                         # run fastqc
      
      if overwrite_output:
        rmdir(os.path.join(fastqc_result_dir,filename))                         # remove fastqc results if its already present
        
      copytree(fastqc_output,fastqc_result_dir)
      fastqc_zip=None
      fastqc_html=None
      
      fastqc_path=os.path.join(fastqc_result_dir,filename)
      
      for root,dirs,files in os.walk(top=fastqc_path):
        for file in files:
          if fnmatch.fnmatch(file, '*.zip'):
            fastqc_zip=os.path.join(root,file)
          
          if fnmatch.fnmatch(file, '*.html'):
            fastqc_html=os.path.join(root,file)
      
      self.param('dataflow_params',{'fastq_file':fastq_file, \
                                    'fastqc':{'fastqc_path':fastqc_path,
                                              'fastqc_zip':fastqc_zip,
                                              'fastqc_html':fastqc_html}})      # set dataflow params
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise