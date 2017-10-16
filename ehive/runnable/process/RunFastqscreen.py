import os, subprocess
from shutil import copytree
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunFastqscreen(IGFBaseProcess):
  def param_defaults(self):
    params_dict=IGFBaseProcess.param_defaults()
    params_dict.update({
      'overwrite_output':True,
      'fastqscreen_dir_label':'fastqscreen',
      'fastqscreen_exe':'fastqscreen',
      'fastqscreen_conf':None,
      'fastqscreen_options':{'--aligner':'bowtie2',
                             '--force':'',
                             '--quiet':'',
                             '--subset':100000,
                             '--threads':1},
      })
    return params_dict
  
  def run(self):
    try:
      fastq_file=self.param_required('fastq_file')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      sample_igf_id=self.param_required('sample_igf_id')
      run_igf_id=self.param_required('run_igf_id')
      fastqscreen_exe=self.param_required('fastqscreen_exe')
      fastqscreen_conf=self.param_required('fastqscreen_conf')
      fastqscreen_options=self.param('fastqscreen_options')
      overwrite_output=self.param('overwrite_output')
      fastqscreen_dir_label=self.param('fastqscreen_dir_label')
      
      fastqscreen_result_dir=os.path.join(base_results_dir, \
                                          project_name, \
                                          fastqscreen_dir_label, \
                                          sample_igf_id, \
                                          run_igf_id)                           # get fastqscreen final output path
      if not os.path.exists(fastqscreen_result_dir):
        os.mkdir(fastqscreen_result_dir)                                        # create output dir if its not present
      
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      if not os.path.exists(fastq_file):
        raise IOError('fastq file {0} not readable'.format(fastq_file))         # raise if fastq file path is not readable
      
      filename=os.path.basename(fastq_file)                                     # get fastqfile base name
      filename=filename.replace('.fastq.gz','')                                 # remove file ext
      fastqscreen_output=os.path.join(temp_work_dir,filename)
      os.mkdir(fastqscreen_output)                                              # create fastqc output dir
      
      fastqscreen_param=[[param,value] if value else [param] \
                         for param, value in fastqscreen_options.items()]       # remove empty values
      fastqscreen_param=[col for row in param for col in fastqscreen_param]     # flatten sub lists
      fastqscreen_cmd=[fastqscreen_exe,
                       '-conf',fastqscreen_conf,
                       '--outdir',fastqscreen_output,
                      ]                                                         # fastqscreen base parameters
      fastqscreen_cmd.extend(fastqscreen_param)                                 # add additional parameters
      fastqscreen_cmd.extend(fastq_file)                                        # fastqscreen input file
      subprocess.check_call(fastqscreen_cmd)                                    # run fastqscreen
      
      if overwrite_output:
        rmdir(os.path.join(fastqscreen_result_dir,filename))                    # remove fastqscreen results if its already present
        
      copytree(fastqscreen_output,fastqscreen_result_dir)                       # copy fastqscreen output files
      fastqscreen_path=os.path.join(fastqscreen_result_dir,filename)
      self.param('dataflow_params',{'fastqscreen': fastqscreen_path})           # set dataflow params
    except:
      raise