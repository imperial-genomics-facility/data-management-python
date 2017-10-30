import os, subprocess,fnmatch
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunMutiQC(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'overwrite_output':True,
      'multiqc_dir_label':'multiqc',
      'multiqc_exe':'multiqc',
      'multiqc_options':{'--zip-data-dir':''},
      })
    return params_dict
  
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      demultiplexing_stats_file=self.param_required('demultiplexing_stats_file')
      fastqc_files=self.param_required('fastqc_files')
      fastqscreen_files=self.param_required('fastqscreen_files')
      multiqc_exe=self.param('multiqc_exe')
      multiqc_options=self.param('multiqc_options')
      multiqc_dir_label=self.param('multiqc_dir_label')
      overwrite_output=self.param('overwrite_output')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      seqrun_igf_id=self.param('seqrun_igf_id')
      lane_id=self.param('lane_id')
      status_tag=self.para,_required('status_tag')                              
      
      seqrun_date=seqrun_igf_id.split('_')[0]                                   # collect seqrun date from igf id
      seqrun_date=datetime.datetime.strptime(seqrun_date,'%y%m%d').date()       # identify actual date
      
      if status_tag not in ['known_smaples','undetermined_reads']:
        raise ValueError('unknown status tag {0}'.format(status_tag))           # check valid status tags
      
      multiqc_result_dir=os.path.join(base_results_dir, \
                                      project_name, \
                                      lane_id, \
                                      fastqscreen_dir_label, \
                                      seqrun_date )                             # get multiqc final output path
      
      if not os.path.exists(multiqc_result_dir):
        os.mkdir(multiqc_result_dir)                                            # create output dir if its not present
        
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      multiqc_input_list=os.path.join(temp_work_dir,'multiqc_input_file.txt')   # get name of multiqc input file
      
      with open(multiqc_input_list,'w') as multiqc_input_file:                  # writing multiqc input
        if not os.path.exists(demultiplexing_stats_file):
          raise IOError('demultiplexing stats file {0} not found'.\
                        format(demultiplexing_stats_file))                      # check demultiplexing stats file
        
        multiqc_input_file.write(demultiplexing_stats_file)                     # add demultiplexing stat to list
        
        for fastqc_file in fastqc_files:
          if not os.path.exists(fastqc_file['fastqc_zip']):
            raise IOError('fasqc file {0} not found'.\
                        format(fastqc_file['fastqc_zip']))                      # check fastqc file
            
          multiqc_input_file.write(fastqc_file['fastqc_zip'])                   # add fastqc file to list
      
        for fastqscreen_file in fastqscreen_files:
          if not os.path.exists(fastqscreen_file['fastqscreen_stat']):
            raise IOError('fastqscreen file {0} not found'.\
                        format(fastqscreen_file['fastqscreen_stat']))           # check fastqscreen file
            
          multiqc_input_file.write(fastqscreen_file['fastqscreen_stat'])        # add fastqscreen file to list
      
      multiqc_report_title='Project:{0},Sequencing_date:{1},Flowcell_lane:{2},status:{0}'.\
                           format(project_name, \
                                  seqrun_date,\
                                  lane_id,\
                                  status_tag)                                   # get multiqc report title and filename
      multiqc_param=[[param,value] if value else [param] \
                         for param, value in multiqc_options.items()]           # remove empty values
      multiqc_param=[col for row in param for col in multiqc_param]             # flatten sub lists
      multiqc_cmd=[multiqc_exe,
                       '--file-list',multiqc_input_file,
                       '--outdir',temp_work_dir,
                      ]                                                         # multiqc base parameters
      multiqc_cmd.extend(multiqc_param)                                         # add additional parameters
      subprocess.check_call(multiqc_cmd)                                        # run multiqc
      
      multiqc_html=None
      multiqc_data=None
      for root, dirs,files in os.walk(top=temp_work_dir):
        for file in files:
          if fnmatch.fnmatch(file, '*.html'):
            copy2(os.path.join(root,file),multiqc_result_dir)
            multiqc_html=os.path.join(multiqc_result_dir,file)                                              # get multiqc html path
          elif fnmatch.fnmatch(file, '*.zip'):
            copy2(os.path.join(root,file),multiqc_result_dir)
            multiqc_data=os.path.join(multiqc_result_dir,file)                                           # get multiqc data path
      
      self.param('dataflow_params',{'multiqc_html':multiqc_html, \
                                    'multiqc_data':multiqc_data})
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise