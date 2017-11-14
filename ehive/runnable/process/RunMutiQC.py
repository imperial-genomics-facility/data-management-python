import os, subprocess,fnmatch
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class RunMutiQC(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(IGFBaseProcess,self).param_defaults()
    params_dict.update({
      'force_overwrite':True,
      'multiqc_dir_label':'multiqc',
      'multiqc_exe':'multiqc',
      'multiqc_options':{'--zip-data-dir':''},
      'demultiplexing_stats_file':'Stats/Stats.json'
      })
    return params_dict
  
  
  def run(self):
    try:
      seqrun_igf_id=self.param_required('seqrun_igf_id')
      demultiplexing_stats_file=self.param_required('demultiplexing_stats_file')
      qc_files_name=self.param('qc_files_name')
      multiqc_exe=self.param('multiqc_exe')
      multiqc_options=self.param('multiqc_options')
      multiqc_dir_label=self.param('multiqc_dir_label')
      force_overwrite=self.param('force_overwrite')
      base_results_dir=self.param_required('base_results_dir')
      project_name=self.param_required('project_name')
      seqrun_date=self.param_required('seqrun_date')
      flowcell_id=self.param_required('flowcell_id')
      tag=self.param_required('tag')
      
      if tag not in ['known','undetermined']:
        raise ValueError('unknown status tag {0}'.format(tag))                  # check valid status tags
      
      if qc_files_name not in ['qc_known','qc_undetermined']:
        raise ValueError('unknown status tag {0}'.format(qc_files_name))        # check valid qc files
      
      qc_files=self.param_required(qc_files_name)                               # get specific qc files
      fastq_dir=[f_dir for f_dir in qc_files.keys()][0]                         # consider only the first fastq dir
      lane_index_info=os.path.basename(fastq_dir)                               # get lane and index info
      
      fastqc_files=list()
      fastqscreen_files=list()
      for fastq_dir, qc_output in qc_files.items():
        fastqc_files.extend([fqc_dir for fqc_dir in qc_output['fastqc']])
        fastqscreen_files.extend([fsr_dir for fsr_dir in qc_output['fastqscreen']])
        
      multiqc_result_dir=os.path.join(base_results_dir, \
                                      project_name, \
                                      seqrun_date, \
                                      flowcell_id, \
                                      lane_index_info,\
                                      tag, \
                                      multiqc_dir_label)                        # get multiqc final output path
      
      if os.path.exists(multiqc_result_dir) and force_overwrite:
        remove_dir(multiqc_result_dir)                                          # remove existing output dir if force_overwrite is true
        
      if not os.path.exists(multiqc_result_dir):
        os.makedirs(multiqc_result_dir,mode=0o775)                              # create output dir if its not present
        
      temp_work_dir=get_temp_dir()                                              # get a temp work dir
      multiqc_input_list=os.path.join(temp_work_dir,'multiqc_input_file.txt')   # get name of multiqc input file
      
      demultiplexing_stats_file=os.path.join(fastq_dir,
                                             demultiplexing_stats_file)
      with open(multiqc_input_list,'w') as multiqc_input_file:                  # writing multiqc input
        if not os.path.exists(demultiplexing_stats_file):
          raise IOError('demultiplexing stats file {0} not found'.\
                        format(demultiplexing_stats_file))                      # check demultiplexing stats file
        
        multiqc_input_file.write('{}\n'.format(demultiplexing_stats_file))      # add demultiplexing stat to list
        
        for fastqc_file in fastqc_files:
          fastqc_zip=fastqc_file['fastqc_zip']
          if not os.path.exists(fastqc_zip):
            raise IOError('fasqc file {0} not found'.\
                        format(fastqc_zip))                                     # check fastqc file
            
          multiqc_input_file.write('{}\n'.format(fastqc_zip))                   # add fastqc file to list
      
        for fastqscreen_file in fastqscreen_files:
          fastqscreen_stat=fastqscreen_file['fastqscreen_stat']
          if not os.path.exists(fastqscreen_stat):
            raise IOError('fastqscreen file {0} not found'.\
                        format(fastqscreen_stat))                               # check fastqscreen file
            
          multiqc_input_file.write('{}\n'.format(fastqscreen_stat))             # add fastqscreen file to list
      
      multiqc_report_title='Project:{0},Sequencing_date:{1},Flowcell_lane:{2},status:{3}'.\
                           format(project_name, \
                                  seqrun_date,\
                                  lane_index_info,\
                                  tag)                                          # get multiqc report title and filename
      multiqc_param=self.format_tool_options(multiqc_options)                   # format multiqc params
      multiqc_cmd=[multiqc_exe,
                       '--file-list',multiqc_input_list,
                       '--outdir',temp_work_dir,
                       '--title',multiqc_report_title,
                      ]                                                         # multiqc base parameters
      multiqc_cmd.extend(multiqc_param)                                         # add additional parameters
      subprocess.check_call(multiqc_cmd)                                        # run multiqc
      
      multiqc_html=None
      multiqc_data=None
      for root, dirs,files in os.walk(top=temp_work_dir):
        for file in files:
          if fnmatch.fnmatch(file, '*.html'):
            copy2(os.path.join(root,file),multiqc_result_dir)
            multiqc_html=os.path.join(multiqc_result_dir,file)                  # get multiqc html path
          elif fnmatch.fnmatch(file, '*.zip'):
            copy2(os.path.join(root,file),multiqc_result_dir)
            multiqc_data=os.path.join(multiqc_result_dir,file)                  # get multiqc data path
      
      self.param('dataflow_params',{'multiqc_html':multiqc_html, \
                                    'multiqc_data':multiqc_data, \
                                    'lane_index_info':lane_index_info})
    except Exception as e:
      message='seqrun: {2}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise