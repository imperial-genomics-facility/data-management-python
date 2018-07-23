import os,subprocess,fnmatch
from shlex import quote
from shutil import copy2
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils
from igf_data.utils.fileutils import get_temp_dir,remove_dir,get_datestamp_label

class RunAnalysisMultiQC(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(RunAnalysisMultiQC,self).param_defaults()
    params_dict.update({
      'analysis_name':'cellranger_multiQC',
      'sample_igf_id':None,
      'analysis_files':[],
      'force_overwrite':True,
      'multiqc_dir_label':'multiqc',
      'multiqc_exe':'multiqc',
      'multiqc_options':{'--zip-data-dir':''},
      })
    return params_dict

  def run(self):
    try:
      project_igf_id=self.param_required('project_igf_id')
      sample_igf_id=self.param_required('sample_igf_id')
      analysis_files=self.param_required('analysis_files')
      multiqc_exe=self.param('multiqc_exe')
      multiqc_options=self.param('multiqc_options')
      multiqc_dir_label=self.param('multiqc_dir_label')
      force_overwrite=self.param('force_overwrite')
      base_results_dir=self.param_required('base_results_dir')
      tag=self.param_required('tag_name')
      analysis_name=self.param_required('analysis_name')
      collection_name=self.param_required('collection_name')
      collection_type=self.param_required('collection_type')
      collection_table=self.param_required('collection_table')
      if not isinstance(analysis_files,list) and \
         len(analysis_files) ==0:
        raise ValueError('Failed to run MultiQC for zero analysis list')        # check analysis files

      temp_work_dir=get_temp_dir()                                              # get temp work dir
      multiqc_input_file=os.path.join(temp_work_dir,
                                      'multiqc.txt')                            # get temp multiqc list
      with open(multiqc_input_file,'w') as fp:
        for file in analysis_files:
          if not os.path.exists(file):
            raise IOError('File {0} not found for multiQC run'.\
                          format(file))                                         # check filepath

          fp.write('{}\n'.format(file))                                         # write file to temp file

      multiqc_report_title='Project:{0}'.format(project_igf_id)                 # base multiqc label
      if sample_igf_id is not None:
        multiqc_report_title='{0},Sample:{1}'.format(multiqc_report_title,
                                                     sample_igf_id)             # add sample, if its present

      multiqc_report_title='{0};tag:{1};date:{2}'.format(multiqc_report_title,
                                                         tag,
                                                         get_datestamp_label)   # add tag and date stamp
      multiqc_param=self.format_tool_options(multiqc_options)                   # format multiqc params
      multiqc_cmd=[multiqc_exe,
                       '--file-list',quote(multiqc_input_file),
                       '--outdir',quote(temp_work_dir),
                       '--title',quote(multiqc_report_title),
                      ]                                                         # multiqc base parameters
      multiqc_param=[quote(param) for param in multiqc_param]                   # wrap params in quotes
      multiqc_cmd.extend(multiqc_param)                                         # add additional parameters
      subprocess.check_call(multiqc_cmd)                                        # run multiqc
      multiqc_html=None
      output_list=list()
      for root, dirs,files in os.walk(top=temp_work_dir):
        for file in files:
          if fnmatch.fnmatch(file, '*.html'):
            copy2(os.path.join(root,file),multiqc_result_dir)
            multiqc_html=os.path.join(multiqc_result_dir,file)                  # get multiqc html path
            au=Analysis_collection_utils(\
                 dbsession_class=igf_session_class,
                 analysis_name=analysis_name,
                 tag_name=tag,
                 collection_name=collection_name,
                 collection_type=collection_type,
                 collection_table=collection_table,
                 base_path=base_result_dir)
            output_list=au.load_file_to_disk_and_db(\
                          input_file_list=[multiqc_html],
                          withdraw_exisitng_collection=force_overwrite)         # load file to db and disk

      self.param('dataflow_params',{'multiqc_html':output_list[0]})             # add output files to dataflow
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.format(self.__class__.__name__, \
                                                      e, \
                                                      project_igf_id,
                                                      sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise