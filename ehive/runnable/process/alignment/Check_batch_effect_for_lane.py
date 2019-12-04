import os,json
from igf_data.utils.fileutils import get_temp_dir,remove_dir,check_file_path
from igf_data.process.batch_effect.batch_effect_report import Batch_effect_report
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils


class Check_batch_effect_for_lane(IGFBaseProcess):
  '''
  A ehive runnable for calculating lane level batch effect for each sample, from star gene count file

  FIXME: Replace with a notebook
  '''
  def param_defaults(self):
    params_dict=super(Check_batch_effect_for_lane,self).param_defaults()
    params_dict.update({
        'strand_info':'reverse_strand',
        'read_threshold':5,
        'collection_type':'RNA_BATCH_EFFECT_HTML',
        'collection_table':'experiment',
        'analysis_name':'batch_effect',
        'tag_name':'star_gene_count',
        'use_ephemeral_space':0,
      })
    return params_dict

  def run(self):
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id=self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      input_files = self.param_required('input_files')
      igf_session_class = self.param_required('igf_session_class')
      template_report_file = self.param_required('template_report_file')
      rscript_path = self.param_required('rscript_path')
      batch_effect_rscript_path = self.param_required('batch_effect_rscript_path')
      base_result_dir = self.param_required('base_result_dir')
      strand_info = self.param('strand_info')
      read_threshold = self.param('read_threshold')
      collection_type = self.param('collection_type')
      collection_table = self.param('collection_table')
      analysis_name = self.param('analysis_name')
      tag_name = self.param('tag_name')
      use_ephemeral_space = self.param('use_ephemeral_space')

      output_file_list = None
      if len(input_files)==0:
        raise ValueError('No input files found for bactch effect checking')
      elif len(input_files) < 3:
        output_file_list = ''                                                   # can't run batch effect checking on less than 3 lanes
      else:
        for file in input_files:
          check_file_path(file)                                                 # check input filepath

        file_data = list()
        ra = RunAdaptor(**{'session_class':igf_session_class})
        ra.start_session()
        for file in input_files:
          run_igf_id = os.path.basename(file).\
                       replace('ReadsPerGene.out.tab','')                       # using simple string match to fetch run igf ids
          flowcell_id, lane_id = \
            ra.fetch_flowcell_and_lane_for_run(run_igf_id=run_igf_id)           # fetch flowcell id and lane info
          file_data.append({'file':file,
                            'flowcell':flowcell_id,
                            'lane':lane_id
                          })
        ra.close_session()
        temp_dir = \
          get_temp_dir(use_ephemeral_space=use_ephemeral_space)
        temp_json_file = \
          os.path.join(temp_dir,'star_gene_counts.json')                        # temp json file path
        temp_output_file = \
          os.path.join(\
            temp_dir,
            os.path.basename(template_report_file))                             # temp report file path
        with open(temp_json_file,'w') as jp:
          json.dump(file_data,jp,indent=2)                                      # dumping json output

        br = Batch_effect_report(\
               input_json_file=temp_json_file,
               template_file=template_report_file,
               rscript_path=rscript_path,
               batch_effect_rscript_path=batch_effect_rscript_path,
               strand_info=strand_info,
               read_threshold=read_threshold
             )                                                                  # set up batch effect run
        br.check_lane_effect_and_log_report(\
             project_name=project_igf_id,
             sample_name=sample_igf_id,
              output_file=temp_output_file
            )                                                                   # generate report file
        au = Analysis_collection_utils(\
               dbsession_class=igf_session_class,
               analysis_name=analysis_name,
               base_path=base_result_dir,
               tag_name=tag_name,
               collection_name=experiment_igf_id,
               collection_type=collection_type,
               collection_table=collection_table
             )                                                                  # prepare to load file
        output_file_list = \
          au.load_file_to_disk_and_db(\
               input_file_list=[temp_output_file])                              # load file to db and disk

      self.param('dataflow_params',
                 {'batch_effect_reports':output_file_list})                     # populating data flow only if report is present
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
        format(\
          self.__class__.__name__,
          e,
          project_igf_id,
          sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise