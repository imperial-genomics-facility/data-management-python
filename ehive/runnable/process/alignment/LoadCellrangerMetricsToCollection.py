import os
from igf_data.utils.fileutils import check_file_path
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.tools.cellranger.cellranger_count_utils import extract_cellranger_count_metrics_summary

class LoadCellrangerMetricsToCollection(IGFBaseProcess):
  '''
  A ehive process class for loading cellranger count metrics to collection attribute table
  '''
  def param_defaults(self):
    params_dict=super(LoadCellrangerMetricsToCollection,self).param_defaults()
    params_dict.update({
        'collection_type':'CELLRANGER_RESULTS',
        'metrics_filename':'metrics_summary.csv',
        'attribute_prefix':'CELLRANGER',
      })
    return params_dict

  def run(self):
    '''
    A method for running the cellranger count metrics extraction
    
    :param project_igf_id: A project igf id
    :param experiment_igf_id: An experiment igf id
    :param sample_igf_id: A sample igf id
    :param igf_session_class: A database session class
    :param analysis_output_list: Cellranger analysis tar output path
    :param collection_type: Cellranger results collection type
    :param metrics_filename: Name of the metrics file, default metrics_summary.csv
    :returns: None
    '''
    try:
      project_igf_id = self.param_required('project_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      igf_session_class = self.param_required('igf_session_class')
      analysis_output_list = self.param_required('analysis_output_list')
      collection_type = self.param('collection_type')
      metrics_filename = self.param('metrics_filename')
      attribute_prefix = self.param('attribute_prefix')
      for infile in analysis_output_list:
        check_file_path(infile)                                                 # check input file path

      cellranger_tar = analysis_output_list[0]
      cellranger_metrics = extract_cellranger_count_metrics_summary(\
                            cellranger_tar=cellranger_tar,
                            target_filename=metrics_filename,
                            collection_name=experiment_igf_id,
                            collection_type=collection_type,
                            attribute_prefix=attribute_prefix
                            )                                                   # extract cellranger metrics stats as dictionary
      ca = CollectionAdaptor(**{'session_class':igf_session_class})
      ca.start_session()
      try:
        ca.create_or_update_collection_attributes(\
           data=cellranger_metrics,
           autosave=False)                                                      # load cellranger metrics to collection attribute table
        ca.commit_session()
        ca.close_session()
      except:
          ca.rollback_session()
          ca.close_session()
          raise

      self.param('dataflow_params',{'cellranger_attribute':'done'})
    except Exception as e:
      message='project: {2}, sample:{3}, Error in {0}: {1}'.\
              format(self.__class__.__name__,
                     e,
                     project_igf_id,
                     sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise