import os
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.tools.ucsc_cellbrowser_utils import convert_scanpy_h5ad_to_cellbrowser_dir

class CreateUCSCCellBrowser(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CreateUCSCCellBrowser,self).param_defaults()
    params_dict.update({
        'cellbrowser_dir_prefix':'cellbrowser',
        'use_ephemeral_space':0,
    })
    return params_dict

  def run(self):
    try:
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      experiment_igf_id = self.param_required('experiment_igf_id')
      base_work_dir = self.param_required('base_work_dir')
      cbImportScanpy_path = self.param_required('cbImportScanpy_path')
      scanpy_h5ad_path = self.param_required('scanpy_h5ad_path')
      cellbrowser_dir_prefix = self.param('cellbrowser_dir_prefix')
      use_ephemeral_space = self.param('use_ephemeral_space')
      work_dir_prefix = \
        os.path.join(\
          base_work_dir,
          project_igf_id,
          sample_igf_id,
          experiment_igf_id)
      work_dir = \
        self.get_job_work_dir(\
          work_dir=work_dir_prefix)
      datestamp = get_datestamp_label()
      cellbrowser_dir = \
          os.path.join( \
            work_dir,
            '{0}_{1}'.\
              format( \
                cellbrowser_dir_prefix,
                datestamp))
      convert_scanpy_h5ad_to_cellbrowser_dir(\
        cbImportScanpy_path=cbImportScanpy_path,
        h5ad_path=scanpy_h5ad_path,
        project_name=experiment_igf_id,
        use_ephemeral_space=use_ephemeral_space,
        cellbrowser_htmldir=cellbrowser_dir)
      self.param('dataflow_params',
                 {'cellbrowser_dir':cellbrowser_dir})
    except Exception as e:
      message = \
        'project: {2}, sample:{3}, Error in {0}: {1}'.\
          format(
            self.__class__.__name__,
            e,
            project_igf_id,
            sample_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise