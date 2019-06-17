import os
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from ehive.runnable.IGFBaseProcess import IGFBaseProcess

class CleanupDirOrFile(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CleanupDirOrFile,self).param_defaults()
    params_dict.update({
      'cleanup_status':False,
      })
    return params_dict

  def run(self):
    try:
      seqrun_igf_id = self.param_required('seqrun_igf_id')
      path = self.param_required('path')
      cleanup_status = self.param_required('cleanup_status')

      message = None
      if cleanup_status:
        if not os.path.exists(path):
          raise IOError('path {0} is not accessible'.format(path))

        if os.path.isdir(path):
          remove_dir(path)
          message = 'removed dir {0}'.format(path)
        elif os.path.isfile(path):
          os.remove(path)
          message = 'removed file {0}'.format(path)

        else:
          message = 'path {0} is not file or directory, skipped removing'.\
                    format(path)

      else:
        message = 'Not removing path {0} as cleanup_status is not True'.\
                  format(path)

      self.param('dataflow_params',
                 {'path':path,
                  'cleanup_status':cleanup_status})                             # set dataflow params
      if message:
        self.post_message_to_slack(message,reaction='pass')                     # send msg to slack
        self.comment_asana_task(task_name=seqrun_igf_id, comment=message)       # send msg to asana
    except Exception as e:
      message = \
        'seqrun: {2}, Error in {0}: {1}'.\
          format(\
            self.__class__.__name__,
            e,
            seqrun_igf_id)
      self.warning(message)
      self.post_message_to_slack(message,reaction='fail')                       # post msg to slack for failed jobs
      raise