import os,subprocess
from shlex import quote
from shutil import copy2, copytree
from ehive.runnable.IGFBaseProcess import IGFBaseProcess
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class CopyAnalysisFilesToRemote(IGFBaseProcess):
  def param_defaults(self):
    params_dict=super(CopyAnalysisFilesToRemote,self).param_defaults()
    params_dict.update({
      'remote_host':None,
      'remote_user':'igf',
      'remote_project_path':None,
      'remote_seqrun_path':None,
      'force_overwrite':True,
      'dir_labels':[],
      'sample_igf_id':None,
      'collect_remote_file':False,
      'collection_name':None,
      'collection_type':None,
      'collection_table':'file',
      'file_location':'ELIOT',
      'use_ephemeral_space':0,
      })
    return params_dict

  def run(self):
    try:
      project_igf_id = self.param_required('project_igf_id')
      sample_igf_id = self.param_required('sample_igf_id')
      file_list = self.param_required('file_list')
      remote_user = self.param_required('remote_user')
      remote_host = self.param_required('remote_host')
      remote_project_path = self.param_required('remote_project_path')
      dir_labels = self.param_required('dir_labels')
      igf_session_class = self.param_required('igf_session_class')
      force_overwrite = self.param('force_overwrite')
      collect_remote_file = self.param('collect_remote_file')
      collection_name = self.param('collection_name')
      collection_type = self.param('collection_type')
      collection_table = self.param('collection_table')
      file_location = self.param('file_location')
      use_ephemeral_space = self.param('use_ephemeral_space')
      destination_output_path = \
        os.path.join(
          remote_project_path,
          project_igf_id)                                                       # get base destination path
      if isinstance(dir_labels, list) and \
         len(dir_labels) > 0:
        destination_output_path=\
          os.path.join(destination_output_path,
                       *dir_labels)

      if collect_remote_file:
        if collection_name is None or \
           collection_type is None:
           raise ValueError('Name and type are required for db collection')

      output_file_list = list()
      temp_work_dir = \
        get_temp_dir(use_ephemeral_space=use_ephemeral_space)                   # get temp dir
      for file in file_list:
        if not os.path.exists(file):
          raise IOError('file {0} not found'.\
                        format(file))

        if os.path.isfile(file):
          copy2(
            file,
            os.path.join(
              temp_work_dir,
              os.path.basename(file)))                                          # copy file to a temp dir
          dest_file_path = \
            os.path.join(
              destination_output_path,
              os.path.basename(file))                                           # get destination file path
        elif os.path.isdir(file):
          copytree(\
            file,
            os.path.join(
              temp_work_dir,
              os.path.basename(file)))                                          # copy dir to a temp dir
          dest_file_path=destination_output_path
        else:
          raise ValueError('Unknown source file type: {0}'.\
                           format(file))

        os.chmod(
          os.path.join(
            temp_work_dir,
            os.path.basename(file)),
          mode=0o754
        )                                                                       # set file permission
        copy_remote_file(\
          source_path=os.path.join(temp_work_dir,
                                   os.path.basename(file)),
          destinationa_path=dest_file_path,
          destination_address='{0}@{1}'.format(remote_user,remote_host),
          force_update=force_overwrite
        )                                                                       # copy file to remote
        if os.path.isdir(file):
          dest_file_path=\
            os.path.join(\
              dest_file_path,
              os.path.basename(file))                                           # fix for dir input

        output_file_list.append(dest_file_path)

      remove_dir(dir_path=temp_work_dir)                                        # remove temp dir
      self.param('dataflow_params',
                 {'status': 'done',
                  'output_list':output_file_list})                              # add dataflow params
      if collect_remote_file:
        data=list()
        remove_data_list=[{'name':collection_name,
                           'type':collection_type}]
        for file in output_file_list:
          data.append(
            {'name':collection_name,
             'type':collection_type,
             'table':collection_table,
             'file_path':file,
             'location':file_location
            }
          )

        ca = CollectionAdaptor(**{'session_class':igf_session_class})
        ca.start_session()
        try:
          ca.remove_collection_group_info(
            data=remove_data_list,
            autosave=False)                                                     # remove existing data before loading new collection
          ca.load_file_and_create_collection(
            data=data,
            autosave=False,
            calculate_file_size_and_md5=False)                                  # load remote files to db
          ca.commit_session()                                                   # commit changes
          ca.close_session()
        except:
          ca.rollback_session()                                                 # rollback changes
          ca.close_session()
          raise

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