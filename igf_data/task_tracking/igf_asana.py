import os, asana, json
from datetime import datetime
from asana.error import ForbiddenError

class IGF_asana:
  '''
  A python class for accessing Asana
  
  :params asana_config: A json config file with personal token
                        e.g. { "asana_personal_token" : "zyx" }
  :param asana_project_id: A project id
  '''

  def __init__(self, asana_config, asana_project_id, **asana_label):
    asana_label.setdefault('asana_personal_token_label','asana_personal_token')
    self.asana_personal_token_label=asana_label['asana_personal_token_label']

    self.asana_personal_token = None
    self.asana_config = asana_config
    self.asana_project_id = asana_project_id                                    # project name can change, project id is stable
    self._read_and_set_asana_config()                                           # read config file and set parameters
    self.asanaclient = asana.Client.access_token(self.asana_personal_token)     # create asana client instance
    self.asanaclient.headers = {'asana-enable': 'string_ids'}                   # fix for string ids
    self.asana_personal_token = None                                            # reset asana token value
    self._get_user_details()                                                    # load user information
    self._check_project_id()                                                    # check user given project id


  def get_asana_task_id(self,task_name,strict_check=False):
    '''
    A method for fetching task id from asana server
    
    :param task_name: A task name
    :param strict_check: Perform strict checking for task id count, default False
    :returns: A asana task gid
    '''
    try:
      asana_task_id = None
      matched_tasks = list()
      try:
        all_asana_tasks = \
          self.asanaclient.\
            tasks.\
            find_all({'project':self.asana_project_id})
        matched_tasks = [
          p for p in all_asana_tasks
            if p['name']==task_name ]
        asana_task_id = matched_tasks[0]['gid']
      except:
        pass

      if strict_check and \
         len(matched_tasks) > 1:
        raise ValueError('received more than one entry for task: {0}'.\
                         format(task_name))

      return asana_task_id
    except:
      raise


  def rename_asana_task(self,task_name,new_name):
    '''
    A method for renaming asana task
    
    :params task_name: A task name
    :params new_name: A new task name
    '''
    try:
      asana_task_id = self.get_asana_task_id(task_name=task_name)
      self.asanaclient.\
        tasks.\
        update(str(asana_task_id),{'name':new_name})
    except:
      raise


  def create_asana_task(self,task_name, notes=''):
    '''
    A method for creating new task in Asana. Tasks will get assigned to the creator.
    
    :params task_name: A task name
    :param notes: A task description
    '''
    try:
      task_id = None
      results = \
        self.asanaclient.tasks.\
          create_in_workspace(
            self.asana_workspace_id,
            {'name':task_name,
             'projects':str(self.asana_project_id),
             'notes':notes,
             'assignee':str(self.asana_user_id),
            })
      task_id = results['gid']                                                  # fetching gid
      return task_id
    except:
      raise


  def fetch_task_id_for_task_name(self, task_name):
    '''
    A method for fetching task id from asana. It will create a new task
    if its not present yet
    
    :params task_name: A task name
    :returns: asana task id
    '''
    try:
      task_id=self.get_asana_task_id(task_name)
      if task_id is None:
        task_id=self.create_asana_task(task_name)

      return task_id
    except:
      raise


  def comment_asana_task(self, task_name, comment,rename_task=True):
    '''
    A method for adding comments to asana tasks. Task will be created if it doesn't exist.
    
    :params task_name: A task name
    :param comment: A comment for the target task
    :param rename_task: Rename task if cvoan't comment, default True
    :returns: A output story as dictionary
    '''
    try:
      asana_task_id = self.fetch_task_id_for_task_name(task_name)
      res = \
        self.asanaclient.\
          stories.\
          create_on_task(
            asana_task_id,
            {'text':comment})
      return res
    except ForbiddenError:
      if rename_task:
        time_stamp = datetime.strftime(datetime.now(),'%Y-%M-%d-%H-%M-%S')
        new_task_name = '{0}_{1}'.format(task_name,time_stamp)
        self.rename_asana_task(
          task_name=task_name,
          new_name=new_task_name)                                               # rename task if can't comment on it, likely due to 1k limit
      raise
    except:
      raise


  def add_notes_for_task(self,task_name, notes):
    '''
    A method for adding comment to the existing or new task
    
    :params task_name: A task name
    :param notes: A text note
    '''
    try:
      asana_task_id = self.fetch_task_id_for_task_name(task_name)
      res = \
        self.asanaclient.\
          tasks.\
          update(
            task=str(asana_task_id),
            params={'notes':notes})
      return res
    except:
      raise


  def attach_file_to_asana_task(self,task_name, filepath, remote_filename=None,
                                comment=None):
    '''
    A method for uploading files to asana
    
    :params task_name: A task name
    :param filepath: A filepath to upload
    :param remote_filename: Name of the uploaded file, default None for original name
    :param comment: A text comment, default None
    '''
    try:
      if not os.path.exists(filepath):
        raise IOError('file {0} not found'.format(filepath))
      if comment:
        _ = self.comment_asana_task(task_name,comment)                        # adding comment to asana)
      asana_task_id = self.fetch_task_id_for_task_name(task_name)               # get task_id from asana

      if os.stat(filepath).st_size > 5000000:
        comment = \
          'skipped uploading file {0}, size {1}'.\
            format(
              os.path.basename(filepath),
              os.stat(filepath).st_size)                                        # skip uploading files more than 5Mb in size
        _ = \
          self.asanaclient.\
            stories.\
            create_on_task(
              asana_task_id,
              {'text':comment})
      else:
        if remote_filename is not None:
          file_name = remote_filename
        else:
          file_name = os.path.basename(filepath)

        _ = \
          self.asanaclient.attachments.\
            create_on_task(
              task_id=str(asana_task_id),
              file_content=open(os.path.join(filepath),'rb'),
              file_name=file_name)                                              # upload file to task_id
    except:
      raise 


  def _check_project_id(self):
    '''
    An internal method for checking user given project id
    '''
    try:
      all_asana_projects = \
        self.asanaclient.\
          projects.\
          find_all({'workspace':str(self.asana_workspace_id)})
      matched_projects = [
        p for p in all_asana_projects
           if p['gid']==str(self.asana_project_id) ]                            # checking gid
      if len(matched_projects)==0:
        raise ValueError(
          'project id {0} not found in workspace {1}'.\
            format(
              self.asana_project_id,
              self.asana_workspace_name))
    except:
      raise


  def _get_user_details(self):
    '''
    An internal method for loading user and workspace information
    '''
    try:
      user_detail = self.asanaclient.users.me()
      self.asana_workspace_id = user_detail['workspaces'][0]['gid']             # fetching gid
      self.asana_workspace_name = user_detail['workspaces'][0]['name']
      self.asana_user_id = user_detail['gid']                                   # fetching gid
      self.asana_user_name = user_detail['name']
    except:
      raise


  def  _read_and_set_asana_config(self):
    '''
    An internal method for reading asana config json file
    '''
    try:
      asana_params = dict()
      with open(self.asana_config,'r') as json_data:
        asana_params = json.load(json_data)

      if self.asana_personal_token_label in asana_params:
        self.asana_personal_token = asana_params[self.asana_personal_token_label]

    except:
      raise
