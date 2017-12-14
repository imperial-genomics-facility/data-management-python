import os,subprocess,fnmatch
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor

class Find_and_register_new_project_data:
  '''
  A class for finding new data for project and registering them to the db. 
  Account for new users will be created in irods server and password will be 
  mailed to them.
  required params:
  projet_info_path: A directory path for project info files
  dbconfig: A json dbconfig file
  user_account_template: A template file for user account activation email
  '''
  def __init__(self,projet_info_path,dbconfig,user_account_template):
    self.projet_info_path=projet_info_path
    self.user_account_template=user_account_template
    dbparams = read_dbconf_json(dbconfig)
    self.session_class = base.get_session_class()
    
  
  def find_new_project_info(self):
    '''
    A method for fetching new project info file
    '''
    try:
      for root_path,dirs,files in os.walk(self.projet_info_path, topdown=True):
        for file in files:
          if fnmatch.fnmatch(file_name, '*.csv'):                               # only consider csv files
            pass
    except:
      raise
