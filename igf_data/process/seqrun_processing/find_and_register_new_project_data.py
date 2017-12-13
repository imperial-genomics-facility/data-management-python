import os

class Find_and_register_new_project_data:
  '''
  A class for finding new data for project and registering them to the db
  '''
  def __init__(self,seqrun_path,db_config):
    self.seqrun_path=seqrun_path
    self.db_config=db_config
    