from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, ProjectUser, Project_attribute

class ProjectAdaptor(BaseAdaptor):
  '''
  An adaptor class fro Project, ProjectUser and Project_attribute tables
  '''

  def load_project_data(self, data_frame):
    '''
    Load data to Project table
    '''
    pass

  def assign_user_to_project(self, data_frame):
    '''
    Load data to ProjectUser table
    '''
    pass

  def _divide_project_user_dataframe(self, data_frame):
    '''
    An internal method for dividing dataframe for Project and ProjectUser tables
    '''
    pass

  def load_project_and_user_data(self, data_frame):
    '''
    Load data to Project and ProjectUser tables
    '''
    (project_data, project_user_data)=self._divide_project_user_dataframe(data_frame)
    load_project_data()
    assign_user_to_project()
    pass

  def load_project_attributes(self, data_frame):
    '''
    A method for storing data to Project_attribute table
    '''
    super(ProjectAdaptor, self).load_attributes( attribute_table='Project_attribute', data_frame=data_frame)
    pass

  def get_project_info(self, format='dataframe', project_igf_id=''):
    '''
    A method for fetching data for Project table
    '''
    projects=super(ProjectAdaptor, self).get_table_info_by_igf_id(table='Project',igf_id=project_igf_id )
    return projects  
    

  def project_user_info(self,format='dataframe', project_igf_id=''):
    '''
    A method for fetching information from Project, User and ProjectUser table 
    '''
    pass

  def get_project_attributes(self, project_igf_id, attribute_name=''): 
    projects=self.get_project_info(format='object', project_igf_id=project_igf_id)
    project=projects[0]

    project_attributes=super(ProjectAdaptor, self).get_attributes(attribute_table='Project_attribute', db_id=project.project_id )
    return project_attributes

  def project_samples(self, format='dataframe', project_igf_id=''):
    pass

  def project_experiments(self, format='dataframe', project_igf_id=''):
    pass

  
