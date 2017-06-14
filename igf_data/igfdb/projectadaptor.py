from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, ProjectUser, Project_attribute

class ProjectAdaptor(BaseAdaptor):
  '''
  An adaptor class for Project, ProjectUser and Project_attribute tables
  '''

  def store_project_data(self, data):
    '''
    Load data to Project table
    '''
    try:
      self.store_records(table=Project, data=data)
    except:
      raise


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


  def store_project_attributes(self, data, project_id):
    '''
    A method for storing data to Project_attribute table
    '''
    try:
      super(ProjectAdaptor, self).store_attributes( attribute_table=Project_attribute, linked_column='project_id', db_id=project_id, data=data)
    except:
      raise


  def fetch_project_records_igf_id(self, project_igf_id, output_mode='dataframe'):
    '''
    A method for fetching data for Project table
    required params:
    project_igf_id: an igf id
     output_mode  : dataframe / object
    '''
    try:
      projects=super(ProjectAdaptor, self).fetch_records_by_column(table=Project, column_name=Project.igf_id, column_id=project_igf_id, output_mode=output_mode )
      return projects  
    except:
      raise
    

  def project_user_info(self, output_mode='dataframe', project_igf_id=''):
    '''
    A method for fetching information from Project, User and ProjectUser table 
    optional params:
    project_igf_id: a project igf id
    output_mode   : dataframe / object
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
  
    session=self.session
    query=session.query(Project).join(ProjectUser).join(User)
    if project_igf_id:
      query=query.filter(Project.igf_id==project_igf_id)
    
    try:    
      results=super(ProjectAdaptor, self).fetch_records(query=query, output_mode=output_mode)
      return results
    except:
      raise
     

  def get_project_attributes(self, project_igf_id, attribute_name=''): 
    projects=self.get_project_info(format='object', project_igf_id=project_igf_id)
    project=projects[0]

    project_attributes=super(ProjectAdaptor, self).get_attributes(attribute_table='Project_attribute', db_id=project.project_id )
    return project_attributes


  def project_samples(self, format='dataframe', project_igf_id=''):
    pass


  def project_experiments(self, format='dataframe', project_igf_id=''):
    pass

  
