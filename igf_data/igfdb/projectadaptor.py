import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.igfTables import Project, ProjectUser, Project_attribute, User

class ProjectAdaptor(BaseAdaptor):
  '''
  An adaptor class for Project, ProjectUser and Project_attribute tables
  '''

  def store_project_and_attribute_data(self, data):
    '''
    A method for dividing and storing data to project and attribute_table
    '''
    (project_data, project_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    
    try:
      self.store_project_data(data=project_data)                              # store project
      if len(project_attr_data.columns) > 0:                                  # check if any attribute is present
        self.store_project_attributes(data=project_attr_data)                 # store project attributes
      self.commit_session()                                                   # save changes to database
    except:
      self.rollback_session()
      raise
     


  def divide_data_to_table_and_attribute(self, data, required_column='project_igf_id', attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for Project and Project_attribute tables
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    project_columns=self.get_table_columns(table_name=Project, excluded_columns=['project_id'])                      # get required columns for project table
    (project_df, project_attr_df)=super(ProjectAdaptor, self).divide_data_to_table_and_attribute( \
                                                                     data=data, \
    	                                                             required_column=required_column, \
    	                                                             table_columns=project_columns,  \
                                                                     attribute_name_column=attribute_name_column, \
                                                                     attribute_value_column=attribute_value_column
    	                                                        )
    return (project_df, project_attr_df)
  

  def store_project_data(self, data):
    '''
    Load data to Project table
    '''
    try:
      self.store_records(table=Project, data=data)
    except:
      raise


  def store_project_attributes(self, data, project_id=''):
    '''
    A method for storing data to Project_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                     # convert data to dataframe

      if 'project_igf_id' in data.columns:                                          # map foreign key if project_igf_id is found
        map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                        data=x, \
                                        lookup_table=Project, \
                                        lookup_column_name='project_igf_id', \
                                        target_column_name='project_id')            # prepare the function
        new_data=data.apply(map_function, axis=1)                                   # map foreign key id
        data=new_data                                                               # overwrite data   
       
      self.store_attributes(attribute_table=Project_attribute, linked_column='project_id', db_id=project_id, data=data)   # store attributes without auto commit
    except:
      raise


  def assign_user_to_project(self, data, required_project_column='project_igf_id',required_user_column='user_igf_id', data_authority_column='data_authority'):
    '''
    Load data to ProjectUser table
    required parameters:
    data: a list of dictionaries, each containing 
          'project_igf_id' and 'user_igf_id' as key
          with relevent igf ids as the values.
          an optional key 'data_authority' with 
          boolean value can be provided to set the user 
          as the data authority of the project
          E.g.
          [{'project_igf_id': val, 'user_igf_id': val, 'data_authority':True},] 
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)
 
    if not set((required_project_column,required_user_column,data_authority_column)).issubset(set(tuple(data.columns))):          # check for required parameters
      raise ValueError('Missing required value in input data {0}'.format(data_columns))

    try:
      project_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                              data=x, \
                                              lookup_table=Project, \
                                              lookup_column_name=required_project_column, \
                                              target_column_name='project_id')                  # prepare the function for Project id
      new_data=data.apply(project_map_function, 1)                                              # map project id
      user_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                              data=x, \
                                              lookup_table=User, \
                                              lookup_column_name=required_user_column, \
                                              target_column_name='user_id')                     # prepare the function for User id
      new_data=new_data.apply(user_map_function, 1)                                             # map user id
      data_authotiry_dict={True:'T'}                                                            # create a mapping dictionary for data authority value
      new_data[data_authority_column]=new_data[data_authority_column].map(data_authotiry_dict)  # add value for data authority
      self.store_records(table=ProjectUser, data=new_data)                                      # store the project_user data
      self.commit_session()                                                                     # save changes to database
    except:
      self.rollback_session()
      raise


  def fetch_project_records_igf_id(self, project_igf_id, target_column_name='project_igf_id'):
    '''
    A method for fetching data for Project table
    required params:
    project_igf_id: an igf id
    output_mode  : dataframe / object
    '''
    try:
      column=[column for column in Project.__table__.columns \
                       if column.key == target_column_name][0]
      project=self.fetch_records_by_column(table=Project, \
      	                                   column_name=column, \
      	                                   column_id=project_igf_id, \
      	                                   output_mode='one')
      return project  
    except:
      raise
    

  def get_project_user_info(self, output_mode='dataframe', project_igf_id=''):
    '''
    A method for fetching information from Project, User and ProjectUser table 
    optional params:
    project_igf_id: a project igf id
    output_mode   : dataframe / object
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
  
    session=self.session
    query=session.query(Project, User, ProjectUser.data_authority).join(ProjectUser).join(User)
    if project_igf_id:
      query=query.filter(Project.project_igf_id==project_igf_id)
    
    try:    
      results=self.fetch_records(query=query, output_mode=output_mode)
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

  
