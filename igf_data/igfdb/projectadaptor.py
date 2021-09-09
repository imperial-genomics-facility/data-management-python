import pandas as pd
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, ProjectUser, Project_attribute, User, Sample

class ProjectAdaptor(BaseAdaptor):
  '''
  An adaptor class for Project, ProjectUser and Project_attribute tables
  '''

  def store_project_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to project and attribute_table

    :param data: A list of data or a pandas dataframe
    :param autosave: A toggle for autocommit, default True
    :returns: None
    '''
    (project_data, project_attr_data) = \
      self.divide_data_to_table_and_attribute(data=data)
    try:
      self.store_project_data(data=project_data)                                # store project
      if len(project_attr_data.index) > 0:                                      # check if any attribute is present
        self.store_project_attributes(data=project_attr_data)                   # store project attributes
      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              "Failed to store project and attribute records, error: {0}".\
                format(e))



  def divide_data_to_table_and_attribute(
        self, data, required_column='project_igf_id', attribute_name_column='attribute_name',
        attribute_value_column='attribute_value'):
    '''
    A method for separating data for Project and Project_attribute tables

    :param data: A list of dictionaries or a pandas dataframe
    :param required_column: Name of the required column, default project_igf_id
    :param attribute_name_column: Value for attribute name column, default attribute_name
    :param attribute_value_column: Valye for attribute value column, default attribute_value
    :returns: A project dataframe and a project attribute dataframe
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)
      project_columns = \
        self.get_table_columns(
          table_name=Project,
          excluded_columns=['project_id'])                                      # get required columns for project table
      (project_df, project_attr_df) = \
        BaseAdaptor.\
          divide_data_to_table_and_attribute(
            self,
            data=data,
            required_column=required_column,
            table_columns=project_columns,
            attribute_name_column=attribute_name_column,
            attribute_value_column=attribute_value_column)
      return (project_df, project_attr_df)
    except Exception as e:
      raise ValueError(
              "Failed to divide data to project table and attribute record, error: {0}".\
                format(e))


  def store_project_data(self, data, autosave=False):
    '''
    Load data to Project table

    :param data: A list of data or a pandas dataframe
    :param autosave: A toggle for autocommit, default False
    :returns: None
    '''
    try:
      self.store_records(
        table=Project,
        data=data)
      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              "Failed to store project data, error: {0}".format(e))


  def store_project_attributes(self, data, project_id='', autosave=False):
    '''
    A method for storing data to Project_attribute table

    :param data: A pandas dataframe
    :param project_id: Project id for attribute table, default ''
    :param autosave: A toggle for autocommit, default False
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert data to dataframe

      if 'project_igf_id' in data.columns:                                      # map foreign key if project_igf_id is found
        map_function = \
          lambda x: self.map_foreign_table_and_store_attribute(
            data=x,
            lookup_table=Project,
            lookup_column_name='project_igf_id',
            target_column_name='project_id')                                    # prepare the function
        data['project_id'] = ''
        data = \
          data.apply(
            map_function,
            axis=1,
            result_type=None)                                                   # map foreign key id
        #data = new_data                                                        # overwrite data
        data.drop(
          'project_igf_id',
          axis=1,
          inplace=True)
      self.store_attributes(
        attribute_table=Project_attribute,
        linked_column='project_id',
        db_id=project_id, data=data)                                            # store attributes without auto commit
      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              "Failed to store project attribute data, error: {0}".\
                format(e))


  def assign_user_to_project(
        self, data, required_project_column='project_igf_id', required_user_column='email_id',
        data_authority_column='data_authority', autosave=True):
    '''
    Load data to ProjectUser table

    :param data: A list of dictionaries, each containing 'project_igf_id' and 'user_igf_id' as key
                 with relevent igf ids as the values. An optional key 'data_authority' with
                 boolean value can be provided to set the user as the data authority of the project
                 E.g.
                 [{'project_igf_id': val, 'email_id': val, 'data_authority':True},]
    :param required_project_column: Name of the project id column, default project_igf_id
    :param required_user_column: Name of the user id column, default email_id
    :param data_authority_column: Name of the data_authority column, default data_authority
    :param autosave: A toggle for autocommit to db, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      if not set((required_project_column,
                  required_user_column,
                  data_authority_column)).\
             issubset(set(tuple(data.columns))):                                # check for required parameters
        raise ValueError('Missing required value in input data {0}'.\
                         format(data.columns))

      project_map_function = \
        lambda x: self.map_foreign_table_and_store_attribute(
          data=x,
          lookup_table=Project,
          lookup_column_name=required_project_column,
          target_column_name='project_id' )                                     # prepare the function for Project id
      data['project_id'] = ''
      data = data.apply(project_map_function, axis=1,result_type=None)                            # map project id
      data.drop(required_project_column,axis=1,inplace=True)
      user_map_function = \
        lambda x: self.map_foreign_table_and_store_attribute(\
          data=x,
          lookup_table=User,
          lookup_column_name=required_user_column,
          target_column_name='user_id' )                                        # prepare the function for User id
      data['user_id'] = ''
      data = \
        data.apply(
          user_map_function,
          axis=1,
          result_type=None)                                                     # map user id
      data.drop(
        required_user_column,
        axis=1,
        inplace=True)
      data_authotiry_dict = {True:'T'}                                          # create a mapping dictionary for data authority value
      data[data_authority_column] = \
        data[data_authority_column].\
        map(data_authotiry_dict)                                                # add value for data authority
      self.store_records(table=ProjectUser, data=data)                      # store the project_user data
      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              "Failed to assign users to project, error: {0}".\
                format(e))


  def check_project_records_igf_id(
        self, project_igf_id, target_column_name='project_igf_id'):
    '''
    A method for checking existing data for Project table

    :param project_igf_id: Project igf id name
    :param target_column_name: Name of the project id column, default project_igf_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      project_check = False
      column = [
        c for c in Project.__table__.columns \
          if c.key == target_column_name][0]
      project_obj = \
        self.fetch_records_by_column(
          table=Project,
          column_name=column,
          column_id=project_igf_id,
          output_mode='one_or_none')
      if project_obj is not None:
        project_check=True
      return project_check
    except Exception as e:
      raise ValueError(
              "Failed to check project records, error: {0}".format(e))


  def fetch_project_records_igf_id(self, project_igf_id, target_column_name='project_igf_id'):
    '''
    A method for fetching data for Project table

    :param project_igf_id: an igf id
    :param output_mode: dataframe / object / one
    :returns: Records from project table
    '''
    try:
      column = [
        c for c in Project.__table__.columns \
          if c.key == target_column_name][0]
      project = \
        self.fetch_records_by_column(
          table=Project,
          column_name=column,
          column_id=project_igf_id,
          output_mode='one')
      return project
    except Exception as e:
      raise ValueError(
              "Failed to fetch project record, error: {0}".format(e))


  def get_project_user_info(self, output_mode='dataframe', project_igf_id=''):
    '''
    A method for fetching information from Project, User and ProjectUser table 

    :param project_igf_id: a project igf id
    :param output_mode   : dataframe / object
    :returns: Records for project user
    '''
    try:
      if not hasattr(self, 'session'):
        raise AttributeError('Attribute session not found')

      session = self.session
      query = \
        session.\
          query(Project,
                User,
                ProjectUser.data_authority).\
          join(ProjectUser,
               Project.project_id==ProjectUser.project_id).\
          join(User,
               User.user_id==ProjectUser.user_id)
      if project_igf_id:
        query = \
          query.filter(
            Project.project_igf_id==project_igf_id)

      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
              "Failed to get project user info, error: {0}".format(e))


  def check_existing_project_user(self, project_igf_id, email_id):
    '''
    A method for checking existing project use info in database

    :param project_igf_id: A project_igf_id
    :param email_id: An email_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      project_user_check = False
      session = self.session
      query = \
        session.\
          query(Project,
                User,
                ProjectUser.data_authority).\
          join(ProjectUser,
               Project.project_id==ProjectUser.project_id).\
          join(User,
               User.user_id==ProjectUser.user_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(User.email_id==email_id)
      results = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if results is not None:
        project_user_check=True
      return project_user_check
    except Exception as e:
      raise ValueError(
              "Failed to check existing project user, error: {0}".format(e))


  def check_data_authority_for_project(self, project_igf_id):
    '''
    A method for checking user data authority for existing projects

    :param project_igf_id: An unique project igf id
    :returns: True if data authority exists for project or false
    '''
    try:
      project_user_check = False
      session = self.session
      query = \
        session.\
          query(Project).\
          join(ProjectUser,
               Project.project_id==ProjectUser.project_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(ProjectUser.data_authority=='T')
      results = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if results is not None:
        project_user_check=True
      return project_user_check
    except Exception as e:
      raise ValueError(
              "Failed to check data authority record, error: {0}".format(e))


  def fetch_data_authority_for_project(self, project_igf_id):
    '''
    A method for fetching user data authority for existing projects

    :param project_igf_id: An unique project igf id
    :returns: A user object or None, if no entry found
    '''
    try:
      session = self.session
      query = \
        session.\
          query(User).\
          join(ProjectUser,
               User.user_id==ProjectUser.user_id).\
          join(Project,
               Project.project_id==ProjectUser.project_id).\
          filter(Project.project_id==ProjectUser.project_id).\
          filter(User.user_id==ProjectUser.user_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(ProjectUser.data_authority=='T')
      results = \
        self.fetch_records(
          query=query, \
          output_mode='one_or_none')
      return results
    except Exception as e:
      raise ValueError(
             "Failed to fetch data authority for project,error: {0}".format(e))


  def check_project_attributes(self, project_igf_id, attribute_name): 
    '''
    A method for checking existing project attribute in database

    :param project_igf_id: An unique project igf id
    :param attribute_name: An attribute name
    :return A boolean value
    '''
    try:
      project_attribute_check = False
      session = self.session
      query = \
        session.\
          query(Project).\
          join(Project_attribute,
               Project.project_id==Project_attribute.project_id).\
          filter(Project.project_igf_id==project_igf_id).\
          filter(Project_attribute.attribute_name==attribute_name)
      results=\
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if results is not None:
        project_attribute_check=True
      return project_attribute_check
    except Exception as e:
      raise ValueError(
              "Failed to check project attribute records, error: {0}".format(e))

  def get_project_attributes(self, project_igf_id, linked_column_name='project_id',
                             attribute_name=''):
    '''
    A method for fetching entries from project attribute table

    :param project_igf_id: A project_igf_id string
    :param attribute_name: An attribute name, default in None
    :param linked_column_name: A column name for linking attribute table
    :returns dataframe of records
    '''
    try:
      project = \
        self.fetch_project_records_igf_id(
          project_igf_id=project_igf_id)

      project_attributes = \
        BaseAdaptor.\
          get_attributes_by_dbid(
            self,
            attribute_table=Project_attribute,
            linked_table=Project,
            linked_column_name=linked_column_name,
            db_id=project.project_id )
      return project_attributes
    except Exception as e:
      raise ValueError(
              "Failed to get project attributes, error: {0}".format(e))


  def fetch_project_samples(self, project_igf_id, only_active=True,
                            output_mode='object'):
    '''
    A method for fetching all the samples for a specific project

    :param project_igf_id: A project id
    :param only_active: Toggle for including only active projects, default is True
    :param output_mode: Output mode, default object
    :returns: Depends on the output_mode, a generator expression, dataframe or an object
    '''
    try:
      query = \
        self.session.\
          query(Sample).\
          join(Project,
               Project.project_id==Sample.project_id).\
          filter(Project.project_id==Sample.project_id).\
          filter(Project.project_igf_id==project_igf_id)
      if only_active:
        query=query.filter(Sample.status=='ACTIVE')                                   # checking only active projects

      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
              "Failed to fetch project sample records, error: {0}".format(e))


  def count_project_samples(self,project_igf_id,only_active=True):
    '''
    A method for counting total number of samples for a project

    :param project_igf_id: A project id
    :param only_active: Toggle for including only active projects, default is True
    :returns: A int sample count
    '''
    try:
      results_data = \
        self.fetch_project_samples(
          project_igf_id=project_igf_id,
          only_active=only_active,
          output_mode='dataframe')                                              # fetch samples as dataframe
      count = \
        len(results_data.index)                                                 # count dataframe row
      return count
    except Exception as e:
      raise ValueError(
              "Failed to count project samples, error: {0}".format(e))

  def fetch_all_project_igf_ids(self,output_mode='dataframe'):
    '''
    A method for fetching a list of all project igf ids

    :param output_mode: Output mode, default dataframe
    '''
    try:
      query = \
        self.session.\
          query(Project.project_igf_id)
      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return results
    except Exception as e:
      raise ValueError(
              "Failed to fetch the list of all project igf ids, error: {0}".\
              format(e))


if __name__=='__main__':
  import os
  from igf_data.igfdb.igfTables import Base

  if os.path.exists('adapter_test.sqlite'):
    os.remove('adapter_test.sqlite')
  dbparam = {'dbname':'adapter_test.sqlite','driver':'sqlite'}
  base = BaseAdaptor(**dbparam)
  engine = base.engine
  Base.metadata.create_all(engine)
  base.start_session()
  project_data = [{
    'project_igf_id':'IGFP0001_test_22-8-2017_rna',
    'project_name':'test_22-8-2017_rna',
    'description':'Its project 1',
    'project_deadline':'Before August 2017',
    'comments':'Some samples are treated with drug X' }]#,
   ##{'project_igf_id':'IGFP0002_test_22-8-2017_rna',
    #'project_name':'test_23-8-2017_rna',
    #'description':'Its project 2',
    ##'project_deadline':'Before August 2017',
    #'comments':'Some samples are treated with drug X' }]
  pa = ProjectAdaptor(**{'session':base.session})
  p_df, pa_df = pa.divide_data_to_table_and_attribute(data=project_data)
  print('a',p_df.to_dict(orient='records'))
  print('b',pa_df.to_dict(orient='records'))
  pa.store_project_data(p_df,autosave=True)
  pa.store_project_attributes(pa_df,autosave=True)
  base.close_session()