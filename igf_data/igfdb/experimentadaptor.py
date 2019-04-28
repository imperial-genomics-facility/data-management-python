import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project,Sample,Experiment,Experiment_attribute,Sample,Sample_attribute,Run

class ExperimentAdaptor(BaseAdaptor):
  '''
  An adaptor class for Experiment and Experiment_attribute tables
  '''

  def store_project_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to experiment and attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame
    :param autosave: A toggle for automatically saving data to db, default True
    '''
    (experiment_data, experiment_attr_data)=\
      self.divide_data_to_table_and_attribute(data=data)
    
    try:
      self.store_experiment_data(data=experiment_data)                          # store experiment data

      if len(experiment_attr_data.index) > 0:                                   # check if any attribute is present of not
        self.store_experiment_attributes(data=experiment_attr_data)             # store run attributes

      if autosave:
        self.commit_session() 
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, table_columns=None,
                                         required_column='experiment_igf_id',
                                         attribute_name_column='attribute_name',
                                         attribute_value_column='attribute_value'):
    '''
    A method for separating data for Experiment and Experiment_attribute tables
    
    :param data: A list of dictionaries or a Pandas DataFrame
    :param table_columns: List of table column names, default None
    :param required_column: column name to add to the attribute data
    :param attribute_name_column: label for attribute name column
    :param attribute_value_column: label for attribute value column
    :returns: Two pandas dataframes, one for Experiment and another for Experiment_attribute table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    experiment_columns=\
      self.get_table_columns(\
        table_name=Experiment,
        excluded_columns=['experiment_id',
                          'project_id',
                          'sample_id' ])                                        # get required columns for experiment table
    experiment_columns.extend(['project_igf_id',
                               'sample_igf_id'])                                # add required columns
    (experiment_df, experiment_attr_df)=\
      BaseAdaptor.\
        divide_data_to_table_and_attribute(\
          self,
          data=data,
          required_column=required_column,
          table_columns=experiment_columns,
          attribute_name_column=attribute_name_column,
          attribute_value_column=attribute_value_column
        )                                                                       # divide data to experiment and adatpor
    return (experiment_df, experiment_attr_df)


  def store_experiment_data(self, data, autosave=False):
    '''
    Load data to Experiment table
    
    :param data: A list of dictionaries or a Pandas DataFrame
    :param autosave: A toggle for automatically saving data to db, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                 # convert data to dataframe

      if 'project_igf_id' in data.columns:
        project_map_function=\
          lambda x: self.map_foreign_table_and_store_attribute(\
                      data=x,
                      lookup_table=Project,
                      lookup_column_name='project_igf_id',
                      target_column_name='project_id')                          # prepare the function for Project id
        new_data=data.apply(project_map_function, axis=1)                       # map project id foreign key id
        data=new_data                                                           # overwrite data

      if 'sample_igf_id' in data.columns:
        sample_map_function=\
          lambda x: self.map_foreign_table_and_store_attribute( \
                      data=x,
                      lookup_table=Sample,
                      lookup_column_name='sample_igf_id',
                      target_column_name='sample_id')                           # prepare the function for Sample id
        new_data=data.apply(sample_map_function, axis=1)                        # map sample id foreign key id
        data=new_data

      self.store_records(table=Experiment, data=data)                           # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_experiment_attributes(self, data, experiment_id='', autosave=False):
    '''
    A method for storing data to Experiment_attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame for experiment attribute data
    :param experiment_id: An optional experiment_id to link attribute records
    :param autosave: A toggle for automatically saving data to db, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                 # convert data to dataframe

      if 'experiment_igf_id' in data.columns:
        exp_map_function=\
          lambda x: self.map_foreign_table_and_store_attribute( \
                      data=x,
                      lookup_table=Experiment,
                      lookup_column_name='experiment_igf_id',
                      target_column_name='experiment_id')                       # prepare the function
        new_data=data.apply(exp_map_function, axis=1)                           # map foreign key id
        data=new_data                                                           # overwrite data

      self.store_attributes(attribute_table=Experiment_attribute,
                            linked_column='experiment_id',
                            db_id=experiment_id,
                            data=data)                                          # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_experiment_records_id(self, experiment_igf_id,
                                  target_column_name='experiment_igf_id'):
    '''
    A method for fetching data for Experiment table
    
    :param experiment_igf_id: an igf id
    :param target_column_name: a column name, default experiment_igf_id
    :returns: Experiment object
    '''
    try:
      column=[column for column in Experiment.__table__.columns \
                       if column.key == target_column_name][0]
      experiment=\
        self.fetch_records_by_column(\
          table=Experiment,
      	  column_name=column,
      	  column_id=experiment_igf_id,
      	  output_mode='one')
      return experiment  
    except:
      raise

  def check_experiment_records_id(self, experiment_igf_id,
                                  target_column_name='experiment_igf_id'):
    '''
    A method for checking existing data for Experiment table
    
    :param experiment_igf_id: an igf id
    :param target_column_name: a column name, default experiment_igf_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      experiment_check=False
      column=[column for column in Experiment.__table__.columns \
                       if column.key == target_column_name][0]
      experiment_obj=\
        self.fetch_records_by_column(\
          table=Experiment,
          column_name=column,
          column_id=experiment_igf_id,
          output_mode='one_or_none')
      if experiment_obj is not None:
        experiment_check=True
      return experiment_check
    except:
      raise

  def fetch_sample_attribute_records_for_experiment_igf_id(\
    self,experiment_igf_id,output_mode='dataframe',attribute_list=None):
    '''
    A method for fetching sample_attribute_records for a given experiment_igf_id
    
    :param experiment_igf_id: An experiment_igf_id
    :param output_mode: Result output mode, default dataframe
    :param attribute_list: A list of attributes for database lookup, default None
    :returns an object or dataframe based on the output_mode
    '''
    try:
      query=self.session.\
            query(Sample_attribute.attribute_name,
                  Sample_attribute.attribute_value).\
            join(Sample,Sample.sample_id==Sample_attribute.sample_id).\
            join(Experiment,Sample.sample_id==Experiment.sample_id).\
            filter(Sample.sample_id==Sample_attribute.sample_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Experiment.experiment_igf_id==experiment_igf_id)             # get basic query

      if attribute_list is not None and \
         isinstance(attribute_list,list) and \
         len(attribute_list)>0:
        query=\
          query.filter(Sample_attribute.attribute_name.in_(attribute_list))       # look for only specific attributes, if list provided
      results=\
        self.fetch_records(\
          query=query,
          output_mode=output_mode)                                              # fetch results
      return results
    except:
      raise

  def update_experiment_records_by_igf_id(self,experiment_igf_id,
                                          update_data,autosave=True):
    '''
    A method for updating experiment records in database
    
    :param experiment_igf_id: An igf ids for the experiment data lookup
    :param update_data: A dictionary containing the updated entries
    :param autosave: Toggle auto commit after database update, default True
    '''
    try:
      if not isinstance(update_data,dict):
        raise AttributeError('Expecting a dictionary with new data for experiment record update and got {0}'.\
                             format(type(update_data)))                         # check update data type before db update
      allowed_experiment_columns=\
        self.get_table_columns(\
          table_name=Experiment,
          excluded_columns='experiment_id')                                     # get list of allowed experiment columns
      for update_key in update_data.keys():
        if update_key not in allowed_experiment_columns:
          raise ValueError('Check your data, column {0} is not part of Experiment table'.\
                           format(update_key))                                  # check each key of the update_data dictionary
      query=self.session.\
            query(Experiment).\
            filter(Experiment.experiment_igf_id==experiment_igf_id)             # define base query
      query.update(update_data)                                                 # update data in db
      if autosave:
        self.commit_session()                                                   # save data if auto commit is on
    except:
      self.rollback_session()                                                   # rollback session if db update has failed
      raise


  def fetch_project_and_sample_for_experiment(self,experiment_igf_id):
    '''
    A method for fetching project and sample igf id information for an experiment
    
    :param experiment_igf_id: An experiment igf id string
    :returns: Two strings, project igf id and sample igd id, or None if not found
    '''
    try:
      project_igf_id=None
      sample_igf_id=None
      query=self.session.\
            query(Project.project_igf_id,Sample.sample_igf_id).\
            join(Sample,Project.project_id==Sample.project_id).\
            join(Experiment,Sample.sample_id==Experiment.sample_id).\
            filter(Project.project_id==Sample.project_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Experiment.experiment_igf_id==experiment_igf_id)
      data=\
        self.fetch_records(\
          query=query,
          output_mode='one_or_none')
      if data is not None:
        project_igf_id=data.project_igf_id
        sample_igf_id=data.sample_igf_id

      return project_igf_id,sample_igf_id
    except:
      raise

  def fetch_runs_for_igf_id(self,experiment_igf_id,include_active_runs=True,
                            output_mode='dataframe'):
    '''
    A method for fetching all the runs for a specific experiment_igf_id
    
    :param experiment_igf_id: An experiment_igf_id
    :param include_active_runs: Include only active runs, if its True, default True
    :param output_mode: Record fetch mode, default dataframe
    '''
    try:
      query=self.session.\
            query(Run.run_igf_id).\
            join(Experiment,Experiment.experiment_id==Run.experiment_id).\
            filter(Experiment.experiment_id==Run.experiment_id).\
            filter(Experiment.experiment_igf_id==experiment_igf_id)
      if include_active_runs:
        query=query.\
              filter(Run.status=='ACTIVE')

      data=\
        self.fetch_records(\
          query=query,
          output_mode=output_mode)
      return data
    except:
      raise

