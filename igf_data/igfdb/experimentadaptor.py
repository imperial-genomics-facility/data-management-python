import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.igfTables import Project, Sample, Experiment, Experiment_attribute

class ExperimentAdaptor(BaseAdaptor):
  '''
  An adaptor class for Experiment and Experiment_attribute tables
  '''

  def store_project_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to experiment and attribute table
    '''
    (experiment_data, experiment_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    
    try:
      self.store_experiment_data(data=experiment_data)                                          # store experiment data

      if len(experiment_attr_data.columns) > 0:                                                 # check if any attribute is present of not
        self.store_experiment_attributes(data=experiment_attr_data)                             # store run attributes

      if autosave:
        self.commit_session() 
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='experiment_igf_id', attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for Experiment and Experiment_attribute tables
    required params:
    required_column: column name to add to the attribute data
    attribute_name_column: label for attribute name column
    attribute_value_column: label for attribute value column

    It returns two pandas dataframes, one for Experiment and another for Experiment_attribute table

    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    experiment_columns=self.get_table_columns(table_name=Experiment, excluded_columns=['experiment_id', 'project_id', 'sample_id' ])    # get required columns for experiment table
    experiment_columns.extend(['project_igf_id', 'sample_igf_id'])                                                                      # add required columns
    (experiment_df, experiment_attr_df)=BaseAdaptor.divide_data_to_table_and_attribute(self, \
                                                                           data=data, \
    	                                                                   required_column=required_column, \
    	                                                                   table_columns=experiment_columns,  \
                                                                           attribute_name_column=attribute_name_column, \
                                                                           attribute_value_column=attribute_value_column \
                                                                         )                                                              # divide data to experiment and adatpor
    return (experiment_df, experiment_attr_df)


  def store_experiment_data(self, data, autosave=False):
    '''
    Load data to Experiment table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                                   # convert data to dataframe

      if 'project_igf_id' in data.columns:
        project_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                data=x, \
                                                lookup_table=Project, \
                                                lookup_column_name='project_igf_id', \
                                                target_column_name='project_id')                  # prepare the function for Project id
        new_data=data.apply(project_map_function, axis=1)                                         # map project id foreign key id
        data=new_data                                                                             # overwrite data

      if 'sample_igf_id' in data.columns:
        sample_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                data=x, \
                                                lookup_table=Sample, \
                                                lookup_column_name='sample_igf_id', \
                                                target_column_name='sample_id')                   # prepare the function for Sample id
        new_data=data.apply(sample_map_function, axis=1)                                          # map sample id foreign key id
        data=new_data

      self.store_records(table=Experiment, data=data)                                             # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_experiment_attributes(self, data, experiment_id='', autosave=False):
    '''
    A method for storing data to Experiment_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                                   # convert data to dataframe

      if 'experiment_igf_id' in data.columns:
        exp_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                data=x, \
                                                lookup_table=Experiment, \
                                                lookup_column_name='experiment_igf_id', \
                                                target_column_name='experiment_id')               # prepare the function 
        new_data=data.apply(exp_map_function, axis=1)                                             # map foreign key id                                       
        data=new_data                                                                             # overwrite data

      self.store_attributes(attribute_table=Experiment_attribute, linked_column='experiment_id', db_id=experiment_id, data=data) # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_experiment_records_id(self, experiment_igf_id, target_column_name='experiment_igf_id'):
    '''
    A method for fetching data for Experiment table
    required params:
    experiment_igf_id: an igf id
    target_column_name: a column name, default experiment_igf_id
    '''
    try:
      column=[column for column in Experiment.__table__.columns \
                       if column.key == target_column_name][0]
      experiment=self.fetch_records_by_column(table=Experiment, \
      	                                   column_name=column, \
      	                                   column_id=experiment_igf_id, \
      	                                   output_mode='one')
      return experiment  
    except:
      raise


