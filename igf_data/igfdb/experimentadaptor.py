import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.useradaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import SampleAdaptor
from igf_data.igfdb.useradaptor import PlatformAdaptor
from igf_data.igfdb.igfTables import Project, Sample, Platform, Experiment, Experiment_attribute

class ExperimentAdaptor(BaseAdaptor):
  '''
  An adaptor class for Experiment and Experiment_attribute tables
  '''

  def store_project_and_attribute_data(self, data):
    '''
    A method for dividing and storing data to experiment and attribute table
    '''
    (experiment_data, experiment_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    
    try:
      project_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                        lookup_table=Project, \
                                                                        lookup_column_name='project_igf_id', \
                                                                        target_column_name='project_id')        # prepare the function for Project id
      sample_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                        lookup_table=Sample, \
                                                                        lookup_column_name='sample_igf_id', \
                                                                        target_column_name='sample_id')         # prepare the function for Sample id
      platform_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                        lookup_table=Platform, \
                                                                        lookup_column_name='platform_igf_id', \
                                                                        target_column_name='platform_id')       # prepare the function for Platform id
      new_experiment_attr_data=experiment_attr_data.apply(project_map_function, axis=1)                         # map project id foreign key id
      new_experiment_attr_data=new_experiment_attr_data.apply(sample_map_function, axis=1)                      # map sample id foreign key id
      new_experiment_attr_data=new_experiment_attr_data.apply(platform_map_function, axis=1)                    # map platform id foreign key id
      self.commit_session() 
    except:
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

    experiment_columns=self.get_table_columns(table_name=Experiment, excluded_columns=['experiment_id'])           # get required columns for experiment table
    (experiment_df, experiment_attr_df)=super(ExperimentAdaptor, self).divide_data_to_table_and_attribute( \
                                                                     data=data, \
    	                                                             required_column=required_column, \
    	                                                             table_columns=experiment_columns,  \
                                                                     attribute_name_column=attribute_name_column, \
                                                                     attribute_value_column=attribute_value_column
                                                                   )
    return (experiment_df, experiment_attr_df)


  def store_experiment_data(self, data):
    '''
    Load data to Experiment table
    '''
    try:
      self.store_records(table=Experiment, data=data)
    except:
      raise


  def store_experiment_attributes(self, data, experiment_id=''):
    '''
    A method for storing data to Experiment_attribute table
    '''
    try:
      self.store_attributes(attribute_table=Experiment_attribute, linked_column='experiment_id', db_id=experiment_id, data=data)
    except:
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


