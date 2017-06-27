import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.useradaptor import ExperimentAdaptor
from igf_data.igfdb.igfTables import Experiment, Run, Run_attribute, Seqrun

class RunAdaptor(BaseAdaptor):
  '''
   An adaptor class for Run and Run_attribute tables
  '''

  def store_run_and_attribute_data(self, data):
    '''
    A method for dividing and storing data to run and attribute table
    '''
    (run_data, run_attr_data)=self.divide_data_to_table_and_attribute(data=data)

    try:
      seqrun_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                               lookup_table=Seqrun, \
                                                                               lookup_column_name='seqrun_igf_id', \     
                                                                               target_column_name='seqrun_id')        # prepare seqrun mapping function
      new_run_data=run_data.apply(seqrun_map_function, axis=1)                                                        # map seqrun id
      self.store_run_data(data=new_run_data)                                                                              # store run
      exp_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                            lookup_table=Experiment, \
                                                                            lookup_column_name='experiment_igf_id', \
                                                                            target_column_name='experiment_id')       # prepare the function
      new_run_attr_data=run_attr_data.apply(exp_map_function, axis=1)                                                 # map foreign key id
      self.store_run_attributes(data=new_run_attr_data)                                                               # store run attributes
      self.commit_session()                                                                                           # save changes to database
    except:
      self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='run_igf_id', attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for Run and Run_attribute tables
    required params:
    required_column: column name to add to the attribute data
    attribute_name_column: label for attribute name column
    attribute_value_column: label for attribute value column

    It returns two pandas dataframes, one for Run and another for Run_attribute table

    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    run_columns=self.get_table_columns(table_name=Run, excluded_columns=['run_id', 'seqrun_id'])                      # get required columns for run table
    (run_df, run_attr_df)=super(RunAdaptor, self).divide_data_to_table_and_attribute( \
                                                      data=data, \
    	                                              required_column=required_column, \
    	                                              table_columns=project_columns,  \
                                                      attribute_name_column=attribute_name_column, \
                                                      attribute_value_column=attribute_value_column \
                                                    )                                                                 # divide data to run and attribute table
    return (run_df, run_attr_df)
   

  def store_run_data(self, data):
    '''
    Load data to Run table
    '''
    try:
      self.store_records(table=Run, data=data)
    except:
      raise


  def store_run_attributes(self, data, run_id=''):
    '''
    A method for storing data to Run_attribute table
    '''
    try:
      self.store_attributes(attribute_table=Run_attribute, linked_column='run_id', db_id=run_id, data=data)
    except:
      raise


  def fetch_run_records_igf_id(self, run_igf_id, target_column_name='run_igf_id'):
    '''
    A method for fetching data for Run table
    required params:
    run_igf_id: an igf id
    target_column_name: a column name, default run_igf_id
    '''
    try:
      column=[column for column in Run.__table__.columns \
                       if column.key == target_column_name][0]
      run=self.fetch_records_by_column(table=Run, \
      	                                   column_name=column, \
      	                                   column_id=run_igf_id, \
      	                                   output_mode='one')
      return run  
    except:
      raise


