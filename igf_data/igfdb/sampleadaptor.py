import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, Sample, Sample_attribute, Experiment


class SampleAdaptor(BaseAdaptor):
  '''
  An adaptor class for Sample and Sample_attribute tables
  '''

  def store_sample_and_attribute_data(self, data):
    '''
    A method for dividing and storing data to sample and attribute table
    '''
    (sample_data, sample_attr_data)=self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_sample_data(data=sample_data)                                         # store sample records
      if len(sample_attr_data.columns) > 0:                                            # check if any attribute is present
        self.store_sample_attributes(data=sample_attr_data)                            # store project attributes
      self.commit_session()
    except:
      self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='sample_igf_id', \
                                         attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for Sample and Sample_attribute tables
    required params:
    required_column: column name to add to the attribute data
    attribute_name_column: label for attribute name column
    attribute_value_column: label for attribute value column

    It returns two pandas dataframes, one for Sample and another for Sample_attribute table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    sample_columns=self.get_table_columns(table_name=Sample, excluded_columns=['sample_id', 'project_id'])
    sample_columns.extend(['project_igf_id'])
    (sample_df, sample_attr_df)=super(SampleAdaptor, self).divide_data_to_table_and_attribute( \
                                                               data=data, \
    	                                                       required_column=required_column, \
    	                                                       table_columns=sample_columns,  \
                                                               attribute_name_column=attribute_name_column, \
                                                               attribute_value_column=attribute_value_column
                                                             )
    return (sample_df, sample_attr_df)


  def store_sample_data(self, data):
    '''
    Load data to Sample table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                          # convert data to dataframe

      if 'project_igf_id' in data.columns:
        project_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                                data=x, \
                                                lookup_table=Project, \
                                                lookup_column_name='project_igf_id', \
                                                target_column_name='project_id')         # prepare the function for project
        new_data=data.apply(project_map_function,1)                                      # map project id
        data=new_data                                                                    # overwrite data

      self.store_records(table=Sample, data=data)                                        # store data without autocommit
    except:
      raise


  def store_sample_attributes(self, data, sample_id=''):
    '''
    A method for storing data to Sample_attribute table
    required columns:
    data: a dataframe or dictionary containing the Sample_attribute data
    sample_id: an optional parameter to link the sample attributes to a specific sample
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                         # convert data to dataframe

      if 'sample_igf_id' in data.columns: 
        sample_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                                data=x, \
                                                lookup_table=Sample, \
                                                lookup_column_name='sample_igf_id', \
                                                target_column_name='sample_id')         # prepare the function for sample
        new_data=data.apply(sample_map_function, 1)                                     # map sample id
        data=new_data                                                                   # overwrite data

      self.store_attributes(data=data, attribute_table=Sample_attribute, linked_column='sample_id', db_id=sample_id)  # store without autocommit
    except:
      raise


  def fetch_sample_records_igf_id(self, sample_igf_id, target_column_name='sample_igf_id'):
    '''
    A method for fetching data for Sample table
    required params:
    sample_igf_id: an igf id
    output_mode  : dataframe / object
    '''
    try:
      column=[column for column in Sample.__table__.columns \
                       if column.key == target_column_name][0]
      sample=self.fetch_records_by_column(table=Sample, \
      	                                   column_name=column, \
      	                                   column_id=sample_igf_id, \
      	                                   output_mode='one')
      return sample  
    except:
      raise


