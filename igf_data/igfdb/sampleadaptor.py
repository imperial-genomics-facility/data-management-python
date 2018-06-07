import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project, Sample, Sample_attribute, Experiment


class SampleAdaptor(BaseAdaptor):
  '''
  An adaptor class for Sample and Sample_attribute tables
  '''

  def store_sample_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to sample and attribute table
    '''
    (sample_data, sample_attr_data)=self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_sample_data(data=sample_data)                                         # store sample records
      if len(sample_attr_data.index) > 0:                                            # check if any attribute is present
        self.store_sample_attributes(data=sample_attr_data)                            # store project attributes
      if autosave:
        self.commit_session()
    except:
      if autosave:
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
    (sample_df, sample_attr_df)=BaseAdaptor.divide_data_to_table_and_attribute(self, \
                                                               data=data, \
    	                                                       required_column=required_column, \
    	                                                       table_columns=sample_columns,  \
                                                               attribute_name_column=attribute_name_column, \
                                                               attribute_value_column=attribute_value_column
                                                             )
    return (sample_df, sample_attr_df)


  def store_sample_data(self, data, autosave=False):
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
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_sample_attributes(self, data, sample_id='', autosave=False):
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
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
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
    
  def check_sample_records_igf_id(self, sample_igf_id, target_column_name='sample_igf_id'):
    '''
    A method for checking existing data for sample table
    required params:
    sample_igf_id: an igf id
    It returns True if the file is present in db or False if its not
    '''
    try:
      sample_check=False
      column=[column for column in Sample.__table__.columns \
                       if column.key == target_column_name][0]
      sample_obj=self.fetch_records_by_column(table=Sample, \
                                              column_name=column, \
                                              column_id=sample_igf_id, \
                                              output_mode='one_or_none')
      if sample_obj is not None:
        sample_check=True
      return sample_check
    except:
      raise
    
    
  def check_project_and_sample(self,project_igf_id,sample_igf_id):
    '''
    A method for checking existing project and sample igf id combination
    in sample table
    
    required params:
    project_igf_id
    sample_igf_id
    It returns True if target entry is present or return False
    '''
    try:
      sample_check=False
      query=self.session.\
                 query(Sample).\
                 join(Project).\
                 filter(Sample.sample_igf_id==sample_igf_id).\
                 filter(Project.project_igf_id==project_igf_id)                 # construct join query
      sample_object=self.fetch_records(query=query,output_mode='one_or_none')   # check for existing records
      if sample_object is not None:
        sample_check=True
      return sample_check
    except:
      raise

  def fetch_sample_project(self, sample_igf_id):
    '''
    A method for fetching project information for the sample
    
    :param sample_igf_id: A sample_igf_id for database lookup
    :returns: A project_igf_id or None, if not found
    '''
    try:
      query=self.session.\
            query(Project.project_igf_id).\
            join(Sample).\
            filter(Project.project_id==Sample.project_id).\
            filter(Sample.sample_igf_id==sample_igf_id)                         # set query
      project=self.fetch_records(query=query,
                                output_mode='one_or_none')                      # fetch project record
      if project is not None:
        project=project.project_igf_id

      return project
    except:
      raise