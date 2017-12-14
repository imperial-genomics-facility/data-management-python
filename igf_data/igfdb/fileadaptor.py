import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import File, File_attribute

class FileAdaptor(BaseAdaptor):
  '''
  An adaptor class for File tables
  '''
  def store_file_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to file and attribute table
    '''
    (file_data, file_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    try:
      self.store_file_data(data=file_data) 
      if len(file_attr_data.columns)>0:                             # check if any attribute exists
        self.store_file_attributes(data=file_attr_data) 

      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='file_path', attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for File and File_attribute tables
    required params:
    required_column: column name to add to the attribute data
    attribute_name_column: label for attribute name column
    attribute_value_column: label for attribute value column
    It returns two pandas dataframes, one for File and another for File_attribute table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)
 
    file_columns=self.get_table_columns(table_name=File, excluded_columns=['file_id'])                             # get required columns for file table
    (file_df, file_attr_df)=BaseAdaptor.divide_data_to_table_and_attribute(self, \
                                                                     data=data, \
    	                                                             required_column=required_column, \
    	                                                             table_columns=file_columns,  \
                                                                     attribute_name_column=attribute_name_column, \
                                                                     attribute_value_column=attribute_value_column
                                                                    )                                               # divide dataframe
    return (file_df, file_attr_df)


  def store_file_data(self, data, autosave=False):
    '''
    Load data to file table
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)     

    try:
      self.store_records(table=File, data=data )                                      # store data without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_file_attributes(self, data, file_id='', autosave=False):
    '''
    A method for storing data to File_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      if 'file_path' in data.columns:
        map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                        data=x, \
                                        lookup_table=File, \
                                        lookup_column_name='file_path', \
                                        target_column_name='file_id')                 # prepare the map function for File id
        new_data=data.apply(map_function, 1)                                          # map file id
        data=new_data                                                                 # overwrite data

      self.store_attributes(attribute_table=File_attribute, linked_column='file_id', db_id=file_id, data=data) # store data without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_file_records_file_path(self, file_path):
    '''
    A method for fetching data for file table
    required params:
    file_path: an absolute file path
    '''
    try:
      file_obj=self.fetch_records_by_column(table=File, column_name=File.file_path, column_id=file_path, output_mode='one')
      return file_obj
    except:
      raise
    
  def check_file_records_file_path(self,file_path):
    '''
    A method for checking file information in database
    required params:
    file_path: A absolute filepath
    It returns True if the file is present in db or False if its not
    '''
    try:
      file_check=False
      file_obj=self.fetch_records_by_column(table=File, column_name=File.file_path, column_id=file_path, output_mode='one_or_none')
      if file_obj:
        file_check=True
      return file_check
    except:
      raise


