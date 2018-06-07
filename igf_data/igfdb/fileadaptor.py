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
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A Toggle for automatically saving changes to db, default True
    '''
    (file_data, file_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    try:
      self.store_file_data(data=file_data) 
      if len(file_attr_data.index)>0:                                           # check if any attribute exists
        self.store_file_attributes(data=file_attr_data) 

      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='file_path',
                                         attribute_name_column='attribute_name',
                                         attribute_value_column='attribute_value'):
    '''
    A method for separating data for File and File_attribute tables
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param required_column: A column name to add to the attribute data
    :param attribute_name_column: A label for attribute name column
    :param attribute_value_column: A label for attribute value column
    :returns: Two pandas dataframes, one for File and another for File_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)
 
      file_columns=self.get_table_columns(table_name=File,
                                          excluded_columns=['file_id'])         # get required columns for file table
      (file_df, file_attr_df)=BaseAdaptor.divide_data_to_table_and_attribute(\
                                            self,
                                            data=data,
    	                                      required_column=required_column,
    	                                      table_columns=file_columns,
                                            attribute_name_column=attribute_name_column,
                                            attribute_value_column=attribute_value_column
                                          )                                     # divide dataframe
      return (file_df, file_attr_df)
    except:
      raise


  def store_file_data(self, data, autosave=False):
    '''
    Load data to file table
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A Toggle for automatically saving changes to db, default True
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)     

    try:
      self.store_records(table=File, data=data )                                # store data without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_file_attributes(self, data, file_id='', autosave=False):
    '''
    A method for storing data to File_attribute table
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param file_id: A file_id for updating the attribute table, default empty string
    :param autosave: A Toggle for automatically saving changes to db, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      if 'file_path' in data.columns:
        map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                      data=x,
                                      lookup_table=File,
                                      lookup_column_name='file_path',
                                      target_column_name='file_id')             # prepare the map function for File id
        new_data=data.apply(map_function, 1)                                    # map file id
        data=new_data                                                           # overwrite data

      self.store_attributes(attribute_table=File_attribute,
                            linked_column='file_id',
                            db_id=file_id,
                            data=data)                                          # store data without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def fetch_file_records_file_path(self, file_path):
    '''
    A method for fetching data for file table
    
    :param file_path: an absolute file path
    :returns: A file object
    '''
    try:
      file_obj=self.fetch_records_by_column(table=File,
                                            column_name=File.file_path,
                                            column_id=file_path,
                                            output_mode='one')
      return file_obj
    except:
      raise


  def check_file_records_file_path(self,file_path):
    '''
    A method for checking file information in database
    
    :param file_path: A absolute filepath
    :returns: True if the file is present in db or False if its not
    '''
    try:
      file_check=False
      file_obj=self.fetch_records_by_column(table=File,
                                            column_name=File.file_path,
                                            column_id=file_path,
                                            output_mode='one_or_none')
      if file_obj:
        file_check=True
      return file_check
    except:
      raise


  def remove_file_data_for_file_path(self,file_path,autosave=True):
    '''
    A method for removing entry for a specific file.
    
    :param file_path: A complete file_path for checking database
    :param autosave: A toggle for automatically saving changes to database, default True
    '''
    try:
      file_exists=self.check_file_records_file_path(file_path=file_path)
      if not file_exists:
        raise ValueError('File {0} not found in database'.format(file_path))

      self.session.\
      query(File).\
      filter(File.file_path==file_path).\
      delete(synchronize_session=False)                                         # remove record from db

      if autosave:
        self.commit_session()                                                   # save changes to database

    except:
      raise


  def update_file_table_for_file_path(self,file_path,tag,value,autosave=False):
    '''
    A method for updating file table
    
    :param file_path: A file_path for database look up
    :param tag: A keyword for file column name
    :param value: A new value for the file column
    :param autosave: Toggle autosave, default off
    '''
    try:
      file_columns=self.get_table_columns(table_name=File,
                                          excluded_columns=['file_id'])
      if tag not in file_columns:
        raise ValueError('column name {0} not allowed for table File'.\
                         format(tag))
      query=self.session.\
            query(File).\
            filter(File.file_path==file_path).\
            update({tag:value},synchronize_session=False)
      if autosave:
        self.commit_session()
    except:
      raise

