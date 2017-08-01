import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Collection, File, Collection_group, Collection_attribute

class CollectionAdaptor(BaseAdaptor):
  '''
  An adaptor class for Collection, Collection_group and Collection_attribute tables
  '''

  def store_collection_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to collection and attribute table
    '''
    (collection_data, collection_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    try:
      self.store_collection_data(data=collection_data)                                                        # store collection data
      if len(collection_attr_data.columns) > 0:
        self.store_collection_attributes(data=collection_attr_data)                                           # store project attributes 

      if autosave:
        self.commit_session()                                                                                 # save changes to database
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column=['name', 'type'], attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for Collection and Collection_attribute tables
    required params:
    required_column: column name to add to the attribute data
    attribute_name_column: label for attribute name column
    attribute_value_column: label for attribute value column

    It returns two pandas dataframes, one for Collection and another for Collection_attribute table

    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    collection_columns=self.get_table_columns(table_name=Collection, excluded_columns=['collection_id'])           # get required columns for collection table    
    (collection_df, collection_attr_df)=super(CollectionAdaptor, self).divide_data_to_table_and_attribute( \
                                                                     data=data, \
    	                                                             required_column=required_column, \
    	                                                             table_columns=collection_columns,  \
                                                                     attribute_name_column=attribute_name_column, \
                                                                     attribute_value_column=attribute_value_column
    	                                                        )
    return (collection_df, collection_attr_df)

 
  def store_collection_data(self, data, autosave=False):
    '''
    Load data to Collection table
    '''
    try:
      self.store_records(table=Collection, data=data)
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_collection_attributes(self, data, collection_id='', autosave=False):
    '''
    A method for storing data to Collectionm_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                                                  # convert data to dataframe

      if 'name' in data.columns and 'type' in data.columns:
        map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                          lookup_table=Collection, \
                                                                          lookup_column_name=['name', 'type'], \
                                                                          target_column_name='collection_id')     # prepare the function
        new_data=data.apply(map_function, axis=1)                                                                 # map foreign key ids
        data=new_data                                                                                             # overwrite data                 

      self.store_attributes(attribute_table=Collection_attribute, linked_column='collection_id', db_id=collection_id, data=data) # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  
  def fetch_collection_records_name_and_type(self, collection_name, collection_type, target_column_name=['name','type']):
    '''
    A method for fetching data for Collection table
    required params:
    collection_name: a collection name value
    collection_type: a collection type value
    target_column_name: a list of columns, default is ['name','type']
    '''
    try:
      column_list=[column for column in Collection.__table__.columns \
                       if column.key in target_column_name]
      column_data=dict(zip(column_list,[collection_name, collection_type]))
      collection=self.fetch_records_by_multiple_column(table=Collection, column_data=column_data, output_mode='one')
      return collection  
    except:
      raise


  def create_collection_group(self, data, autosave=True, required_collection_column=['name','type'],required_file_column='file_path'):
    '''
    A function for creating collection group, a link between a file and a collection
    [{'name':'a collection name', 'type':'a collection type', 'file_path': 'path'},]
    '''

    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    required_columns=required_collection_column
    required_columns.append(required_file_column)

    if not set((required_columns)).issubset(set(tuple(data.columns))):                                            # check for required parameters
      raise ValueError('Missing required value in input data {0}, required {1}'.format(tuple(data.columns), required_columns))    

    try:
      collection_map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                          lookup_table=Collection, \
                                                                          lookup_column_name=['name', 'type'], \
                                                                          target_column_name='collection_id')     # prepare the function
      new_data=data.apply(collection_map_function, axis=1)                                                        # map collection id
      file_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                                 data=x, \
                                                 lookup_table=File, \
                                                 lookup_column_name=required_file_column, \
                                                 target_column_name='file_id')                   # prepare the function for file id
      new_data=new_data.apply(file_map_function, axis=1)                                         # map collection id
      self.store_records(table=Collection_group, data=new_data.astype(str), mode='serial')       # storing data after converting it to string
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def get_collection_files(self, collection_name, collection_type='', output_mode='dataframe'):
    '''
    A method for fetching information from Collection, File, Collection_group tables
    required params:
    collection_name: a collection name to fetch the linked files
    optional params:
    collection_type: a collection type 
    output_mode: dataframe / object
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    session=self.session
    query=session.query(Collection, File).join(Collection_group).join(File)  # sql join Collection, Collection_group and File tables
    query=query.filter(Collection.name.in_([collection_name]))               # filter query based on collection_name
    if collection_type: 
      query=query.filter(Collection.type.in_([collection_type]))             # filter query on collection_type, if its present
   
    try:    
       results=self.fetch_records(query=query, output_mode=output_mode)      # get results
       return results
    except:
       raise


