import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.useradaptor import FileAdaptor
from igf_data.igfdb.igfTables import Collection, File, Collection_group, Collection_attribute, Experiment, Run, Sample

class ProjectAdaptor(BaseAdaptor):
  '''
  An adaptor class for Collection, Collection_group and Collection_attribute tables
  '''

  def store_collection_and_attribute_data(self, data):
    '''
    A method for dividing and storing data to collection and attribute table
    '''
    (collection_data, collection_attr_data)=self.divide_data_to_table_and_attribute(data=data)
    
    try:
      self.store_collection_data(data=collection_data)                                                        # store collection data
      map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                        lookup_table=Collection, \
                                                                        lookup_column_name=['name', 'type'] \ 
                                                                        target_column_name='collection_id')   # prepare the function
      new_collection_attr_data=collection_attr_data.apply(map_function, axis=1)                               # map foreign key id
      self.store_collection_attributes(data=new_project_attr_data)                                            # store project attributes 
      self.commit_session()                                                                                   # save changes to database
    except:
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

 
   def store_collection_data(self, data):
    '''
    Load data to Collection table
    '''
    try:
      self.store_records(table=Collection, data=data)
    except:
      raise


  def store_collection_attributes(self, data, collection_id=''):
    '''
    A method for storing data to Collectionm_attribute table
    '''
    try:
      self.store_attributes(attribute_table=Collection_attribute, linked_column='collection_id', db_id=collection_id, data=data)
    except:
      raise


