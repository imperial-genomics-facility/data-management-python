import os
import pandas as pd
from sqlalchemy.sql import column
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.igfTables import Collection, File, Collection_group, Collection_attribute

class CollectionAdaptor(BaseAdaptor):
  '''
  An adaptor class for Collection, Collection_group and Collection_attribute tables
  '''

  def store_collection_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to collection and attribute table
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A toggle for saving changes to database, default True
    '''
    try:
      (collection_data,
       collection_attr_data)=self.divide_data_to_table_and_attribute(data=data)
      self.store_collection_data(data=collection_data)                          # store collection data
      if len(collection_attr_data.index) > 0:
        self.store_collection_attributes(data=collection_attr_data)             # store project attributes

      if autosave:
        self.commit_session()                                                   # save changes to database
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column=['name', 'type'],
                                         attribute_name_column='attribute_name',
                                         attribute_value_column='attribute_value'):
    '''
    A method for separating data for Collection and Collection_attribute tables
    
    :param required_column: column name to add to the attribute data, default 'name', 'type'
    :param attribute_name_column: label for attribute name column, default attribute_name
    :param attribute_value_column: label for attribute value column, default attribute_value
    :returns: Two pandas dataframes, one for Collection and another for Collection_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      collection_columns=self.get_table_columns(table_name=Collection,
                                                excluded_columns=['collection_id']) # get required columns for collection table    
      (collection_df, collection_attr_df)=BaseAdaptor.\
                                          divide_data_to_table_and_attribute(\
                                            self,
                                            data=data, \
                                            required_column=required_column, \
                                            table_columns=collection_columns,  \
                                            attribute_name_column=attribute_name_column, \
                                            attribute_value_column=attribute_value_column
                                          )
      return (collection_df, collection_attr_df)
    except:
      raise


  def store_collection_data(self, data, autosave=False):
    '''
    A method for loading data to Collection table
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A toggle for saving changes to database, default True
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
    
    :param data: A list of dictionary or a Pandas DataFrame
    :param collection_id: A collection id, optional
    :param autosave: A toggle for saving changes to database, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                 # convert data to dataframe

      if 'name' in data.columns and 'type' in data.columns:
        map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                      data=x,
                                      lookup_table=Collection,
                                      lookup_column_name=['name', 'type'],
                                      target_column_name='collection_id')       # prepare the function
        new_data=data.apply(map_function, axis=1)                               # map foreign key ids
        data=new_data                                                           # overwrite data

      self.store_attributes(attribute_table=Collection_attribute,
                            linked_column='collection_id',
                            db_id=collection_id, data=data)                     # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def check_collection_records_name_and_type(self, collection_name, collection_type):
    '''
    A method for checking existing data for Collection table
    
    :param collection_name: a collection name value
    :param collection_type: a collection type value
    :returns: True if the file is present in db or False if its not
    '''
    try:
      collection_check=False
      query=self.session.\
            query(Collection).\
            filter(Collection.name==collection_name).\
            filter(Collection.type==collection_type)
      collection_obj=self.fetch_records(query=query, output_mode='one_or_none')
      if collection_obj is not None:
        collection_check=True
      return collection_check
    except:
      raise


  def fetch_collection_records_name_and_type(self, collection_name, collection_type, target_column_name=['name','type']):
    '''
    A method for fetching data for Collection table
    
    :param collection_name: a collection name value
    :param collection_type: a collection type value
    :param target_column_name: a list of columns, default is ['name','type']
    '''
    try:
      column_list=[column for column in Collection.__table__.columns \
                       if column.key in target_column_name]
      column_data=dict(zip(column_list,[collection_name, collection_type]))
      collection=self.fetch_records_by_multiple_column(table=Collection, column_data=column_data, output_mode='one')
      return collection  
    except:
      raise


  def load_file_and_create_collection(self,data,autosave=True, hasher='md5',
                                      calculate_file_size_and_md5=True,
                                      required_coumns=['name','type','table',
                                                       'file_path','location']):
    '''
    A function for loading files to db and creating collections
    
    :param data: A list of dictionary or a Pandas dataframe
    :param autosave: Save data to db, default True
    :param required_coumns: List of required columns
    :param hasher: Method for file checksum, default md5
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      if not set(data.columns).issubset(set(required_coumns)):
        raise ValueError('missing required columns: {0}'.\
                         format(data.columns))

      if calculate_file_size_and_md5:
        data['md5']=data['file_path'].\
                    map(lambda x: \
                    calculate_file_checksum(filepath=x,
                                            hasher=hasher))                     # calculate file checksum
        data['size']=data['file_path'].\
                     map(lambda x: os.path.getsize(x))                          # calculate file size

      file_columns=['file_path','md5','size','location']
      file_data=data.loc[:,file_columns]
      file_data=file_data.drop_duplicates()

      collection_columns=['name','type','table']
      collection_data=data.loc[:,collection_columns]
      collection_data=collection_data.drop_duplicates()

      file_group_column=['name','type','file_path']
      file_group_data=data.loc[:,file_group_column]
      file_group_data=file_group_data.drop_duplicates()

      fa=FileAdaptor(**{'session':self.session})
      fa.store_file_and_attribute_data(data=file_data,autosave=False)           # store file data
      self.session.flush()
      collection_data=collection_data.apply(lambda x: \
                                            self._tag_existing_collection_data(\
                                              data=x,\
                                              tag='EXISTS',\
                                              tag_column='data_exists'),
                                            axis=1)                             # tag existing collections
      collection_data=collection_data[collection_data['data_exists']!='EXISTS'] # filter existing collections
      if len(collection_data.index) > 0:
        self.store_collection_and_attribute_data(data=collection_data,\
                                                 autosave=False)                # store new collection if any entry present
        self.session.flush()

      self.create_collection_group(data=file_group_data,autosave=False)         # store collection group info
      if autosave:
        self.commit_session()
    except:
      raise


  def _tag_existing_collection_data(self,data,tag='EXISTS',tag_column='data_exists'):
    '''
    An internal method for checking a dataframe for existing collection record
    
    :param data: A Pandas data series or a dictionary with following keys
                        name
                        type
    :param tag: A text tag for marking existing collections, default EXISTS
    :param tag_column: A column name for adding the tag, default data_exists
    :returns: A pandas series
    '''
    try:
      if not isinstance(data, pd.Series):
        data=pd.Series(data)

      if 'name' not in data or \
         'type' not in data:
        raise ValueError('Required collection column name or type not found in data: {0}'.\
                         format(data.to_dict()))

      data[tag_column]=''
      collection_exists=self.check_collection_records_name_and_type(\
                               collection_name=data['name'],
                               collection_type=data['type'])
      if collection_exists:
        data[tag_column]=tag

      return data
    except:
      raise


  def fetch_collection_name_and_table_from_file_path(self,file_path):
    '''
    A method for fetching collection name and collection_table info using the
    file_path information. It will return None if the file doesn't have any 
    collection present in the database
    
    :param file_path: A filepath info
    :returns: Collection name and collection table for first collection group
    '''
    try:
      collection_name=None
      collection_table=None
      session=self.session
      query=session.query(Collection, File).\
                    join(Collection_group).\
                    join(File).\
                    filter(File.file_path==file_path)
      results=self.fetch_records(query=query, output_mode='dataframe')          # get results
      results=results.to_dict(orient='records')
      if len(results)>0:
        collection_name=results[0]['name']
        collection_table=results[0]['table']
        return collection_name, collection_table
      else:
        raise  ValueError('No collection found for file: {0}'.\
                          format(len(results)))
    except:      
      raise   


  def create_collection_group(self, data, autosave=True, required_collection_column=['name','type'],required_file_column='file_path'):
    '''
    A function for creating collection group, a link between a file and a collection
    
    :param data: A list dictionary or a Pandas DataFrame with following columns
                           name
                           type
                           file_path
                 E.g. [{'name':'a collection name', 'type':'a collection type', 'file_path': 'path'},]
    :param required_collection_column: List of required column for fetching collection,
                                       default 'name','type'
    :param required_file_column: Required column for fetching file information,
                                 default file_path
    :param autosave: A toggle for saving changes to database, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      required_columns=required_collection_column
      required_columns.append(required_file_column)

      if not set((required_columns)).issubset(set(tuple(data.columns))):        # check for required parameters
        raise ValueError('Missing required value in input data {0}, required {1}'.\
                         format(tuple(data.columns), required_columns))    

      collection_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                               data=x,
                                               lookup_table=Collection,
                                               lookup_column_name=['name', 'type'],
                                               target_column_name='collection_id') # prepare the function
      new_data=data.apply(collection_map_function,
                          axis=1)                                               # map collection id
      file_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                         data=x,
                                         lookup_table=File,
                                         lookup_column_name=required_file_column,
                                         target_column_name='file_id')          # prepare the function for file id
      new_data=new_data.apply(file_map_function,
                              axis=1)                                           # map collection id
      self.store_records(table=Collection_group,
                         data=new_data.astype(str),
                         mode='serial')                                         # storing data after converting it to string
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def get_collection_files(self, collection_name, collection_type='', output_mode='dataframe'):
    '''
    A method for fetching information from Collection, File, Collection_group tables
    
    :param collection_name: a collection name to fetch the linked files
    :param collection_type: a collection type 
    :param output_mode: dataframe / object
    '''
    try:
      if not hasattr(self, 'session'):
        raise AttributeError('Attribute session not found')

      query=self.session.\
            query(Collection, File).\
            join(Collection_group).\
            join(File)                                                          # sql join Collection, Collection_group and File tables
      query=query.\
            filter(Collection.name.in_([collection_name]))                      # filter query based on collection_name
      if collection_type: 
        query=query.\
              filter(Collection.type.in_([collection_type]))                    # filter query on collection_type, if its present

      results=self.fetch_records(query=query,
                                 output_mode=output_mode)                       # get results
      return results
    except:
       raise

  def _check_and_remove_collection_group(self,data,autosave=True,collection_name_col='name',
                                         collection_type_col='type',file_path_col='file_path',
                                         collection_id_col='collection_id',file_id_col='file_id'):
    '''
    An internal method for checking and removing collection group data
    
    :param data: A dictionary or a Pandas Series
    :param autosave: A toggle for saving changes to database, default True
    :param collection_name_col: Name of the collection name column, default name
    :param collection_type_col: Name of the collection_type column, default type
    :param file_path_col: Name of the file_path column, default file_path
    :param collection_id_col: Name of the collection_id column, default collection_id
    :param file_id_col: Name of the file_id column, default file_id
    '''
    try:
      if not isinstance(data, pd.Series):
        data=pd.Series(data)

      if collection_name_col not in data or \
         collection_type_col not in data or \
         file_path_col not in data:
        raise ValueError('Missing required fields for checking existing collection group'.\
                         format(data.to-dict()))

      collection_files=self.get_collection_files(collection_name=data[collection_name_col],
                                                 collection_type=data[collection_type_col],
                                                 output_mode='dataframe')       # fetch collection files info from db

      if data.file_path != '':
        collection_files=collection_files[collection_files[file_path_col]==data[file_path_col]] # filter collection group files

      if len(collection_files.index)>0:
        for row in collection_files.to_dict(orient='records'):
          collection_id=row[collection_id_col]
          file_id=row[file_id_col]

          self.session.\
          query(Collection_group).\
          filter(Collection_group.collection_id==collection_id).\
          filter(Collection_group.file_id==file_id).\
           delete(synchronize_session=False)                                    # remove records from db

      if autosave:
        self.commit_session()                                                   # save changes to db
    except:
      raise


  def remove_collection_group_info(self,data,autosave=True,
                                   required_collection_column=['name','type'],
                                   required_file_column='file_path'):
    '''
    A method for removing collection group information from database
    
    :param data: A list dictionary or a Pandas DataFrame with following columns
                           name
                           type
                           file_path
                 File_path information is not mandatory
    :param required_collection_column: List of required column for fetching collection,
                                       default 'name','type'
    :param required_file_column: Required column for fetching file information,
                                 default file_path
    :param autosave: A toggle for saving changes to database, default True
    '''
    try:
      if not isinstance(data,pd.DataFrame):
        data=pd.DataFrame(data)

      required_columns=required_collection_column
      required_columns.append(required_file_column)

      if required_file_column not in data.columns:
        data[required_file_column]=''                                           # add an empty file_path column if its not present

      if not set((required_columns)).issubset(set(tuple(data.columns))):        # check for required parameters
        raise ValueError('Missing required value in input data {0}, required {1}'.\
                         format(tuple(data.columns), required_columns))    

      data.apply(lambda x: \
                 self._check_and_remove_collection_group(data=x,
                                                         autosave=autosave),
                 axis=1)                                                        # check and remove collection group data
    except:
      raise

if __name__=='__main__':
  from sqlalchemy import create_engine
  from igf_data.igfdb.igfTables import Base
  from igf_data.utils.dbutils import read_dbconf_json
  from igf_data.utils.fileutils import get_temp_dir
  from igf_data.utils.fileutils import remove_dir

  dbparams = read_dbconf_json('data/dbconfig.json')
  dbname=dbparams['dbname']
  if os.path.exists(dbname):
    os.remove(dbname)

  temp_dir=get_temp_dir()
  base=BaseAdaptor(**dbparams)
  Base.metadata.create_all(base.engine)
  base.start_session()
  collection_data=[{ 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment'
                   },
                   { 'name':'IGF002_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment'
                   }]

  ca=CollectionAdaptor(**{'session':base.session})
  ca.store_collection_and_attribute_data(data=collection_data,
                                         autosave=True)
  base.close_session()
  base.start_session()
  ca=CollectionAdaptor(**{'session':base.session})
  collection_exists=ca.fetch_collection_records_name_and_type(collection_name='IGF001_MISEQ',
                                                              collection_type='ALIGNMENT_CRAM')
  #print(collection_exists)
  collection_data=[{ 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'a.cram',
                   },
                   { 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'a1.cram',
                   },
                   { 'name':'IGF003_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'b.cram',
                   }]
  #collection_data=pd.DataFrame(collection_data)
  #collection_data=collection_data.apply(lambda x: \
  #                                          ca._tag_existing_collection_data(\
  #                                            data=x,\
  #                                            tag='EXISTS',\
  #                                            tag_column='data_exists'),
  #                                          axis=1)                             # tag existing collections
  #collection_data=collection_data[collection_data['data_exists']!='EXISTS']
  ca.load_file_and_create_collection(data=collection_data,
                                     calculate_file_size_and_md5=False)
  remove_data_list=[{'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     }]
  ca.remove_collection_group_info(data=remove_data_list)
  cg_data=ca.get_collection_files(collection_name='IGF001_MISEQ',
                                  collection_type='ALIGNMENT_CRAM',
                                  output_mode='dataframe')
  print(cg_data.to_dict(orient='records'))
  #print([element.file_path
  #         for row in cg_data
  #          for element in row
  #            if isinstance(element, File)])
  base.close_session()
  remove_dir(temp_dir)
  if os.path.exists(dbname):
    os.remove(dbname)