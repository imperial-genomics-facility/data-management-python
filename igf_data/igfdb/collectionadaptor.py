import os
import stat
import pandas as pd
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.igfTables import Collection, File, Collection_group, Collection_attribute
from igf_data.utils.fileutils import check_file_path, remove_dir

class CollectionAdaptor(BaseAdaptor):
  '''
  An adaptor class for Collection, Collection_group and Collection_attribute tables
  '''

  def store_collection_and_attribute_data(self,data,autosave=True):
    '''
    A method for dividing and storing data to collection and attribute table

    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A toggle for saving changes to database, default True
    :returns: None
    '''
    try:
      (collection_data,collection_attr_data) = \
         self.divide_data_to_table_and_attribute(data=data)
      self.store_collection_data(data=collection_data)                          # store collection data
      if len(collection_attr_data.index) > 0:
        self.store_collection_attributes(
          data=collection_attr_data)                                            # store project attributes

      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store collection and attribute data, error: {0}'.\
              format(e))


  def divide_data_to_table_and_attribute(
        self,data,required_column=('name', 'type'),table_columns=None,
        attribute_name_column='attribute_name',attribute_value_column='attribute_value'):
    '''
    A method for separating data for Collection and Collection_attribute tables

    :param data: A list of dictionaries or a pandas dataframe
    :param table_columns: List of table column names, default None
    :param required_column: column name to add to the attribute data, default 'name', 'type'
    :param attribute_name_column: label for attribute name column, default attribute_name
    :param attribute_value_column: label for attribute value column, default attribute_value
    :returns: Two pandas dataframes, one for Collection and another for Collection_attribute table
    '''
    try:
      required_column = list(required_column)
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      collection_columns = \
        self.get_table_columns(
          table_name=Collection,
          excluded_columns=['collection_id'])                                   # get required columns for collection table
      (collection_df, collection_attr_df) = \
        BaseAdaptor.\
          divide_data_to_table_and_attribute(
            self,
            data=data,
            required_column=required_column,
            table_columns=collection_columns,
            attribute_name_column=attribute_name_column,
            attribute_value_column=attribute_value_column)
      return (collection_df, collection_attr_df)
    except Exception as e:
      raise ValueError(
              'Failed to divide data for attribute table, error: {0}'.\
              format(e))


  def store_collection_data(self,data,autosave=False):
    '''
    A method for loading data to Collection table

    :param data: A list of dictionary or a Pandas DataFrame
    :param autosave: A toggle for saving changes to database, default True
    :returns: None
    '''
    try:
      self.store_records(
        table=Collection,
        data=data)
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store collection data, error: {0}'.\
              format(e))


  def check_collection_attribute(
        self,collection_name,collection_type,attribute_name):
    '''
    A method for checking collection attribute records for an attribute_name

    :param collection_name: A collection name
    :param collection_type: A collection type
    :param attribute_name: A collection attribute name
    :returns: Boolean, True if record exists or False
    '''
    try:
      record_exists = False
      query = \
        self.session.\
          query(Collection).\
          join(Collection_attribute,
               Collection.collection_id==Collection_attribute.collection_id).\
          filter(Collection.name==collection_name).\
          filter(Collection.type==collection_type).\
          filter(Collection.collection_id==Collection_attribute.collection_id).\
          filter(Collection_attribute.attribute_name==attribute_name)
      records = \
        self.fetch_records(\
          query=query,
          output_mode='dataframe')                                              # attribute can present more than one time
      if len(records.index)>0:
        record_exists=True

      return record_exists
    except Exception as e:
      raise ValueError(
              'Failed to check collection attribute, error: {0}'.\
              format(e))


  def update_collection_attribute(
        self,collection_name,collection_type,
        attribute_name,attribute_value,autosave=True):
    '''
    A method for updating collection attribute

    :param collection_name: A collection name
    :param collection_type: A collection type
    :param attribute_name: A collection attribute name
    :param attribute_value: A collection attribute value
    :param autosave: A toggle for committing changes to db, default True
    '''
    try:
      data = [{
        'name':collection_name,
        'type':collection_type,
        'attribute_name':attribute_name,
        'attribute_value':attribute_value}]
      data = pd.DataFrame(data)
      map_function = \
        lambda x: \
          self.map_foreign_table_and_store_attribute(
            data=x,
            lookup_table=Collection,
            lookup_column_name=['name','type'],
            target_column_name='collection_id')                                 # prep function
      data['collection_id'] = ''
      data = \
        data.apply(
          map_function,
          axis=1,
          result_type=None)                                                     # add collection id
      data.drop(
        columns=['name','type'],
        axis=1,
        inplace=True)
      for entry in data.to_dict(orient='records'):
        if entry.get('collection_id') is None:
          raise ValueError('Collection id not found')

        self.session.\
          query(Collection_attribute).\
          filter(Collection_attribute.collection_id==entry.get('collection_id')).\
          filter(Collection_attribute.attribute_name==entry.get('attribute_name')).\
          update({'attribute_value':entry.get('attribute_value')})              # update collection value

      if autosave:
        self.commit_session()                                                   # commit changes
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to update collection attribute, error: {0}'.format(e))


  def create_or_update_collection_attributes(self,data,autosave=True):
    '''
    A method for creating or updating collection attribute table, if the collection exists

    :param data: A list of dictionaries, containing following entries
      * name
      * type
      * attribute_name
      * attribute_value

    :param autosave: A toggle for saving changes to database, default True
    :returns: None
    '''
    try:
      if not isinstance(data,list) or \
         len(data) == 0:
        raise ValueError('No data found for collection attribute update')

      for entry in data:
        collection_name = entry.get('name')
        collection_type = entry.get('type')
        attribute_name = entry.get('attribute_name')
        attribute_value = entry.get('attribute_value')
        if collection_name is None or \
           collection_type is None or \
           attribute_name is None or \
           attribute_value is None:
          raise ValueError(
                  'Required data not found for collection attribute updates: {0}'.\
                  format(entry))
        collection_exists = \
          self.check_collection_records_name_and_type(
            collection_name=collection_name,
            collection_type=collection_type)
        if not collection_exists:
          raise ValueError(
                  'No collection found for name: {0} and type: {1}'.\
                  format(collection_name,collection_type))

        collection_attribute_exists = \
          self.check_collection_attribute(
            collection_name=collection_name,
            collection_type=collection_type,
            attribute_name=attribute_name)
        if collection_attribute_exists:
          self.update_collection_attribute(
            collection_name=collection_name,
            collection_type=collection_type,
            attribute_name=attribute_name,
            attribute_value=attribute_value,
            autosave=False)
        else:
          attribute_data = [{
            'name':collection_name,
            'type':collection_type,
            'attribute_name':attribute_name,
            'attribute_value':attribute_value}]
          self.store_collection_attributes(
            data=attribute_data,
            autosave=False)
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to create or update collection attribute, error: {0}'.\
              format(e))


  @staticmethod
  def prepare_data_for_collection_attribute(
        collection_name,collection_type,data_list):
    '''
    A static method for building data structure for collection attribute table update

    :param collection_name: A collection name
    :param collection_type: A collection type
    :param data: A list of dictionaries containing the data for attribute table
    :returns: A new list of dictionary for the collection attribute table
    '''
    try:
      if not isinstance(data_list,list) or \
         len(data_list)==0 or \
         not isinstance(data_list[0],dict):
        raise ValueError('No data found for attribute table')

      attribute_data_list = [{
        'name':collection_name,
        'type':collection_type,
        'attribute_name':key,
        'attribute_value':val}
          for item in data_list
            for key,val in item.items()]
      return attribute_data_list
    except Exception as e:
      raise ValueError(
              'Failed to prep data for collection, error: {0}'.\
              format(e))


  def store_collection_attributes(self,data,collection_id='',autosave=False):
    '''
    A method for storing data to Collectionm_attribute table

    :param data: A list of dictionary or a Pandas DataFrame
    :param collection_id: A collection id, optional
    :param autosave: A toggle for saving changes to database, default False
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert data to dataframe

      if 'name' in data.columns and 'type' in data.columns:
        map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Collection,
              lookup_column_name=['name', 'type'],
              target_column_name='collection_id')                               # prepare the function
        data['collection_id'] = ''
        data = \
          data.apply(
            map_function,
            axis=1,
            result_type=None)                                                   # map foreign key ids
        data.drop(
          columns=['name', 'type'],
          axis=1,
          inplace=True)
        #data = new_data                                                        # overwrite data

      self.store_attributes(
        attribute_table=Collection_attribute,
        linked_column='collection_id',
        db_id=collection_id, data=data)                                         # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store collection attribute, error: {0}'.\
              format(e))


  def check_collection_records_name_and_type(self,collection_name,collection_type):
    '''
    A method for checking existing data for Collection table
    
    :param collection_name: a collection name value
    :param collection_type: a collection type value
    :returns: True if the file is present in db or False if its not
    '''
    try:
      collection_check=False
      query = \
        self.session.\
          query(Collection).\
          filter(Collection.name==collection_name).\
          filter(Collection.type==collection_type)
      collection_obj = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if collection_obj is not None:
        collection_check=True
      return collection_check
    except Exception as e:
      raise ValueError(
              'Failed to check collection name and type, error: {0}'.\
              format(e))


  def fetch_collection_records_name_and_type(
        self, collection_name,collection_type,target_column_name=('name','type')):
    '''
    A method for fetching data for Collection table

    :param collection_name: a collection name value
    :param collection_type: a collection type value
    :param target_column_name: a list of columns, default is ['name','type']
    :returns: Collection record
    '''
    try:
      target_column_name = list(target_column_name)
      column_list = [
        column
          for column in Collection.__table__.columns \
            if column.key in target_column_name]
      column_data = \
        dict(zip(column_list,[collection_name, collection_type]))
      collection = \
        self.fetch_records_by_multiple_column(
          table=Collection,
          column_data=column_data,
          output_mode='one')
      return collection
    except Exception as e:
      raise ValueError(
              'Failed to fetch collection name and type, error: {0}'.\
              format(e))


  def load_file_and_create_collection(
        self,data,autosave=True, hasher='md5',
        calculate_file_size_and_md5=True,
        required_coumns=(
          'name','type','table',
          'file_path','size','md5','location')):
    '''
    A function for loading files to db and creating collections

    :param data: A list of dictionary or a Pandas dataframe
    :param autosave: Save data to db, default True
    :param required_coumns: List of required columns
    :param hasher: Method for file checksum, default md5
    :param calculate_file_size_and_md5: Enable file size and md5 check, default True
    :returns: None
    '''
    try:
      required_coumns = list(required_coumns)
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      data.fillna('',inplace=True)                                              # replace missing value
      if not set(data.columns).issubset(set(required_coumns)):
        raise ValueError(
                'missing required columns: {0}, found columns:{1}'.\
                  format(required_coumns,data.columns))
      for r in required_coumns:                                                 # adding the missing required columns
        if r not in data.columns:
          data[r] = ''
      if calculate_file_size_and_md5:                                           # adding md5 and size
        data['md5'] = \
          data['file_path'].\
            map(
              lambda x: \
                calculate_file_checksum(
                  filepath=x,
                  hasher=hasher))                                               # calculate file checksum
        data['size'] = \
          data['file_path'].\
            map(lambda x: os.path.getsize(x))                                   # calculate file size

      file_columns = ['file_path','md5','size','location']
      file_data = \
        data.loc[:,data.columns.intersection(file_columns)]
      file_data = file_data.drop_duplicates()
      collection_columns = ['name','type','table']
      collection_data = \
        data.loc[:,data.columns.intersection(collection_columns)]
      collection_data = collection_data.drop_duplicates()
      file_group_column = ['name','type','file_path']
      file_group_data = \
        data.loc[:,data.columns.intersection(file_group_column)]
      file_group_data = file_group_data.drop_duplicates()
      fa = FileAdaptor(**{'session':self.session})
      fa.store_file_and_attribute_data(
        data=file_data,autosave=False)                                          # store file data
      self.session.flush()
      collection_data['data_exists'] = ''
      collection_data = \
        collection_data.apply(
          lambda x: \
            self._tag_existing_collection_data(
              data=x,
              tag='EXISTS',
              tag_column='data_exists'),
            axis=1,
            result_type=None)                                                   # tag existing collections
      collection_data = \
        collection_data[collection_data['data_exists']!='EXISTS']               # filter existing collections
      if len(collection_data.index) > 0:
        self.store_collection_and_attribute_data(
          data=collection_data,
          autosave=False)                                                       # store new collection if any entry present
        self.session.flush()

      self.create_collection_group(
        data=file_group_data,
        autosave=False)                                                         # store collection group info
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to load file and create collection, error: {0}'.\
              format(e))


  def _tag_existing_collection_data(self,data,tag='EXISTS',tag_column='data_exists'):
    '''
    An internal method for checking a dataframe for existing collection record

    :param data: A Pandas data series or a dictionary with following keys

                        * name
                        * type

    :param tag: A text tag for marking existing collections, default EXISTS
    :param tag_column: A column name for adding the tag, default data_exists
    :returns: A pandas series
    '''
    try:
      if not isinstance(data, pd.Series):
        data = pd.Series(data)

      if 'name' not in data or \
         'type' not in data:
        raise ValueError(
                'Required collection column name or type not found in data: {0}'.\
                format(data.to_dict()))

      data[tag_column] = ''
      collection_exists = \
        self.check_collection_records_name_and_type(
          collection_name=data['name'],
          collection_type=data['type'])
      if collection_exists:
        data[tag_column] = tag

      return data
    except Exception as e:
      raise ValueError(
              'Failed to tag existing collections, error: {0}'.\
              format(e))


  def fetch_collection_name_and_table_from_file_path(self,file_path):
    '''
    A method for fetching collection name and collection_table info using the
    file_path information. It will return None if the file doesn't have any
    collection present in the database

    :param file_path: A filepath info
    :returns: Collection name and collection table for first collection group
    '''
    try:
      collection_name = None
      collection_table = None
      session = self.session
      query = \
        session.\
          query(Collection, File).\
          join(Collection_group,
               Collection.collection_id==Collection_group.collection_id).\
          join(File,
               File.file_id==Collection_group.file_id).\
          filter(File.file_path==file_path)
      results = \
        self.fetch_records(
          query=query,
          output_mode='dataframe')                                              # get results
      results = \
        results.to_dict(orient='records')
      if len(results)>0:
        collection_name = results[0]['name']
        collection_table = results[0]['table']
        return collection_name, collection_table
      else:
        raise  ValueError('No collection found for file: {0}'.\
                          format(len(results)))
    except Exception as e:
      raise ValueError(
              'Failed to fectch collection from table path, error: {0}'.\
              format(e))


  def create_collection_group(
        self, data, autosave=True,required_collection_column=('name','type'),
        required_file_column='file_path'):
    '''
    A function for creating collection group, a link between a file and a collection

    :param data: A list dictionary or a Pandas DataFrame with following columns

                           * name
                           * type
                           * file_path

                 E.g. [{'name':'a collection name', 'type':'a collection type', 'file_path': 'path'},]
    :param required_collection_column: List of required column for fetching collection,
                                       default 'name','type'
    :param required_file_column: Required column for fetching file information,
                                 default file_path
    :param autosave: A toggle for saving changes to database, default True
    :returns: None
    '''
    try:
      if isinstance(required_collection_column,tuple):
        required_collection_column = \
          list(required_collection_column)
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      required_columns = list()
      required_columns.\
        extend(required_collection_column)
      required_columns.\
        append(required_file_column)

      if not set((required_columns)).issubset(set(tuple(data.columns))):        # check for required parameters
        raise ValueError(
                'Missing required value in input data {0}, required {1}'.\
                format(tuple(data.columns), required_columns))    

      data.to_dict(orient='records')
      collection_map_function = \
        lambda x: \
          self.map_foreign_table_and_store_attribute(
            data=x,
            lookup_table=Collection,
            lookup_column_name=required_collection_column,
            target_column_name='collection_id')                                 # prepare the function
      data['collection_id'] = ''
      data = \
        data.apply(
          collection_map_function,
          axis=1,
          result_type=None)                                                     # map collection id
      data.drop(
        columns=required_collection_column,
        axis=1,
        inplace=True)
      file_map_function = \
        lambda x: \
          self.map_foreign_table_and_store_attribute(
            data=x,
            lookup_table=File,
            lookup_column_name=required_file_column,
            target_column_name='file_id')                                       # prepare the function for file id
      data['file_id'] = ''
      data = \
        data.apply(
          file_map_function,
          axis=1,
          result_type=None)                                                     # map collection id
      data.drop(
        required_file_column,
        axis=1,
        inplace=True)
      self.store_records(
        table=Collection_group,
        data=data.astype(str),
        mode='serial')                                                          # storing data after converting it to string
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to create collection group, error: {0}'.format(e))


  def get_collection_files(
        self, collection_name, collection_type='',
        collection_table='',output_mode='dataframe'):
    '''
    A method for fetching information from Collection, File, Collection_group tables

    :param collection_name: A collection name to fetch the linked files
    :param collection_type: A collection type
    :param collection_table: A collection table
    :param output_mode: dataframe / object
    '''
    try:
      if not hasattr(self, 'session'):
        raise AttributeError('Attribute session not found')

      query = \
        self.session.\
          query(Collection, File).\
          join(Collection_group,
               Collection.collection_id==Collection_group.collection_id).\
          join(File,
               File.file_id==Collection_group.file_id)                          # sql join Collection, Collection_group and File tables
      query = \
        query.\
          filter(Collection.name.in_([collection_name]))                        # filter query based on collection_name
      if collection_type:
        query = \
          query.\
            filter(Collection.type.in_([collection_type]))                      # filter query on collection_type, if its present

      if collection_table !='':
        query = \
          query.\
            filter(Collection.table==collection_table)

      results = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)                                              # get results
      return results
    except Exception as e:
       raise ValueError(\
                'Failed to fetch collection files, error: {0}'.format(e))


  def _check_and_remove_collection_group(
        self,data,autosave=True,collection_name_col='name',
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
        data = pd.Series(data)

      if collection_name_col not in data or \
         collection_type_col not in data or \
         file_path_col not in data:
        raise ValueError(
                'Missing required fields for checking existing collection group, {0}'.\
                format(data.to_dict(orient='records')))

      collection_files = \
        self.get_collection_files(
          collection_name=data[collection_name_col],
          collection_type=data[collection_type_col],
          output_mode='dataframe')                                              # fetch collection files info from db

      if data.file_path != '':
        collection_files = \
          collection_files[collection_files[file_path_col]==data[file_path_col]] # filter collection group files

      if len(collection_files.index)>0:
        for row in collection_files.to_dict(orient='records'):
          collection_id = row[collection_id_col]
          file_id = row[file_id_col]

          self.session.\
            query(Collection_group).\
            filter(Collection_group.collection_id==collection_id).\
            filter(Collection_group.file_id==file_id).\
            delete(synchronize_session=False)                                   # remove records from db

      if autosave:
        self.commit_session()                                                   # save changes to db
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to check and remove collection group, error: {0}'.format(e))


  def remove_collection_group_info(
        self,data,autosave=True,required_collection_column=('name','type'),
        required_file_column='file_path'):
    '''
    A method for removing collection group information from database

    :param data: A list dictionary or a Pandas DataFrame with following columns

      * name
      * type
      * file_path

      File_path information is not mandatory
    :param required_collection_column: List of required column for fetching collection,
                                       default 'name','type'
    :param required_file_column: Required column for fetching file information,
                                 default file_path
    :param autosave: A toggle for saving changes to database, default True
    '''
    try:
      required_collection_column = \
        list(required_collection_column)
      if not isinstance(data,pd.DataFrame):
        data = pd.DataFrame(data)

      required_columns = required_collection_column
      required_columns.\
        append(required_file_column)

      if required_file_column not in data.columns:
        data[required_file_column] = ''                                         # add an empty file_path column if its not present

      if not set((required_columns)).issubset(set(tuple(data.columns))):        # check for required parameters
        raise ValueError(
                'Missing required value in input data {0}, required {1}'.\
                format(tuple(data.columns), required_columns))

      data.apply(
        lambda x: \
          self._check_and_remove_collection_group(
            data=x,
            autosave=autosave),
          axis=1)                                                               # check and remove collection group data
    except Exception as e:
      raise ValueError(
              'Failed to remove collection group info, error; {0}'.format(e))


  def cleanup_collection_and_file_for_name_and_type(
        self,
        collection_name: str,
        collection_type: str,
        autosave: bool = True,
        remove_files_on_disk: bool = False,
        skip_dirs: bool = True) -> None:
    try:
      query = \
        self.session.\
          query(Collection.name, Collection.type, File.file_path).\
          join(Collection_group, Collection.collection_id==Collection_group.collection_id).\
          join(File, File.file_id==Collection_group.file_id).\
          filter(Collection.name==collection_name).\
          filter(Collection.type==collection_type)
      records = \
        self.fetch_records(
          query=query,
          output_mode='dataframe')
      if len(records.index) > 0:
        collection_records = \
          records[['name', 'type']].drop_duplicates().to_dict(orient='records')
        file_records = \
          records['file_path'].drop_duplicates().values.tolist()
        ## clean files from db
        try:
          ## file clean up
          self.session.\
            query(File).\
            filter(File.file_path.in_(file_records)).\
            delete(synchronize_session=False)
          ## collection clean up
          for collection_entry in collection_records:
            self.session.\
              query(Collection).\
              filter(Collection.name==collection_entry.get('name')).\
              filter(Collection.type==collection_entry.get('type')).\
              delete(synchronize_session=False)
          if autosave:
            self.commit_session()
        except:
          if autosave:
            self.rollback_session()
          raise
        ## clean files from disk
        if remove_files_on_disk:
          for file_path in file_records:
            check_file_path(file_path)
            ## enable read, write and execute for user
            os.chmod(
              file_path,
              stat.S_IRUSR |
              stat.S_IWUSR)
            os.remove(file_path)
            if os.path.isdir(file_path) and \
               not skip_dirs:
              for root, dirs, files in os.walk(file_path):
                for dir_name in dirs:
                  dir_path = \
                    os.path.join(root, dir_name)
                  ## enable read, write and execute for user
                  os.chmod(
                    dir_path,
                    stat.S_IRUSR |
                    stat.S_IWUSR |
                    stat.S_IXUSR)
                for file_name in files:
                  path = \
                    os.path.join(root, file_name)
                  ## enable read and write for user
                  os.chmod(
                    path,
                    stat.S_IRUSR |
                    stat.S_IWUSR)
              remove_dir(file_path)
    except Exception as e:
      raise ValueError(
        f'Failed to cleanup collection and file for name and type, error: {e}')

if __name__=='__main__':
  from igf_data.igfdb.igfTables import Base
  from igf_data.utils.fileutils import get_temp_dir
  from igf_data.utils.fileutils import remove_dir


  dbparams = {'dbname':'collection_test.sqlite','driver':'sqlite'}
  dbname = dbparams['dbname']
  if os.path.exists(dbname):
    os.remove(dbname)

  temp_dir=get_temp_dir()
  base=BaseAdaptor(**dbparams)
  Base.metadata.create_all(base.engine)
  base.start_session()
  collection_data=[{ 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                   },
                   { 'name':'IGF002_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment'
                   }]

  ca=CollectionAdaptor(**{'session':base.session})
  c_data,ca_data = ca.divide_data_to_table_and_attribute(collection_data)
  print(c_data.to_dict(orient='records'))
  print(ca_data.to_dict(orient='records'))

  ca.store_collection_and_attribute_data(data=collection_data,
                                         autosave=True)
  """
  base.close_session()
  base.start_session()
  ca=CollectionAdaptor(**{'session':base.session})
  collection_exists=ca.fetch_collection_records_name_and_type(collection_name='IGF001_MISEQ',
                                                              collection_type='ALIGNMENT_CRAM')
  print(collection_exists)
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
  collection_data=pd.DataFrame(collection_data)
  collection_data=collection_data.apply(lambda x: \
                                            ca._tag_existing_collection_data(\
                                              data=x,\
                                              tag='EXISTS',\
                                              tag_column='data_exists'),
                                            axis=1)                             # tag existing collections
  collection_data=collection_data[collection_data['data_exists']!='EXISTS']
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
  """
  base.close_session()
  remove_dir(temp_dir)
  if os.path.exists(dbname):
    os.remove(dbname)