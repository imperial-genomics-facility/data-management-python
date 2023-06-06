import pandas as pd
from igf_data.igfdb.dbconnect import DBConnect

class BaseAdaptor(DBConnect):
  '''
  The base adaptor class
  '''

  def __init__(self,**data):
    data.setdefault('session_class','')
    data.setdefault('session', '')

    if 'session_class' in data and data['session_class']:
      self.session_class = data['session_class']
    elif 'session' in data and data['session']:
      self.session = data['session'] 
    else:
      DBConnect.__init__(self,**data)


  def _store_record_serial(self,table,data):
    '''
    An internal method for storing dataframe records in serial mode

    :param table: A table class
    :param data: A list of data for upload to the table
    :returns: None
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      if not isinstance(data,pd.Series):
        raise ValueError(
                'Expecting a Pandas dataframe and recieved data type: {0}'.\
                format(type(data)))

      session = self.session
      data = data.fillna('')
      data_frame_dict = data.to_dict()
      data_frame_dict = \
        {key:value
          for key,value in data_frame_dict.items()
            if value}                                                           # filter any key with empty value
      mapped_object = table(**data_frame_dict)                                  # map dictionary to table class
      session.add(mapped_object)                                                # add data to session
    except Exception as e:
      raise ValueError(
              'Failed to store record, error: {0}'.\
              format(e))


  def _store_record_bulk(self,table,data):
    '''
    An internal method for storing dataframe records in bulk mode

    :param table: A table class
    :param data: A list of data for upload to the table
    :returns: None
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      if not isinstance(data,dict):
        data = data.to_dict(orient='records')
      else:
        raise ValueError(
                'Expecting a dictionary and recieved data type: {0}'.\
                format(type(data)))

      session = self.session
      session.bulk_insert_mappings(table,data)
    except Exception as e:
      raise ValueError(
              'Failed to store bulk record, error: {0}'.format(e))


  def _format_attribute_table_row(self,data,required_column,attribute_name_column,
                                  attribute_value_column ):
    '''
    An internal function for converting attribute table dataframe

    :param data: a attribute dataframe or dictionary
    :param required_column: column to add to the attribute table, it must be part of the data series
    :param attribute_name_column: column label for attribute name
    :param attribute_value_column: column label for attribute value
    :returns: A pandas series
    '''
    try:
      if not isinstance(data,pd.DataFrame):
        data = pd.DataFrame(data)

      final_list = list()
      data_dict = data.to_dict(orient='records')
      for element in data_dict:
        row_list = list()
        id_name = ''
        id_value = ''
        id_list = dict()
        for key, value in element.items():
          row_dict = dict()
          if isinstance(required_column,str):
            if value and key != required_column:
              row_dict[attribute_name_column] = key
              row_dict[attribute_value_column] = value
              row_list.append(row_dict)
            elif value and key == required_column:
              id_name = key
              id_value = value
          elif isinstance(required_column,list):
            if value and key not in required_column:
              row_dict[attribute_name_column] = key
              row_dict[attribute_value_column] = value
              row_list.append(row_dict)
            elif value and key in required_column:
              id_name = key
              id_value = value
              id_list[key] = value
          else:
            raise TypeError(
                    'Expecting a string or list and got: {0}'.\
                    format(type(required_column)))
        row_df = pd.DataFrame(row_list)
        if not id_name and id_value:
          raise ValueError(
                  'Required id or value not found for column: {0}'.\
                  format(required_column))

        if isinstance(required_column,str):
          row_df[id_name] = id_value
        elif isinstance(required_column,list):
          for key,value in id_list.items():
            row_df[key] = value

        row_df_data = row_df.to_dict(orient='records')
        final_list.extend(row_df_data)

      new_data_series = pd.DataFrame(final_list)
      new_data_series = new_data_series.dropna()
      return new_data_series
    except Exception as e:
      raise ValueError(
              'Failed to format attribute table row, error: {0}'.\
              format(e))


  def divide_data_to_table_and_attribute(self,data,required_column,table_columns,
                                         attribute_name_column='attribute_name',
                                         attribute_value_column='attribute_value'):
    '''
    A method for separating data for main and attribute tables

    :param data: a dictionary or dataframe containing the data
    :param required_column: column to add to the attribute table, it must be part of the data
    :param table_columns: required columns for the main table
    :param attribute_name_column: column label for attribute name
    :param attribute_value_column: column label for attribute value
    :returns: Two pandas dataframes, one for main table and one for attribute tables 
    '''
    try:
      if not isinstance(data,pd.DataFrame):
        data = pd.DataFrame(data)
      table_df = \
        data.\
          loc[:,data.columns.intersection(table_columns)]                       # slice df for table
      table_attr_columns = \
        list(
          set(data.columns).\
          difference(set(table_df.columns)))                                    # assign remaining columns to attribute dataframe
      if isinstance(required_column,str):
        table_attr_columns.\
          append(required_column)                                               # append required column if its a string
      elif isinstance(required_column,list):
        table_attr_columns.\
          extend(required_column)                                               # extend required column for a list
      else:
        raise TypeError(
                'Expecting a string or list and got: {0}'.\
                format(type(required_column)))

      table_attr_df = \
        data.\
          loc[:,data.columns.intersection(table_attr_columns)]                  # slice df for attribute table
      new_table_attr_df = \
        self._format_attribute_table_row(
          data=table_attr_df,
          required_column=required_column,
          attribute_name_column=attribute_name_column,
          attribute_value_column=attribute_value_column)
      return (table_df,new_table_attr_df)
    except Exception as e:
      raise ValueError(
              'Failed to divide data between data and attribute table, error: {0}'.\
                format(e))


  def map_foreign_table_and_store_attribute(self,data,lookup_table,lookup_column_name,
                                            target_column_name):
    '''
    A method for mapping foreign key id to the new column

    :param data: a data dictionary or pandas series, to be stored in attribute table
    :param lookup_table: a table class to look for the foreign key id
    :param lookup_column_name: a string or a list of column names which will be used 
                        to link the data frame with lookup_table,
                        this column will be removed from the output series
    :param target_column_name: column name for the foreign key id
    :returns: A data series
    '''
    try:
      if not isinstance(data,pd.Series):
        raise ValueError('Expecting a pandas data series for mapping foreign key id')

      if isinstance(lookup_column_name,list):
        lookup_values = list()
        lookup_columns = list()
        for lookup_column_key in lookup_column_name:
          value = data.get(lookup_column_key)
          lookup_values.append(value)
          column = \
            [column
              for column in lookup_table.__table__.columns
                if column.key == lookup_column_key][0]
          lookup_columns.\
            append(column)
          #del data[lookup_column_key]

        lookup_data = \
          dict(zip(lookup_columns,lookup_values))
        target_object = \
          self.fetch_records_by_multiple_column(
            table=lookup_table,
            column_data=lookup_data,
            output_mode='one')
      elif isinstance(lookup_column_name,str):
        lookup_value = \
          data.get(lookup_column_name)
        lookup_column = \
          [column
            for column in lookup_table.__table__.columns
              if column.key == lookup_column_name][0]
        target_object = \
          self.fetch_records_by_column(
            table=lookup_table,
            column_name=lookup_column,
            column_id=lookup_value,
            output_mode='one')
        #del data[lookup_column_name]
      else:
        raise TypeError(
                'Expecting a list or a string and found :{}'.\
                format(type(lookup_column_name)))

      target_value = \
        [getattr(target_object,column.key)
          for column in lookup_table.__table__.columns
            if column.key == target_column_name][0]                             # get target value from the target_object
      data[target_column_name] = target_value                                   # set value for target column
      #data = data.to_dict()
      #data = pd.Series(data)
      return data
    except Exception as e:
        raise ValueError(
                'Failed to map foreign table and store attribute, error: {0}'\
                  .format(e))


  def store_records(self,table,data,mode='serial'):
    '''
    A method for loading data to table

    :param table: name of the table class
    :param data : pandas dataframe or a list of dictionary
    :param mode : serial / bulk
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      if mode not in ('serial','bulk'):
        raise ValueError('Mode {0} is not recognised'.format(mode))

      if not isinstance(data,pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert dictionary to dataframe

      session = self.session
      if mode == 'serial':
        data.\
          apply(
            lambda x: \
              self._store_record_serial(
                table=table,
                data=x),
              axis=1)                                                           # load data in serial mode
      elif mode == 'bulk':
        self._store_record_bulk(
          table=table,data=data)                                                # load data in bulk mode
      session.flush()
    except Exception as e:
        session.rollback()
        raise ValueError(
                'Failed to store records, error: {0}'.format(e))


  def store_attributes(self,attribute_table,data,linked_column='',db_id='',
                       mode='serial'):
    '''
    A method for storing attributes

    :param attribute_table: a attribute table name
    :param linked_column: a column name to link the db_id to attribute table
    :param db_id: a db_id to link the attribute records
    :param mode: serial / bulk
    '''
    try:
      if isinstance(data,dict):
        data = pd.DataFrame(data)                                               # converting to dataframe

      if linked_column and db_id: 
        data[linked_column] = db_id                                             # adding or reseting db_id value for the linked column

      self.store_records(
        table=attribute_table,
        data=data,mode=mode)                                                    # storing data to attribute table
    except Exception as e:
      raise ValueError(
              'Failed to store attributes, error: {0}'.format(e))


  def _fetch_records_as_dataframe(self,query):
    '''
    An internal method for fetching database records as dataframe
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      session = self.session
      result = pd.read_sql(query.statement,session.bind)
      return result                                                             # return a dataframe
    except Exception as e:
      raise ValueError(
              'Failed to fetch records as dataframe, error: {0}'.\
                format(e))


  def _fetch_records_as_object(self,query):
    '''
    An internal method for fetching database records as query object
    '''
    try:
      for row in query:
        yield row                                                               # return a generator object
    except Exception as e:
      raise ValueError(
              'Failed to fetch records as object, error; {0}'.format(e))


  def _fetch_records_as_one(self,query):
    '''
    An internal method for fetching unique database record
    '''
    try:
      result = query.one()
      return result
    except Exception as e:
      raise ValueError(
              'Failed to fetch a single record, error: {0}'.format(e))


  def _fetch_records_as_one_or_none(self,query):
    '''
    An internal function for fetching record using one_or_none() method
    '''
    try:
      result = query.one_or_none()
      return result
    except Exception as e:
      raise ValueError(
              'Failed to fetch record as one or none, error: {0}'.\
                format(e))


  def _construct_query(self,table,filter_criteria):
    '''
    An internal method for query construction
    It doesn't support any join statement
    
    :param table: table name class
    :param filter_criteria: a list of criteria, each filter statement has three values, column_name, operator and value
    :returns: a session.query object
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      if not isinstance(filter_criteria,tuple) and \
         not isinstance(filter_criteria,list):
        raise ValueError('Expecting a list of filter_criteria, received data type: {0}'.\
                         format(type(filter_criteria)))

      session = self.session
      query = session.query(table)
      for filter_statement in filter_criteria:
        if len(filter_statement) != 3:
          raise ValueError(
                  'Expecting three parameters for filter criteria, got {0}'.\
                  format(len(filter_statement)))

        if filter_statement[1] == '==':
          query = \
            query.\
              filter(filter_statement[0] == filter_statement[2])
      return query
    except Exception as e:
      raise ValueError(
              'Failed to construct query, error: {0}'.format(e))


  def fetch_records(self,query,output_mode='dataframe'):
    '''
    A method for fetching records using a query

    :param query: A sqlalchmeny query object
    :param output_mode: dataframe / object / one / one_or_none
    :returns: A pandas dataframe for dataframe mode and a generator object for object mode
    '''
    try:
      if output_mode not in ('dataframe','object','one','one_or_none'):
        raise ValueError(
                'Expecting output_mode as dataframe or object, no support for {0}'.\
                format(output_mode))

      result=''
      if output_mode == 'dataframe':
        result=self._fetch_records_as_dataframe(query=query)                    # result is a dataframe
      elif output_mode == 'object':
        result=self._fetch_records_as_object(query=query)                       # result is a generator object
      elif output_mode == 'one':
        result=self._fetch_records_as_one(query=query)                          # fetch record as a unique match
      elif output_mode == 'one_or_none':
        result=self._fetch_records_as_one_or_none(query=query)                  # fetch record as a unique match or None
      return result
    except Exception as e:
      raise ValueError(
              'Failed to fetch records, error; {0}'.format(e))


  def fetch_records_by_column(self,table,column_name,column_id,output_mode):
    '''
    A method for fetching record with the column

    :param table: table name
    :param column_name: a column name
    :param column_id: a column id value
    :param output_mode: dataframe / object / one / one_or_none
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      session = self.session
      query = \
        session.\
          query(table).\
          filter(column_name==column_id)
      result = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return result
    except Exception as e:
      raise ValueError(
              'Failed to fetch records by column, error: {0}'.format(e))


  def fetch_records_by_multiple_column(self,table,column_data,output_mode):
    '''
    A method for fetching record with the column

    :param table: table name
    :param column_dict: a dictionary of column_names: column_value
    :param output_mode: dataframe / object/ one / one_or_none
    '''
    try:
      if not hasattr(self,'session'):
        raise AttributeError('Attribute session not found')

      session = self.session
      query = session.query(table)
      for column_name, column_id in column_data.items():
        query = \
          query.\
            filter(column_name.in_([column_id]))
      result = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return result
    except Exception as e:
      raise ValueError(
              'Failed to fetch records by multiple columns, error: {0}'.\
                format(e))


  def get_attributes_by_dbid(self,attribute_table,linked_table,linked_column_name,
                             db_id):
    '''
    A method for fetching attribute records for a specific attribute table with a db_id linked as foreign key

    :param attribute_table: A attribute table object
    :param linked_table: A main table object
    :param linked_column_name: A table name to link main table
    :param db_id: A unique id to link main  table
    :returns a dataframe of records
    '''
    try:
      session = self.session
      linked_column = \
        [column
          for column in linked_table.__table__.columns
            if column.key == linked_column_name][0]
      attribute_linked_column = \
        [column
          for column in attribute_table.__table__.columns
            if column.key == linked_column_name][0]
      query = \
        session.\
          query(attribute_table).\
          join(linked_table).\
          filter(linked_column==attribute_linked_column).\
          filter(linked_column==db_id)
      result = self.fetch_records(query=query)
      return result
    except Exception as e:
      raise ValueError(
              'Failed to get attributes by dbid, error; {0}'.\
                format(e))


  def get_table_columns(self,table_name,excluded_columns):
    '''
    A method for fetching the columns for table table_name

    :param table_name: a table class name
    :param excluded_columns: a list of column names to exclude from output
    '''
    try:
      columns = \
        [column.key
          for column in table_name.__table__.columns
            if column.key not in excluded_columns]
      return columns
    except Exception as e:
      raise ValueError(
              'Failed to get table columns, error: {0}'.\
                format(e))

