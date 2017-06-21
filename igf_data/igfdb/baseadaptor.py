import json
import warnings
import pandas as pd
from igf_data.igfdb.dbconnect import DBConnect
 
class BaseAdaptor(DBConnect):
  '''
  The base adaptor class
  '''
  
  def __init__(self, **data):
    data.setdefault('session_class', '')
    data.setdefault('session', '')

    if 'session_class' in data and data['session_class']:
      self.session_class = data['session_class']
    elif 'session' in data and data['session']:
      self.session = data['session'] 
    else:
      super(BaseAdaptor, self).__init__(**data)

     
  def _store_record_serial(self, table, data):
    '''
    An internal method for storing dataframe records in serial mode
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
    
    if not isinstance(data, pd.Series):
      raise  ValueError('Expecting a Pandas dataframe and recieved data type: {0}'.format(type(data)))

    session=self.session
    data=data.fillna('')
    data_frame_dict=data.to_dict()
    try:
      data_frame_dict={ key:value for key, value in data_frame_dict.items() if value} # filter any key with empty value
      mapped_object=table(**data_frame_dict)                                          # map dictionary to table class
      session.add(mapped_object)                                                      # add data to session
    except:
      raise
    

  def _store_record_bulk(self, table, data):
    '''
    An internal method for storing dataframe records in bulk mode
    '''   
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
 
    if isinstance(data, pd.DataFrame):
      data=data.to_dict(orient='records')

    session=self.session
    if not isinstance(data, dict):
      raise ValueError('Expecting a dictionary and recieved data type: {0}'.format(type(data)))
 
    try:
      session.bulk_insert_mappings(table, data)
    except:
      raise 


  def _format_attribute_table_row(self, data, required_column, attribute_name_column, attribute_value_column ):
    '''
    An internal function for converting attribute table dataframe
    required param:
    data: a attribute dataframe or dictionary
    required_column: column to add to the attribute table, it must be part of the data series
    attribute_name_column: column label for attribute name
    attribute_value_column: column label for attribute value
  
    It returns a pandas series
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    final_list=list()
    data_dict=data.to_dict(orient='records')
    for element in data_dict:
      row_list=list()
      id_name=''
      id_value=''
      for key, value in element.items():
        row_dict=dict()
        if value and key != required_column:
          row_dict[attribute_name_column]=key
          row_dict[attribute_value_column]=value
          row_list.append(row_dict)
        elif value and key == required_column:
          id_name=key
          id_value=value
      row_df=pd.DataFrame(row_list)
      if not id_name and id_value:
        raise ValueError('Required id or value not found for column: {0}'.format(required_column))

      row_df[id_name]=id_value
      row_df_data=row_df.to_dict(orient='records')
      final_list.extend(row_df_data)
    new_data_series=pd.DataFrame(final_list)
    new_data_series=new_data_series.dropna()
    return new_data_series


  def divide_data_to_table_and_attribute(self, data, required_column, table_columns, attribute_name_column='attribute_name', attribute_value_column='attribute_value'):
    '''
    A method for separating data for main and attribute tables
    required params:
    data: a dictionary or dataframe containing the data
    required_column: column to add to the attribute table, it must be part of the data
    table_columns: required columns for the main table
    attribute_name_column: column label for attribute name
    attribute_value_column: column label for attribute value

    It returns two pandas dataframes, one for main table and one for attribute tables 
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    table_df=data.ix[:, table_columns]                                           # slice df for table
    table_attr_columns=list(set(data.columns).difference(set(table_df.columns))) # assign remaining columns to attribute dataframe
    table_attr_columns.append(required_column)                                   # append required column
    table_attr_df=data.ix[:, table_attr_columns]                                 # slice df for attribute table
   
    new_table_attr_df=self._format_attribute_table_row(data=table_attr_df, \
                                                       required_column=required_column, \
                                                       attribute_name_column=attribute_name_column, \
                                                       attribute_value_column=attribute_value_column \
                                                      )    
    return (table_df, new_table_attr_df)
    

  def store_records(self, table, data, mode='serial'):
    '''
    A method for loading data to table
    required parameters:
    table: name of the table class
    data : pandas dataframe or a list of dictionary
    mode : serial / bulk
    '''
    if not hasattr(self,'session'):
      raise AttributeError('Attribute session not found')

    if mode not in ('serial', 'bulk'):
      raise ValueError('Mode {0} is not recognised'.format(mode))

    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)                                                        # convert dicttionary to dataframe

    session=self.session
    try:
      if mode is 'serial':
        data.apply(lambda x: self._store_record_serial(table=table, data=x), axis=1)   # load data in serial mode
      elif mode is 'bulk':
        self._store_record_bulk( table=table, data=data)                               # load data in bulk mode
      session.flush()
    except:
        session.rollback()
        raise


  def store_attributes(self, attribute_table, data, linked_column='', db_id='',  mode='serial'):
    '''
    A method for storing attributes
    required params:
    attribute_table: a attribute table name
    linked_column  : a column name to link the db_id to attribute table
    db_id          : a db_id to link the attribute records
    mode           : serial / bulk
    '''
    if isinstance(data, dict):
      data=pd.DataFrame(data)                                                       # converting to dataframe
    
    if linked_column and db_id: 
      data[linked_column]=db_id                                                     # adding or reseting db_id value for the linked column
    
    try:
      self.store_records(table=attribute_table, data=data, mode=mode)               # storing data to attribute table
    except:
      raise


  def modify_records(self, query, update_values):
    '''
    A method for updating table records
    required params:
    query: a session.query object with filter criteria
    update_values: a dictionaries, with key as the column 
                   name and value as the new updated value
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    session=self.session 
    for row in query:
      try:
        for column_name, new_value in update_values.items():
          row.column_name=new_value
        session.commit()
      except:
        session.rollback()


  def _fetch_records_as_dataframe(self, query):
    '''
    An internal method for fetching database records as dataframe
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    session=self.session
    try:
      result=pd.read_sql(query.statement, session.bind)
      return result                                      # return a dataframe
    except:
      raise
  
 
  def _fetch_records_as_object(self, query):
    '''
    An internal method for fetching database records as query object
    '''
    result=list()
    for row in query:
      yield row                                          # return a generator object

  def _fetch_records_as_one(self, query):
    '''
    An internal method for fetching unique database record
    '''
    try:
      result=query.one()
      return result
    except:
      raise

  def _construct_query(self, table, filter_criteria ):
    '''
    An internal method for query construction
    It doesn't support any join statement
    required params:
    table: table name class
    filter_criteria: a list of criteria, each filter statement has three values, column_name, operator and value
    return a session.query object
    '''   
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')

    if not isinstance(filter_criteria, tuple) and  not isinstance(filter_criteria, list):
      raise ValueError('Expecting a list of filter_criteria, received data type: {0}'.format(type(filter_criteria)))

    session=self.session
    query=session.query(table)
    for filter_statement in filter_criteria:
      if len(filter_statement) != 3:
        raise ValueError('Expecting three parameters for filter criteria, got {0}'.format(len(filter_statement)))

      if filter_statement[1] == '==':
        query=query.filter(filter_statement[0] == filter_statement[2])
    return query  

    
  def fetch_records(self, query, output_mode='dataframe'):
    '''
    A method for fetching records using a query
    optional parameters:
    output_mode: dataframe / object / one
    
    returns a pandas dataframe for dataframe mode and a generator object for object mode
    ''' 
    if output_mode not in ('dataframe', 'object', 'one'):
      raise ValueError('Expecting output_mode as dataframe or object, no support for {0}'.format(output_mode))

    result=''
    try:
      if output_mode == 'dataframe':
        result=self._fetch_records_as_dataframe(query=query)  # result is a dataframe
      elif output_mode == 'object':
        result=self._fetch_records_as_object(query=query)     # result is a generator object
      elif output_mode == 'one':
        result=self._fetch_records_as_one(query=query)        # fetch record as a unique match
      return result
    except:
      raise 

    
  def fetch_records_by_column(self, table, column_name, column_id, output_mode='dataframe'):
    '''
    A method for fetching record with the column
    required param:
    table      : table name
    column_name: a column name
    column_id  : a column id value
    output_mode: dataframe / object
    '''
    if not hasattr(self, 'session'):
      raise AttributeError('Attribute session not found')
 
    session=self.session
    query=session.query(table).filter(column_name==column_id)
    try:
      result=self.fetch_records(query=query, output_mode=output_mode)
      return result
    except:
      raise


  def get_attributes_by_dbid(self, attribute_table, linked_table, linked_column, db_id):
    '''
    A method for fetching attribute records for a specific attribute table with a db_id linked as foreign key
    '''
    session=self.session
    query=session.query(linked_table).join(linked_table)
    filter_criteria=[linked_table.linked_column==db_id]
    try:
      result=self.fetch_records(query=query, filter_criteria=filter_criteria)
      return result
    except:
      raise


