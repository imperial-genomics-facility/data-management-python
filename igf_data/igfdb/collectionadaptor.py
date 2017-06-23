import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.useradaptor import FileAdaptor
from igf_data.igfdb.igfTables import Collection, File, Collection_group, Collection_attribute, Experiment, Run, Sample

class CollectionAdaptor(BaseAdaptor):
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

  def _check_and_fetch_collection_group_requirements(self, data_series, table_column='table', file_column='file_path', \
                                                     sample_column='sample_igf_id', experiment_column='experiment_igf_id' \
                                                     run_column='run_igf_id', name_column='name', type_column='type' ):
    '''
    An internal function for checking data requirements
    required param:
    data_series: a data series
    optional params:
    table_column: default table
    file_column: default file_path
    sample_column: default sample_igf_id
    experiment_column: default experiment_igf_id
    run_column: run_igf_id
    name_column: default name
    type_column: default type
    
    returns a pandas data series after adding a primary key of the respective table
    '''
    if not isinstance(data_series, pd.Series):
      data_series=pd.Series(data_series)

    data_series=data_series.dropna()                                               # delete None values from the series
    if table_column not in data_series.index: 
      raise ValueError('A value for column table is required: {0}'.format(json.dumps(data_series.to_dict())))   

    if not name_column in data_series.index or not type_column in data_series.index: 
       raise ValueError('A value for {0} and {1} is required: {3}'.format(name_column, type_column, json.dumps(data_series.to_dict())))
      
    if data_series.table == 'file':                                                # FILE
      if file_column in data_series.index:
        raise ValueError('Missing {0}: {1}'.format(file_column, json.dumps(data_series.to_dict())))

      file=self.fetch_file_records_path(file_path=file_path)
      data_series['file_id']=file.file_id
      del data_series[file_column]
    elif data_series.table == 'sample':                                            # SAMPLE
      if not sample_column in data_series.index:
        raise ValueError('Missing {0}: {1}'.format(sample_column, json.dumps(data_series.to_dict())))

      sample=self.fetch_sample_records_igf_id(sample_igf_id=sample_igf_id)
      data_series['sample_id']=sample.sample_id
      del data_series[sample_column]
    elif data_series.table == 'experiment':                                        # EXPERIMENT
      if not experiment_column in data_series.index:
        raise ValueError('Missing {0}: {1}'.format(experiment_column, json.dumps(data_series.to_dict())))
      
      experiment=self.fetch_experiment_records_igf_id(experiment_igf_id=experiment_igf_id)
      data_series['experiment_id']=experiment.experiment_id
      del data_series[experiment_column]
    elif data_series.table == 'run':                                               # RUN
      if not run_column in data_series.index:
        raise ValueError('Missing {0}: {1}'.format(run_column, json.dumps(data_series.to_dict())))

      run=self.fetch_run_records_igf_id(run_igf_id=run_igf_id)
      data_series['run_id']=run.run_id
      del data_series[run_column]
    return data_series


  def create_collection_group(self, data):
    '''
    A function for creating collection group
    ['name':'a collection name', 'type':'a collection type', 'table':'a table_name', required column: value]
    Following collumns must be present for the respective table type
    type : file       => required_column: file_path
    type : sample     => required_column: sample_igf_id
    type : experiment => required_column: experiment_igf_id
    type : run        => required_column: run_igf_id
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)
 
    data.apply(lambda x: self._check_and_fetch_collection_group_requirements(data_series=x))   # check and add required columns in data frame
    
     
      
