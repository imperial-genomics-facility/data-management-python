import json
import pandas as pd
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Pipeline, Pipeline_seed, Project, Sample, Experiment, Run, Collection, File

class PipelineAdaptor(BaseAdaptor):
  '''
  An adaptor class for Pipeline and Pipeline_seed tables
  ''' 
  def store_pipeline_data(self, data):
    '''
    Load data to Pipeline table
    '''
    try:
      self.store_records(table=Pipeline, data=data)
    except:
      raise

  
  def fetch_pipeline_records_pipeline_name(self, pipeline_name, target_column_name='pipeline_name'):
    '''
    A method for fetching data for Pipeline table
    required params:
    pipeline_name: a name
    target_column_name: default pipeline_name
    '''
    try:
      column=[column for column in Pipeline.__table__.columns \
                       if column.key == target_column_name][0]
      pipeline=self.fetch_records_by_column(table=Pipeline, \
      	                                   column_name=column, \
      	                                   column_id=pipeline, \
      	                                   output_mode='one')
      return pipeline  
    except:
      raise


  def _map_pipeline_seed_info(self, series, seed_table='seed_table', project_column='project_igf_id', seed_id='seed_id', \
                              sample_column='sample_igf_id', experiment_column='experiment_igf_id', run_column='run_igf_id', \
                              collection_column=['name', 'type'], file_column='file_path'):
    '''
    An internal method for mapping pipeline info
    '''
    if not isinstance(data_series, pd.Series):
      data_series=pd.Series(data_series)

    if seed_table not in data_series.index:
      raise ValueError('A value for column table is required: {0}'.format(json.dumps(data_series.to_dict())))

    map_function=''
    temp_column_name=''
    temp_target_column_name=''

    if project_column in data_series.index and data_series.seed_table == 'PROJECT':   
      temp_column_name=project_column
      temp_target_column_name='project_id'
      table_name=Project
    elif sample_column in data_series.index and data_series.seed_table == 'SAMPLE':  
      temp_column_name=sample_column
      temp_target_column_name='sample_id'
      table_name=Sample
    elif experiment_column in data_series.index and data_series.seed_table == 'EXPERIMENT':
      temp_column_name=experiment_column
      temp_target_column_name='experiment_id'
      table_name=Experiment
    elif run_column in data_series.index and data_series.seed_table == 'RUN':
      temp_column_name=run_column      
      temp_target_column_name='run_id'
      table_name=Run
    elif data_series.seed_table == 'COLLECTION':
      temp_column_name=collection_column
      temp_target_column_name='collection_id'
      table_name=Collection
    elif file_column in data_series.index and data_series.seed_table == 'FILE':
      temp_column_name=file_column
      temp_target_column_name='file_id'
      table_name=File
    else:
      raise ValueError('Seed table {0} not supported'.format(data_series.seed_table))
    
    map_function=lambda x: self.map_foreign_table_and_store_attribute(data=x, \
                                                                      lookup_table=table_name, \
                                                                      lookup_column_name=temp_column_name, \
                                                                      target_column_name=temp_target_column_name)       # prepare the function for project
    data_series.map(map_function)                                                                                       # map project_id
    data_series.reindex({temp_target_column_name:seed_id}, inplace=True)
    return data_series


  def create_pipeline_seed(self, data):
    '''
    A method for creating the pipeline seed
    '''
    if not isinstance(data, pd.DataFrame):
      data=pd.DataFrame(data)

    map_function=lambda x: self._map_pipeline_seed_info(series=x)
    data.apply(map_function,1)                                    # map the foreign keys
        
    try:
      self.store_records(table=Pipeline_seed, data=data) 
      self.commit_session()
    except:
      self.rollback_session()
      raise


