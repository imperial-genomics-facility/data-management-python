import json
import pandas as pd
from sqlalchemy import update
from sqlalchemy.sql import table, column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Pipeline, Pipeline_seed, Project, Sample, Experiment, Run, Collection, File, Seqrun

class PipelineAdaptor(BaseAdaptor):
  '''
  An adaptor class for Pipeline and Pipeline_seed tables
  ''' 
  def store_pipeline_data(self, data, autosave=True):
    '''
    Load data to Pipeline table
    '''
    try:
      self.store_records(table=Pipeline, data=data)
      if autosave:
        self.commit_session()
    except:
      self.rollback_session()
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


  def fetch_pipeline_seed(self, pipeline_id, seed_id, seed_table, target_column_name=['pipeline_id', 'seed_id', 'seed_table']):
    '''
    A method for fetching unique pipeline seed using pipeline_id, seed_id and seed_table
    '''
    try:
      column_list=[column for column in Collection.__table__.columns \
                     if column.key in target_column_name]
      column_data=dict(zip(column_list,[pipeline_id, seed_id, seed_table]))
      pipe_seed=self.fetch_records_by_multiple_column(table=Pipeline_seed, column_data=column_data, output_mode='one')
      return pipe_seed
    except:
      raise

  def _map_pipeline_id_to_data(self, data):
    '''
    An internal method for mapping pipeline_id to datafame and remove the pipeline_name
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      if 'pipeline_name' in data.columns:
        pipeline_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                                 data=x, \
                                                 lookup_table=Pipeline, \
                                                 lookup_column_name='pipeline_name', \
                                                 target_column_name='pipeline_id')     # prepare the function for Pipeline id
        new_data=data.apply(pipeline_map_function, axis=1)                             # map pipeline id foreign key id
        data=new_data
      return data
    except:
      raise

  def create_pipeline_seed(self, data, autosave=True, status_column='status', seeded_label='SEEDED', required_columns=['pipeline_id', 'seed_id', 'seed_table']):
    '''
    A method for creating new entry in th pipeline_seed table
    required params:
    data: Dataframe or hash, it sould contain following fields
          pipeline_name / pipeline_id
          seed_id
          seed_table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      data=self._map_pipeline_id_to_data(data)                                       # overwrite data
      data[status_column]=seeded_label                                               # overwrite status as seeded
      if not set((required_columns)).issubset(set(tuple(data.columns))):
        raise valueError('Missing required columns for pipeline seed. required: {0}, got: {1}'.format(required_columns, tuple(data.columns)))

      self.store_records(table=Pipeline_seed, data=data)
      if autosave:
        self.commit_session()
    except:
      self.rollback_session()
      raise

 
  def _map_and_update_pipeline_seed(self, data_series):
    '''
    An internal function for updating pipeline_seed status
    '''
    if not isinstance(data_series, pd.Series):
      data_series=pd.Series(data_series)
    
    try:
      self.session.query(Pipeline_seed).\
           filter(Pipeline_seed.pipeline_id==data_series.pipeline_id).\
           filter(Pipeline_seed.seed_id==data_series.seed_id).\
           filter(Pipeline_seed.seed_table==data_series.seed_table).\
           update({'status':data_series.status})
    except:
      raise


  def update_pipeline_seed(self, data, autosave=True, required_columns=['pipeline_id', 'seed_id', 'seed_table', 'status']):
    '''
    A method for updating the seed status in pipeline_seed table
    required params:
    data: dataframe or a hash, should contain following fields
          pipeline_name / pipeline_id
          seed_id
          seed_table
          status 
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      data=self._map_pipeline_id_to_data(data)                                        # overwrite data
      if not set((required_columns)).issubset(set(tuple(data.columns))):
        raise valueError('Missing required columns for pipeline seed. required: {0}, got: {1}'.format(required_columns, tuple(data.columns)))

      seed_update_func=lambda x: self._map_and_update_pipeline_seed(data_series=x)     # defined update function
      data.apply(seed_update_func, axis=1)                                             # apply function on dataframe
      if autosave:
        self.commit_session()                                                          # commit changes in db
    except:
      self.rollback_session()
      raise


  def seed_new_seqruns(self, pipeline_name, autosave=True, seed_table='SEQRUN'):
    '''
    A method for creating seed for new seqruns
    required params:
    pipeline_name: A pipeline name
    '''
    try:
      seeded_seqruns=self.session..query(Seqrun.seqrun_igf_id).\
                   filter(Seqrun.reject_run=='N').\
                   join(Pipeline_seed,Pipeline_seed.seed_id==Seqrun.seqrun_id).\
                   filter(Pipeline_seed.seed_table==seed_table).\
                   join(Pipeline, Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
                   filter(Pipeline.pipeline_name==pipeline_name).subquery()               # get list of seqruns which are already seeded for the pipeline
      
      seqrun_query=base.session.query(Seqrun.seqrun_id).\
                   filter(Seqrun.reject_run=='N').\
                   filter(~Seqrun.seqrun_igf_id.in_(seeded_seqrun))

      new_seqruns=self.fetch_records(query=seqrun_query, output_mode='object')            # get lists of valid seqruns which are not in the previous list

      seqrun_data=list()
      for seqrun in new_seqruns: 
        seqrun_data.append({'seed_id':seqrun.seqrun_id, 'seed_table':seed_table,'pipeline_name':pipeline_name})
 
      if len(seqrun_data) > 0:
        self.create_pipeline_seed(data=seqrun_data, autosave)

    except:
      raise  


