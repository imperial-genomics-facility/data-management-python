import pandas as pd
from sqlalchemy import update
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Pipeline, Pipeline_seed, Platform, Project, Sample, Experiment, Run, Collection, File, Seqrun, Collection_group

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

  
  def fetch_pipeline_records_pipeline_name(self, pipeline_name, 
                                           target_column_name='pipeline_name'):
    '''
    A method for fetching data for Pipeline table
    
    :param pipeline_name: a name
    :param target_column_name: default pipeline_name
    '''
    try:
      column=[column for column in Pipeline.__table__.columns \
                       if column.key == target_column_name][0]
      pipeline=\
        self.fetch_records_by_column(\
          table=Pipeline,
      	  column_name=column,
          column_id=pipeline_name,
      	  output_mode='one')
      return pipeline  
    except:
      raise


  def fetch_pipeline_seed(self, pipeline_id, seed_id, seed_table, 
                          target_column_name=['pipeline_id', 'seed_id', 
                                              'seed_table']):
    '''
    A method for fetching unique pipeline seed using pipeline_id, seed_id and seed_table
    
    :param pipeline_id: A pipeline db id
    :param seed_id: A seed entry db id
    :param seed_table: A seed table name
    :param target_column_name: Target set of columns
    '''
    try:
      column_list=[column for column in Collection.__table__.columns \
                     if column.key in target_column_name]
      column_data=dict(zip(column_list,[pipeline_id, seed_id, seed_table]))
      pipe_seed=\
        self.fetch_records_by_multiple_column(\
          table=Pipeline_seed,
          column_data=column_data,
          output_mode='one')
      return pipe_seed
    except:
      raise


  def __map_seed_data_to_foreign_table(self, data):
    '''
    An internal method for mapping seed records to foreign tables

    :param data: A pandas series containing pipeline_seed table entries
    :returns: A pandas series containing following entries
                seqrun - Entries from seqrun and platform tables
                experiment - Entries from Project, Sample and Experiment tables
    '''
    try:
      if not isinstance(data, pd.Series):
        data=pd.Series(data)

      if 'seed_table' not in data or \
         data.seed_table not in ('seqrun','experiment'):
        raise ValueError('seed_table {0} not supported'.format(data.seed_table))

      new_data=pd.Series()
      query = None
      if data.seed_table=='seqrun':
        query = self.session.\
                query(Seqrun,
                      Platform.platform_igf_id,
                      Platform.model_name,
                      Platform.vendor_name,
                      Platform.software_name,
                      Platform.software_version). \
                join(Platform,
                     Seqrun.platform_id==Platform.platform_id).\
                filter(Seqrun.platform_id==Platform.platform_id).\
                filter(Seqrun.reject_run=='N').\
                filter(Seqrun.seqrun_id==data.seed_id)
      elif data.seed_table=='experiment':
        query=self.session.\
              query(Experiment,
                    Project.project_igf_id,
                    Sample.sample_igf_id,
                    Sample.species_name,
                    Sample.phenotype,
                    Sample.sex,
                    Sample.cell_line,
                    Sample.donor_anonymized_id,
                    Sample.sample_submitter_id).\
              join(Sample,
                   Sample.sample_id==Experiment.sample_id).\
              join(Project,
                   Project.project_id==Sample.project_id).\
              filter(Project.project_id==Sample.project_id).\
              filter(Project.project_id==Experiment.project_id).\
              filter(Sample.status=='ACTIVE').\
              filter(Experiment.status=='ACTIVE').\
              filter(Experiment.experiment_id==data.seed_id)
      new_data=self.fetch_records(query)
      return new_data
    except:
      raise



  def fetch_pipeline_seed_with_table_data(self, pipeline_name, table_name='seqrun',
                                          status='SEEDED'):
    '''
    A method for fetching linked table records for the seeded entries in pipeseed table
    
    :param pipeline_name: A pipeline name
    :param table_name: A table name for pipeline_seed lookup, default seqrun
    :param status: A text label for seeded status, default is SEEDED
    :returns: Two pandas dataframe for pipeline_seed entries and data from other tables
    '''
    try:
      if table_name not in ('seqrun','experiment'):
        raise ValueError('Not support for fetching pipeseed data for table {0}'.\
                         format(table_name))

      pipeseed_data=pd.DataFrame()
      table_data=pd.DataFrame()                                                 # return empty dataframes if no data found

      pipeseed_query=self.session.\
                     query(Pipeline_seed).\
                     join(Pipeline,
                          Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
                     filter(Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
                     filter(Pipeline_seed.status==status).\
                     filter(Pipeline.pipeline_name==pipeline_name).\
                     filter(Pipeline_seed.seed_table==table_name)
      pipeseed_data=\
        self.fetch_records(query=pipeseed_query)
      if len(pipeseed_data.index)>0:
        table_data=\
          pd.concat([self.__map_seed_data_to_foreign_table(data=record) \
                      for record in pipeseed_data.to_dict(orient='records')], 
                    axis=0)                                                     # transform dataframe to dictionary and map records

      return (pipeseed_data, table_data)
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
        pipeline_map_function=\
          lambda x: self.map_foreign_table_and_store_attribute(\
                      data=x,
                      lookup_table=Pipeline,
                      lookup_column_name='pipeline_name',
                      target_column_name='pipeline_id')                         # prepare the function for Pipeline id
        new_data=data.apply(pipeline_map_function, axis=1)                             # map pipeline id foreign key id
        data=new_data
      return data
    except:
      raise

  def create_pipeline_seed(self, data, autosave=True, status_column='status',
                           seeded_label='SEEDED',
                           required_columns=['pipeline_id',
                                             'seed_id',
                                             'seed_table']):
    '''
    A method for creating new entry in th pipeline_seed table
    
    :param data: Dataframe or hash, it sould contain following fields
                   * pipeline_name / pipeline_id
                   * seed_id
                   * seed_table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      data=self._map_pipeline_id_to_data(data)                                       # overwrite data
      data[status_column]=seeded_label                                               # overwrite status as seeded
      if not set((required_columns)).issubset(set(tuple(data.columns))):
        raise ValueError('Missing required columns for pipeline seed. required: {0}, got: {1}'.format(required_columns, tuple(data.columns)))

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
      self.session.\
      query(Pipeline_seed).\
      filter(Pipeline_seed.pipeline_id==data_series.pipeline_id).\
      filter(Pipeline_seed.seed_id==data_series.seed_id).\
      filter(Pipeline_seed.seed_table==data_series.seed_table).\
      update({'status':data_series.status})
    except:
      raise


  def update_pipeline_seed(self, data, autosave=True,
                           required_columns=['pipeline_id',
                                             'seed_id',
                                             'seed_table',
                                             'status']):
    '''
    A method for updating the seed status in pipeline_seed table
    
    :param data: dataframe or a hash, should contain following fields
                 * pipeline_name / pipeline_id
                 * seed_id
                 * seed_table
                 * status 
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      data=self._map_pipeline_id_to_data(data)                                        # overwrite data
      if not set((required_columns)).issubset(set(tuple(data.columns))):
        raise ValueError('Missing required columns for pipeline seed. required: {0}, got: {1}'.format(required_columns, tuple(data.columns)))

      seed_update_func=\
        lambda x: self._map_and_update_pipeline_seed(data_series=x)             # defined update function
      data.apply(seed_update_func, axis=1)                                             # apply function on dataframe
      if autosave:
        self.commit_session()                                                          # commit changes in db
    except:
      if autosave:
        self.rollback_session()
      raise


  def seed_new_seqruns(self, pipeline_name, autosave=True, seed_table='seqrun'):
    '''
    A method for creating seed for new seqruns
    
    :param pipeline_name: A pipeline name
    '''
    try:
      seeded_seqruns=\
        self.session.\
        query(Seqrun.seqrun_igf_id).\
        filter(Seqrun.reject_run=='N').\
        join(Pipeline_seed,
             Pipeline_seed.seed_id==Seqrun.seqrun_id).\
        filter(Pipeline_seed.seed_table==seed_table).\
        join(Pipeline,
            Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
        filter(Pipeline.pipeline_name==pipeline_name).subquery()                # get list of seqruns which are already seeded for the pipeline
      
      seqrun_query=\
        self.session.\
        query(Seqrun.seqrun_id).\
        filter(Seqrun.reject_run=='N').\
        filter(~Seqrun.seqrun_igf_id.in_(seeded_seqruns))

      new_seqruns=\
        self.fetch_records(\
          query=seqrun_query,
          output_mode='object')                                                 # get lists of valid seqruns which are not in the previous list

      seqrun_data=list()
      for seqrun in new_seqruns: 
        seqrun_data.append({'seed_id':seqrun.seqrun_id,
                            'seed_table':seed_table,
                            'pipeline_name':pipeline_name})
 
      if len(seqrun_data) > 0:
        self.create_pipeline_seed(data=seqrun_data, autosave=autosave)

    except:
      raise  


  def seed_new_experiments(self,pipeline_name,species_name_list,fastq_type,project_list=None,
                           library_source_list=None,active_status='ACTIVE',
                           autosave=True,seed_table='experiment'):
    '''
    A method for seeding new experiments for primary analysis
    
    :param pipeline_name: Name of the analysis pipeline
    :param project_list: List of projects to consider for seeding analysis pipeline, default None
    :param library_source_list: List of library source to consider for analysis, default None
    :param species_name_list: List of sample species to consider for seeding analysis pipeline
    :param active_status: Label for active status, default ACTIVE
    :param autosave: A toggle for autosaving records in database, default True
    :param seed_tabel: Seed table for pipeseed table, default experiment
    :returns: A list of available projects for seeding analysis table (if project_list is None) or None
              and a list of seeded experiments or None
    '''
    try:
      seeded_experiments=\
        self.session.\
        query(Experiment.experiment_id).\
        join(Pipeline_seed,
             Experiment.experiment_id==Pipeline_seed.seed_id).\
        join(Pipeline,
             Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
        filter(Pipeline.pipeline_name==pipeline_name).\
        filter(Pipeline_seed.pipeline_id==Pipeline.pipeline_id).\
        filter(Pipeline_seed.seed_table==seed_table).\
        filter(Pipeline_seed.status.in_(['SEEDED',
                                         'RUNNING',
                                         'FINISHED'])).\
                         subquery()                                             # get list of seeded and running experiments
      new_experiments_query=\
        self.session.\
        query(Experiment.experiment_id,
              Project.project_igf_id).\
        join(Sample,
             Sample.sample_id==Experiment.sample_id).\
        join(Project,
             Project.project_id==Sample.project_id).\
        join(Run,
             Experiment.experiment_id==Run.experiment_id).\
        join(Collection,
             Run.run_igf_id==Collection.name).\
        join(Collection_group,
             Collection.collection_id==Collection_group.collection_id).\
        join(File,
             File.file_id==Collection_group.file_id).\
        filter(Experiment.sample_id==Sample.sample_id).\
        filter(Project.project_id==Sample.project_id).\
        filter(Run.experiment_id==Experiment.experiment_id).\
        filter(Sample.species_name.in_(species_name_list)).\
        filter(Collection.type==fastq_type).\
        filter(Collection.collection_id==Collection_group.collection_id).\
        filter(File.file_id==Collection_group.file_id).\
        filter(Experiment.status==active_status).\
        filter(Run.status==active_status).\
        filter(Experiment.experiment_id.notin_(seeded_experiments))
      if library_source_list is not None and \
         isinstance(library_source_list, list) and \
         len(library_source_list)>0:
        new_experiments_query=\
          new_experiments_query.\
          filter(Experiment.library_source.in_(library_source_list))            # filter experiment based on library source

      if project_list is not None and \
         isinstance(project_list, list) and \
         len(project_list) >0:
        new_experiments_query=\
          new_experiments_query.\
          filter(Project.project_igf_id.in_(project_list))

      new_experiments=\
        self.fetch_records(\
          query=new_experiments_query,
          output_mode='dataframe')
      if project_list is None or \
         (isinstance(project_list, list) and \
          len(project_list)==0):
        available_project_list=\
          list(set(new_experiments['project_igf_id'].values))                   # get unique list of available projects
        return available_project_list, None
      else:
        available_experiments_list=\
          list(set(new_experiments['experiment_id'].values))                    # get available experiments
        exp_data=list()
        for exp_id in available_experiments_list:
          exp_data.append({'seed_id':exp_id,
                           'seed_table':seed_table,
                           'pipeline_name':pipeline_name})
 
        seeded_project_list=None
        if len(exp_data) > 0:
          self.create_pipeline_seed(data=exp_data,
                                    autosave=autosave)                          # seed pipeline

          seeded_project_list=list(set(new_experiments['project_igf_id'].\
                                       drop_duplicates().\
                                       values))

        if isinstance(seeded_project_list,list) and \
           len(seeded_project_list)==0:
          seeded_project_list=None

        return None,seeded_project_list

    except:
      raise
