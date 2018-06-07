import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Experiment, Run, Run_attribute, Seqrun, Sample

class RunAdaptor(BaseAdaptor):
  '''
   An adaptor class for Run and Run_attribute tables
  '''

  def store_run_and_attribute_data(self, data, autosave=True):
    '''
    A method for dividing and storing data to run and attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the run data
    :param autosave: A toggle for saving data automatically to db, default True
    '''
    (run_data, run_attr_data)=self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_run_data(data=run_data)                                        # store run
      if len(run_attr_data.index)>0:                                            # check if any attribute exists
        self.store_run_attributes(data=run_attr_data)                           # store run attributes
      if autosave:
        self.commit_session()                                                   # save changes to database
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(self, data, required_column='run_igf_id',
                                         attribute_name_column='attribute_name',
                                         attribute_value_column='attribute_value'):
    '''
    A method for separating data for Run and Run_attribute tables
    
    :param data: A list of dictionaries or a Pandas DataFrame
    :param required_column: column name to add to the attribute data
    :param attribute_name_column: label for attribute name column
    :param attribute_value_column: label for attribute value column
    :returns: Two pandas dataframes, one for Run and another for Run_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      run_columns=self.get_table_columns(table_name=Run,
                                         excluded_columns=['run_id',
                                                           'seqrun_id',
                                                           'experiment_id'])    # get required columns for run table
      run_columns.extend(['seqrun_igf_id', 'experiment_igf_id'])
      (run_df, run_attr_df)=BaseAdaptor.\
                            divide_data_to_table_and_attribute(\
                              self,
                              data=data,
                              required_column=required_column,
                              table_columns=run_columns,
                              attribute_name_column=attribute_name_column,
                              attribute_value_column=attribute_value_column
                            )                                                   # divide data to run and attribute table
      return (run_df, run_attr_df)
    except:
      raise


  def store_run_data(self, data, autosave=False):
    '''
    A method for loading data to Run table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the attribute data
    :param autosave: A toggle for saving data automatically to db, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)

      if 'seqrun_igf_id' in data.columns:
        seqrun_map_function=lambda x: self.map_foreign_table_and_store_attribute( \
                                             data=x,
                                             lookup_table=Seqrun,
                                             lookup_column_name='seqrun_igf_id',
                                             target_column_name='seqrun_id')    # prepare seqrun mapping function
        new_data=data.apply(seqrun_map_function, axis=1)                        # map seqrun id
        data=new_data                                                           # overwrite data

      if 'experiment_igf_id' in data.columns:
        exp_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                          data=x,
                                          lookup_table=Experiment,
                                          lookup_column_name='experiment_igf_id',
                                          target_column_name='experiment_id')   # prepare experiment mapping function
        new_data=data.apply(exp_map_function, axis=1)                           # map experiment id
        data=new_data                                                           # overwrite data

      self.store_records(table=Run, data=data)                                  # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def store_run_attributes(self, data, run_id='', autosave=False):
    '''
    A method for storing data to Run_attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the attribute data
    :param autosave: A toggle for saving data automatically to db, default True
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                 # convert data to dataframe

      if 'run_igf_id' in data.columns:
        run_map_function=lambda x: self.map_foreign_table_and_store_attribute(\
                                               data=x,
                                               lookup_table=Run,
                                               lookup_column_name='run_igf_id',
                                               target_column_name='run_id')     # prepare run mapping function
        new_data=data.apply(run_map_function, axis=1)
        data=new_data                                                           # overwrite data

      self.store_attributes(attribute_table=Run_attribute,
                            linked_column='run_id',
                            db_id=run_id,
                            data=data)                                          # store without autocommit
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def check_run_records_igf_id(self, run_igf_id, target_column_name='run_igf_id'):
    '''
    A method for existing data for Run table
    
    :param run_igf_id: an igf id
    :param target_column_name: a column name, default run_igf_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      run_check=False
      column=[column for column in Run.__table__.columns \
                       if column.key == target_column_name][0]
      run_obj=self.fetch_records_by_column(table=Run, \
                                           column_name=column, \
                                           column_id=run_igf_id, \
                                           output_mode='one_or_none')
      if run_obj is not None:
        run_check=True
      return run_check
    except:
      raise


  def fetch_run_records_igf_id(self, run_igf_id, target_column_name='run_igf_id'):
    '''
    A method for fetching data for Run table
    
    :param run_igf_id: an igf id
    :param target_column_name: a column name, default run_igf_id
    '''
    try:
      column=[column for column in Run.__table__.columns \
                       if column.key == target_column_name][0]
      run=self.fetch_records_by_column(table=Run, \
                                           column_name=column, \
                                           column_id=run_igf_id, \
                                           output_mode='one')
      return run  
    except:
      raise


  def fetch_sample_info_for_run(self,run_igf_id):
    '''
    A method for fetching sample information linked to a run_igf_id
    
    :param run_igf_id: A run_igf_id to search database
    '''
    try:
      session=self.session
      query=session.query(Sample).\
                    join(Experiment).\
                    join(Run).\
                    filter(Run.run_igf_id==run_igf_id)
      samples=self.fetch_records(query=query, output_mode='dataframe')          # get results
      samples=samples.to_dict(orient='records')
      return samples[0]
    except:
      raise


  def fetch_project_sample_and_experiment_for_run(self,run_igf_id):
    '''
    A method for fetching project, sample and experiment information for a run
    
    :param run_igf_id: A run igf id string
    :returns: A list of three strings, or None if not found
               project_igf_id
               sample_igf_id
               experiment_igf_id
    '''
    try:
      project_igf_id=None
      sample_igf_id=None
      experiment_igf_id=None
      query=self.session.\
            query(Project.project_igf_id,
                  Sample.sample_igf_id,
                  Experiment.experiment_igf_id).\
            join(Sample).\
            join(Experiment).\
            join(Run).\
            filter(Project.project_id==Sample.project_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Experiment.experiment_id==Run.experiment_id).\
            filter(Run.run_igf_id==run_igf_id)                                  # get query
      data=self.fetch_records(query=query,
                              output_mode='one_or_none')
      if data is not None:
        project_igf_id=data.project_igf_id
        sample_igf_id=data.sample_igf_id
        experiment_igf_id=data=experiment_igf_id

      return project_igf_id,sample_igf_id,experiment_igf_id
    except:
      raise
