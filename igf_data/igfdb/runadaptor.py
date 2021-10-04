import pandas as pd
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Experiment, Run, Run_attribute, Seqrun, Sample,Project

class RunAdaptor(BaseAdaptor):
  '''
   An adaptor class for Run and Run_attribute tables
  '''

  def store_run_and_attribute_data(self,data,autosave=True):
    '''
    A method for dividing and storing data to run and attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the run data
    :param autosave: A toggle for saving data automatically to db, default True
    :returns: None
    '''
    (run_data, run_attr_data) = \
      self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_run_data(data=run_data)                                        # store run
      if len(run_attr_data.index)>0:                                            # check if any attribute exists
        self.store_run_attributes(data=run_attr_data)                           # store run attributes
      if autosave:
        self.commit_session()                                                   # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed tostore run records, error: {0}'.format(e))


  def divide_data_to_table_and_attribute(
        self,data,required_column='run_igf_id',table_columns=None,
        attribute_name_column='attribute_name',attribute_value_column='attribute_value'):
    '''
    A method for separating data for Run and Run_attribute tables
    
    :param data: A list of dictionaries or a Pandas DataFrame
    :param table_columns: List of table column names, default None
    :param required_column: column name to add to the attribute data
    :param attribute_name_column: label for attribute name column
    :param attribute_value_column: label for attribute value column
    :returns: Two pandas dataframes, one for Run and another for Run_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      run_columns = \
        self.get_table_columns(
          table_name=Run,
          excluded_columns=[
            'run_id',
            'seqrun_id',
            'experiment_id'])                                                   # get required columns for run table
      run_columns.\
        extend([
          'seqrun_igf_id',
          'experiment_igf_id'])
      (run_df, run_attr_df) = \
        BaseAdaptor.\
          divide_data_to_table_and_attribute(
            self,
            data=data,
            required_column=required_column,
            table_columns=run_columns,
            attribute_name_column=attribute_name_column,
            attribute_value_column=attribute_value_column)                      # divide data to run and attribute table
      return (run_df, run_attr_df)
    except Exception as e:
      raise ValueError(
              'Failed to divied run records, error: {0}'.format(e))


  def store_run_data(self,data,autosave=False):
    '''
    A method for loading data to Run table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the attribute data
    :param autosave: A toggle for saving data automatically to db, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      if 'seqrun_igf_id' in data.columns:
        seqrun_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Seqrun,
              lookup_column_name='seqrun_igf_id',
              target_column_name='seqrun_id')                                   # prepare seqrun mapping function
        data['seqrun_id'] = ''
        data = \
          data.apply(
            seqrun_map_function,
            axis=1,
            result_type=None)                                                   # map seqrun id
        data.drop(
          'seqrun_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                          # overwrite data

      if 'experiment_igf_id' in data.columns:
        exp_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Experiment,
              lookup_column_name='experiment_igf_id',
              target_column_name='experiment_id')                               # prepare experiment mapping function
        data['experiment_id'] = ''
        data = \
          data.apply(
            exp_map_function,
            axis=1,
            result_type=None)                           # map experiment id
        data.drop(
          'experiment_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                           # overwrite data

      self.store_records(
        table=Run,
        data=data)                                                              # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store run data, error: {0}'.format(e))


  def store_run_attributes(self,data,run_id='',autosave=False):
    '''
    A method for storing data to Run_attribute table
    
    :param data: A list of dictionaries or a Pandas DataFrame containing the attribute data
    :param autosave: A toggle for saving data automatically to db, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert data to dataframe

      if 'run_igf_id' in data.columns:
        run_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Run,
              lookup_column_name='run_igf_id',
              target_column_name='run_id')                                      # prepare run mapping function
        data['run_id'] = ''
        data = \
          data.apply(
            run_map_function,
            axis=1,
            result_type=None)
        data.drop(
          'run_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                          # overwrite data

      self.store_attributes(
        attribute_table=Run_attribute,
        linked_column='run_id',
        db_id=run_id,
        data=data)                                                              # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store run atributes, error: {0}'.format(e))


  def check_run_records_igf_id(
        self,run_igf_id,target_column_name='run_igf_id'):
    '''
    A method for existing data for Run table
    
    :param run_igf_id: an igf id
    :param target_column_name: a column name, default run_igf_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      run_check = False
      column = [
        column
          for column in Run.__table__.columns \
            if column.key == target_column_name][0]
      run_obj = \
        self.fetch_records_by_column(
          table=Run,
          column_name=column,
          column_id=run_igf_id,
          output_mode='one_or_none')
      if run_obj is not None:
        run_check = True
      return run_check
    except Exception as e:
      raise ValueError(
              'Failed to check run records, error: {0}'.format(e))


  def fetch_run_records_igf_id(
        self,run_igf_id,target_column_name='run_igf_id'):
    '''
    A method for fetching data for Run table
    
    :param run_igf_id: an igf id
    :param target_column_name: a column name, default run_igf_id
    :returns: Run record
    '''
    try:
      column = [
        column
          for column in Run.__table__.columns \
            if column.key == target_column_name][0]
      run = \
        self.fetch_records_by_column(
          table=Run,
          column_name=column,
          column_id=run_igf_id,
          output_mode='one')
      return run
    except Exception as e:
      raise ValueError(
              'Failed to fetch run record, error: {0}'.format(e))


  def fetch_sample_info_for_run(self,run_igf_id):
    '''
    A method for fetching sample information linked to a run_igf_id
    
    :param run_igf_id: A run_igf_id to search database
    :returns: Sample record
    '''
    try:
      session = self.session
      query = \
        session.\
          query(Sample).\
          join(Experiment,
               Sample.sample_id==Experiment.sample_id).\
          join(Run,
               Experiment.experiment_id==Run.experiment_id).\
          filter(Run.run_igf_id==run_igf_id)
      samples = \
        self.fetch_records(
          query=query,
          output_mode='dataframe')                                              # get results
      samples = \
        samples.to_dict(orient='records')
      return samples[0]
    except Exception as e:
      raise ValueError(
              'Failed to fetch sample, error: {0}'.format(e))


  def fetch_project_sample_and_experiment_for_run(self,run_igf_id):
    '''
    A method for fetching project, sample and experiment information for a run
    
    :param run_igf_id: A run igf id string
    :returns: A list of three strings, or None if not found
               * project_igf_id
               * sample_igf_id
               * experiment_igf_id
    '''
    try:
      project_igf_id = None
      sample_igf_id = None
      experiment_igf_id = None
      query = \
        self.session.\
          query(Project.project_igf_id,
                Sample.sample_igf_id,
                Experiment.experiment_igf_id).\
          join(Sample,
               Project.project_id==Sample.project_id).\
          join(Experiment,
               Sample.sample_id==Experiment.sample_id).\
          join(Run,
               Experiment.experiment_id==Run.experiment_id).\
          filter(Project.project_id==Sample.project_id).\
          filter(Sample.sample_id==Experiment.sample_id).\
          filter(Experiment.experiment_id==Run.experiment_id).\
          filter(Run.run_igf_id==run_igf_id)                                    # get query
      data = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if data is not None:
        project_igf_id = data.project_igf_id
        sample_igf_id = data.sample_igf_id
        experiment_igf_id = data.experiment_igf_id

      return project_igf_id,sample_igf_id,experiment_igf_id
    except Exception as e:
      raise ValueError(
              'Failed to fetch project, sample and exp, error: {0}'.format(e))

  def fetch_flowcell_and_lane_for_run(self,run_igf_id):
    '''
    A run adapter method for fetching flowcell id and lane info for each run

    :param run_igf_id: A run igf id string
    :returns: Flowcell id and lane number
              It will return None if no records found
    '''
    try:
      flowcell_id = None
      lane_number = None
      query = \
        self.session.\
          query(Run.run_igf_id,
                Run.lane_number,
                Seqrun.flowcell_id).\
          join(Seqrun,
               Run.seqrun_id==Seqrun.seqrun_id).\
          filter(Run.run_igf_id==run_igf_id)
      data = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if data is not None:
        flowcell_id = data.flowcell_id
        lane_number = data.lane_number

      return flowcell_id,lane_number
    except Exception as e:
      raise ValueError(
              'Failed to fetch flowcell and lane, error: {0}'.format(e))


  def update_run_attribute_records_by_igfid(self, update_data, autosave=True):
    try:
      if not isinstance(update_data, list):
        raise ValueError(
                'Expecting a list of dictionary and got {0}'.\
                  format(type(update_data)))
      update_data = pd.DataFrame(update_data)
      if 'run_igf_id' not in update_data.columns or \
         'attribute_name' not in update_data.columns or \
         'attribute_value' not in update_data.columns:
        raise ValueError('Missing required column from update data')
      run_map_function = \
        lambda x: \
          self.map_foreign_table_and_store_attribute(
            data=x,
            lookup_table=Run,
            lookup_column_name='run_igf_id',
            target_column_name='run_id')
      update_data = \
        update_data.\
          apply(
            run_map_function,
            axis=1,
            result_type=None)
      update_data.\
        drop(
          'run_igf_id',
          axis=1,
          inplace=True)
      update_data = \
        update_data.\
          to_dict(orient='records')
      for run_data in update_data:
        run_id = run_data.get('run_id')
        query = \
          self.session.\
            query(Run_attribute).\
            filter(Run_attribute.run_id==run_id)                                   # define base query
        query.update(run_data)
      if autosave:
        self.commit_session()
    except Exception as e:
      self.rollback_session()
      raise ValueError('Failed to update run attribute records, error: {0}'.format(e))