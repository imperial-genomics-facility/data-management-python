import json
import pandas as pd
from sqlalchemy.sql import table,column,func
from sqlalchemy import distinct
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Project,Sample,Sample_attribute,Experiment,Run,Seqrun,Platform


class SampleAdaptor(BaseAdaptor):
  '''
  An adaptor class for Sample and Sample_attribute tables
  '''

  def store_sample_and_attribute_data(self,data,autosave=True):
    '''
    A method for dividing and storing data to sample and attribute table
    '''
    (sample_data, sample_attr_data) = \
      self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_sample_data(data=sample_data)                                  # store sample records
      if len(sample_attr_data.index) > 0:                                       # check if any attribute is present
        self.store_sample_attributes(data=sample_attr_data)                     # store project attributes
      if autosave:
        self.commit_session()
    except:
      if autosave:
        self.rollback_session()
      raise


  def divide_data_to_table_and_attribute(
        self,data,required_column='sample_igf_id',
        table_columns=None,attribute_name_column='attribute_name',
        attribute_value_column='attribute_value'):
    '''
    A method for separating data for Sample and Sample_attribute tables

    :param data: A list of dictionaries or a pandas dataframe
    :param table_columns: List of table column names, default None
    :param required_column: column name to add to the attribute data
    :param attribute_name_column: label for attribute name column
    :param attribute_value_column: label for attribute value column
    :returns: Two pandas dataframes, one for Sample and another for Sample_attribute table
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      sample_columns = \
        self.get_table_columns(
          table_name=Sample,
          excluded_columns=['sample_id', 'project_id'])
      sample_columns.extend(['project_igf_id'])
      (sample_df, sample_attr_df)=\
        BaseAdaptor.\
          divide_data_to_table_and_attribute(\
            self,
            data=data,
            required_column=required_column,
            table_columns=sample_columns,
            attribute_name_column=attribute_name_column,
            attribute_value_column=attribute_value_column)
      return (sample_df, sample_attr_df)
    except Exception as e:
      raise ValueError(
              'Failed to divide sample data, error: {0}'.format(e))


  def store_sample_data(self,data,autosave=False):
    '''
    Load data to Sample table

    :param data: A dataframe or list of dictionary containing the data
    :param autosave: A toggle for autocommit, default False
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data=pd.DataFrame(data)                                                 # convert data to dataframe

      if 'project_igf_id' in data.columns:
        project_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Project,
              lookup_column_name='project_igf_id',
              target_column_name='project_id')                                  # prepare the function for project
        data['project_id'] = ''
        data = data.apply(project_map_function,axis=1,result_type=None)         # map project id
        data.drop('project_igf_id',axis=1,inplace=True)
        #data=new_data                                                          # overwrite data
      self.store_records(table=Sample, data=data)                               # store data without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store sample data, error: {0}'.format(e))


  def store_sample_attributes(self,data,sample_id='',autosave=False):
    '''
    A method for storing data to Sample_attribute table

    :param data: A dataframe or list of dictionary containing the Sample_attribute data
    :param sample_id: An optional parameter to link the sample attributes to a specific sample
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert data to dataframe

      if 'sample_igf_id' in data.columns: 
        sample_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Sample,
              lookup_column_name='sample_igf_id',
              target_column_name='sample_id')                                   # prepare the function for sample
        data['sample_id'] = ''
        data = \
          data.apply(
            sample_map_function,
            axis=1,
            result_type=None)                                                   # map sample id
        data.drop('sample_igf_id',axis=1,inplace=True)
        #data=new_data                                                          # overwrite data
      self.store_attributes(
        data=data,
        attribute_table=Sample_attribute,
        linked_column='sample_id',
        db_id=sample_id)                                                        # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store sample attributes, error: {0}'.format(e))


  def fetch_sample_species_name(
        self,sample_igf_id,sample_column_name='sample_igf_id',
        species_column_name='species_name'):
    try:
      sample = \
        self.fetch_sample_records_igf_id(
          sample_igf_id=sample_igf_id,
          target_column_name=sample_column_name)
      species_column = [
        column
          for column in Sample.__table__.columns
            if column.key == species_column_name]
      if(len(species_column))==0:
        raise KeyError(
                'Failed to find species column {0}'.\
                  format(species_column_name))
      species_name = getattr(sample,species_column_name)
      return species_name
    except Exception as e:
      raise ValueError(
              'No species info found for sample: {0},error: {1}'.\
                format(sample_igf_id,e))


  def fetch_sample_records_igf_id(self,sample_igf_id,target_column_name='sample_igf_id'):
    '''
    A method for fetching data for Sample table

    :param sample_igf_id: A sample igf id
    :param output_mode: dataframe, object, one or on_on_none
    :returns: An object or dataframe, based on the output_mode
    '''
    try:
      sample_column = [
        column
          for column in Sample.__table__.columns
            if column.key == target_column_name]
      if len(sample_column)==0:
        raise KeyError('Column {0} not found'.format(target_column_name))
      sample_column = sample_column[0]
      sample = \
        self.fetch_records_by_column(
          table=Sample,
          column_name=sample_column,
          column_id=sample_igf_id,
          output_mode='one')
      return sample
    except Exception as e:
      raise ValueError(
              'Failed to sample igf id, error: {0}'.format(e))


  def check_sample_records_igf_id(self, sample_igf_id,
                                  target_column_name='sample_igf_id'):
    '''
    A method for checking existing data for sample table

    :param sample_igf_id: an igf id
    :param target_column_name: name of the target lookup column, default sample_igf_id
    :returns: True if the file is present in db or False if its not
    '''
    try:
      sample_check=False
      column = [
        column
          for column in Sample.__table__.columns
            if column.key == target_column_name][0]
      sample_obj = \
        self.fetch_records_by_column(
          table=Sample,
          column_name=column,
          column_id=sample_igf_id,
          output_mode='one_or_none')
      if sample_obj is not None:
        sample_check = True
      return sample_check
    except Exception as e:
      raise ValueError(
              'Failed to check sample igf id, error: {0}'.format(e))


  def check_project_and_sample(self,project_igf_id,sample_igf_id):
    '''
    A method for checking existing project and sample igf id combination
    in sample table

    :param project_igf_id: A project igf id string
    :param sample_igf_id: A sample igf id string
    :returns: True if target entry is present or return False
    '''
    try:
      sample_check = False
      query = \
        self.session.\
          query(Sample).\
          join(Project,
               Project.project_id==Sample.project_id).\
          filter(Sample.sample_igf_id==sample_igf_id).\
          filter(Project.project_igf_id==project_igf_id)                        # construct join query
      sample_object = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')                                            # check for existing records
      if sample_object is not None:
        sample_check = True
      return sample_check
    except Exception as e:
      raise ValueError(
              'Failed to check project and sample, error: {0}'.format(e))


  def fetch_sample_project(self, sample_igf_id):
    '''
    A method for fetching project information for the sample

    :param sample_igf_id: A sample_igf_id for database lookup
    :returns: A project_igf_id or None, if not found
    '''
    try:
      query = \
        self.session.\
          query(Project.project_igf_id).\
          join(Sample,
               Project.project_id==Sample.project_id).\
          filter(Project.project_id==Sample.project_id).\
          filter(Sample.sample_igf_id==sample_igf_id)                           # set query
      project = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')                                            # fetch project record
      if project is not None:
        project = project.project_igf_id

      return project
    except Exception as e:
      raise ValueError('Failed to sample project, error: {0}'.format(e))


  def fetch_seqrun_and_platform_list_for_sample_id(self,sample_igf_id,output_mode='dataframe'):
    '''
    A method for fetching seqrn and platform information for a sample

    :param sample_igf_id: Sample igf id
    :param output_mode: Output format, default 'dataframe'
    :returns: A object or dataframe
    '''
    try:
      query = \
        self.session.\
          query(
            Seqrun.seqrun_igf_id,
            Platform.platform_igf_id,
            Platform.model_name).\
          join(Platform,Seqrun.platform_id==Platform.platform_id).\
          join(Run,Run.seqrun_id==Seqrun.seqrun_id).\
          join(Experiment,Experiment.experiment_id==Run.experiment_id).\
          join(Sample,Sample.sample_id==Experiment.sample_id).\
          filter(Sample.sample_igf_id==sample_igf_id)
      records = \
        self.fetch_records(
          query=query,
          output_mode=output_mode)
      return records
    except Exception as e:
      raise ValueError(
              'Failed to retrieve seqrun and platform list for sample {0}, error: {1}'.\
                format(sample_igf_id,e))