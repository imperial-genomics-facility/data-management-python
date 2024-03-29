import pandas as pd
from typing import List, Dict, Any, Tuple, Union, Optional
from sqlalchemy.sql import column
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Seqrun, Run, Platform, Seqrun_attribute, Seqrun_stats, Flowcell_barcode_rule

class SeqrunAdaptor(BaseAdaptor):
  '''
  An adaptor class for table Seqrun
  '''

  def store_seqrun_and_attribute_data(self,data,autosave=True):
    '''
    A method for dividing and storing data to seqrun and attribute table

    :param data: A list of dictionary or a Pandas dataframe containing Seqrun data
    :param autosave: A toggle for auto commit, default True
    :returns: None
    '''
    (seqrun_data, seqrun_attr_data) = \
      self.divide_data_to_table_and_attribute(data=data)

    try:
      self.store_seqrun_data(data=seqrun_data)                                                # store run
      if len(seqrun_attr_data.index)>0:                                                     # check if any attribute exists
        self.store_seqrun_attributes(data=seqrun_attr_data)                                   # store run attributes

      if autosave:
        self.commit_session()                                                                 # save changes to database
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store seqrun data, error: {0}'.format(e))


  def divide_data_to_table_and_attribute(
        self,data,required_column='seqrun_igf_id',
        table_columns=None,attribute_name_column='attribute_name',
        attribute_value_column='attribute_value'):
    '''
    A method for separating data for Seqrun and Seqrun_attribute tables
    
    :param data: A list of dictionaries or a pandas dataframe
    :param table_columns: List of table column names, default None
    :param required_column: column name to add to the attribute data
    :param attribute_name_column: label for attribute name column
    :param attribute_value_column: label for attribute value column
    :returns: two pandas dataframes, one for Seqrun and another for Run_attribute table
    '''
    if not isinstance(data, pd.DataFrame):
      data = pd.DataFrame(data)

    seqrun_columns = \
      self.get_table_columns(
        table_name=Seqrun,
        excluded_columns=['seqrun_id', 'platform_id'])                          # get required columns for run table
    seqrun_columns.\
      extend(['platform_igf_id'])
    (seqrun_df, seqrun_attr_df) = \
      BaseAdaptor.\
        divide_data_to_table_and_attribute(
          self,
          data=data,
          required_column=required_column,
          table_columns=seqrun_columns,
          attribute_name_column=attribute_name_column,
          attribute_value_column=attribute_value_column)                        # divide data to run and attribute table
    return (seqrun_df, seqrun_attr_df)


  def store_seqrun_data(self,data,autosave=False):
    '''
    Load data to Seqrun table

    :param data: A list of dictionary or a Pandas dataframe containing Seqrun data
    :param autosave: A toggle for auto commit, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)

      if 'platform_igf_id' in data.columns:
        platform_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Platform,
              lookup_column_name='platform_igf_id',
              target_column_name='platform_id')                                 # prepare the function for Platform id
        data['platform_id'] = ''
        data = \
          data.apply(
            platform_map_function,
            axis=1,
            result_type=None)                                                   # map platform id foreign key id
        data.drop(
          'platform_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                          # overwrite data

      self.store_records(
        table=Seqrun,
        data=data)                                                              # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store seqrun data, error: {0}'.format(e))


  def store_seqrun_attributes(self,data,seqrun_id='',autosave=False):
    '''
    A method for storing data to Seqrun_attribute table

    :param data: A list of dictionary or a Pandas dataframe containing Seqrun attribute data
    :param autosave: A toggle for auto commit, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                                             # convert data to dataframe

      if 'seqrun_igf_id' in data.columns:
        seqrun_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Seqrun,
              lookup_column_name='seqrun_igf_id',
              target_column_name='seqrun_id')                                   # prepare run mapping function
        data['seqrun_id'] = ''
        data = \
          data.apply(
            seqrun_map_function,
            axis=1,
            result_type=None)
        data.drop(
          'seqrun_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                          # overwrite data    

      self.store_attributes(
        attribute_table=Seqrun_attribute,
        linked_column='seqrun_id',
        db_id=seqrun_id,
        data=data)                                                              # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store seqrun attribute, error: {0}'.format(e))


  def store_seqrun_stats_data(self,data,seqrun_id='',autosave=True):
    '''
    A method for storing data to seqrun_stats table

    :param data: A list of dictionary or a Pandas dataframe containing Seqrun stats data
    :param seqrun_id: Seqrun id info, default ''
    :param autosave: A toggle for auto commit, default True
    :returns: None
    '''
    try:
      if not isinstance(data, pd.DataFrame):
        data = pd.DataFrame(data)                                               # convert data to dataframe

      if 'seqrun_igf_id' in data.columns:
        seqrun_map_function = \
          lambda x: \
            self.map_foreign_table_and_store_attribute(
              data=x,
              lookup_table=Seqrun,
              lookup_column_name='seqrun_igf_id',
              target_column_name='seqrun_id')                                   # prepare run mapping function
        data['seqrun_id'] = ''
        data = \
          data.apply(
            seqrun_map_function,
            axis=1,
            result_type=None)
        data.drop(
          'seqrun_igf_id',
          axis=1,
          inplace=True)
        #data=new_data                                                          # overwrite data    

      self.store_attributes(
        attribute_table=Seqrun_stats,
        linked_column='seqrun_id',
        db_id=seqrun_id,
        data=data)                                                              # store without autocommit
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
              'Failed to store seqrun stats data, error: {0}'.format(e))


  def get_flowcell_id_for_seqrun_id(
        self,
        seqrun_igf_id: str) \
          -> Union[None, str]:
    try:
      query = \
        self.session.\
          query(Seqrun.flowcell_id).\
          filter(Seqrun.seqrun_igf_id == seqrun_igf_id)
      seqrun_record = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if seqrun_record is None:
        return None
      else:
        return seqrun_record[0]
    except Exception as e:
      raise ValueError(
        f'Failed to get flowcell id for seqrun id, error: {e}')


  def check_seqrun_exists(self,seqrun_id):
    '''
    A method for checking if seqrun exists

    :param seqrun_id: Seqrun id
    :returns: True if seqrun exists, False otherwise
    '''
    try:
      seqrun_entry = \
        self.fetch_records_by_column(
          table=Seqrun,
          column_name=Seqrun.seqrun_igf_id,
          column_id=seqrun_id,
          output_mode='one_or_none')
      if seqrun_entry is not None:
        return True
      else:
        return False
    except Exception as e:
      raise ValueError(
              'Failed to check seqrun exists, error: {0}'.format(e))

  def fetch_seqrun_records_igf_id(
        self,seqrun_igf_id,target_column_name='seqrun_igf_id'):
    '''
    A method for fetching data for Seqrun table

    :param seqrun_igf_id: an igf id
    :param target_column_name: a column name in the Seqrun table, default seqrun_igf_id
    :returns: Seqrun record as oblect
    '''
    try:
      column = [
        column
          for column in Seqrun.__table__.columns \
            if column.key == target_column_name][0]
      seqrun = \
        self.fetch_records_by_column(
          table=Seqrun,
          column_name=column,
          column_id=seqrun_igf_id,
          output_mode='one')
      return seqrun
    except Exception as e:
      raise ValueError(
              'Failed to fetch seqrun if, error: {0}'.format(e))

  def fetch_platform_info_for_seqrun(self,seqrun_igf_id):
    '''
    A method for fetching platform info for seqrun

    :param seqrun_igf_id: Seqrun igf id for platform name lookup
    :returns: A string containing platform name or None
    '''
    try:
      query = \
        self.session.\
        query(Platform.model_name).\
        join(Seqrun,Seqrun.platform_id==Platform.platform_id).\
        filter(Seqrun.seqrun_igf_id==seqrun_igf_id)
      platform_name = \
        self.fetch_records(query=query, output_mode='one_or_none')
      if platform_name is not None:
        platform_name = platform_name[0]
      return platform_name
    except Exception as e:
      raise ValueError('Failed to fetch platform info for seqrun {0}'.\
              format(seqrun_igf_id))

  def fetch_flowcell_barcode_rules_for_seqrun(
        self,seqrun_igf_id,flowcell_label='flowcell',output_mode='dataframe'):
    '''
    A method for fetching flowcell barcode rule for Seqrun

    :param seqrun_igf_id: A seqrun igf id
    :param flowcell_label: Flowcell label, default 'flowcell'
    :param output_mode: Query output mode, default 'dataframe'
    :returns: Flowcell rules records
    '''
    try:
      rules_query = \
        self.session.\
          query(
            Seqrun.seqrun_igf_id,
            Seqrun_attribute.attribute_value,
            Platform.platform_igf_id,
            Flowcell_barcode_rule.index_1,
            Flowcell_barcode_rule.index_2).\
          join(Seqrun_attribute,
               Seqrun.seqrun_id==Seqrun_attribute.seqrun_id).\
          join(Platform,
               Platform.platform_id==Seqrun.platform_id).\
          join(Flowcell_barcode_rule,
               Flowcell_barcode_rule.platform_id==Platform.platform_id).\
          filter(Seqrun_attribute.attribute_name==flowcell_label).\
          filter(Seqrun_attribute.attribute_value==Flowcell_barcode_rule.flowcell_type).\
          filter(Seqrun.seqrun_igf_id==seqrun_igf_id)
      rules_data = \
        self.fetch_records(
          query=rules_query,
          output_mode=output_mode)
      return rules_data
    except Exception as e:
      raise ValueError(
              'Failed to fetch barcode rules for seqrun, error: {0}'.format(e))


  def check_seqrun_attribute_exists(
        self,
        seqrun_igf_id: str,
        attribute_name: str) -> bool:
    try:
      query = \
        self.session.\
          query(
            Seqrun.seqrun_igf_id,
            Seqrun_attribute.attribute_name).\
          join(Seqrun_attribute,
               Seqrun.seqrun_id==Seqrun_attribute.seqrun_id).\
          filter(Seqrun_attribute.attribute_name==attribute_name).\
          filter(Seqrun.seqrun_igf_id==seqrun_igf_id)
      attribute_record = \
        self.fetch_records(
          query=query,
          output_mode='one_or_none')
      if attribute_record is None:
        return False
      else:
        return True
    except Exception as e:
      raise ValueError(
        f"Failed to check seqrun attribute, error: {e}")


  def create_or_update_seqrun_attribute_records(
        self,
        seqrun_igf_id: str,
        attribute_list: list[dict],
        autosave: bool = False) -> None:
    try:
      seqrun_exists = \
        self.check_seqrun_exists(
          seqrun_id=seqrun_igf_id)
      if not seqrun_exists:
        raise KeyError(f"Seqrun {seqrun_igf_id} is not in DB!")
      seqrun = \
        self.fetch_seqrun_records_igf_id(seqrun_igf_id=seqrun_igf_id)
      seqrun_id = seqrun.seqrun_id
      for entry in attribute_list:
        attribute_name = entry.get('attribute_name')
        attribute_value = entry.get('attribute_value')
        if attribute_name is None or \
           attribute_value is None:
          raise KeyError(
            f"attribute_name or attribute_value key is missing in attribute_list data")
        attribute_exists = \
          self.check_seqrun_attribute_exists(
            seqrun_igf_id=seqrun_igf_id,
            attribute_name=attribute_name)
        if attribute_exists:
          # update attribute
          self.session.\
          query(Seqrun_attribute).\
          filter(Seqrun_attribute.seqrun_id==seqrun_id).\
          filter(Seqrun_attribute.attribute_name==attribute_name).\
          update({'attribute_value': attribute_value})
        else:
          # create attribute
          attribute_data = [{
            'attribute_name': attribute_name,
            'attribute_value': attribute_value}]
          self.store_seqrun_attributes(
            data=attribute_data,
            seqrun_id=seqrun_id)
      if autosave:
        self.commit_session()
    except Exception as e:
      if autosave:
        self.rollback_session()
      raise ValueError(
        f"Failed to create attribute record, error {e}")