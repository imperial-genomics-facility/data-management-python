import os, json
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple
from abc import ABC, abstractmethod
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_portal.api_utils import get_data_from_portal
from igf_data.igfdb.igfTables import (
  Project,
  User,
  Sample,
  ProjectUser)
from jsonschema import (
  Draft4Validator,
  ValidationError)
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir,
  read_json_data,
  check_file_path)

def _get_db_session_class(db_config_file: str) -> Any:
  try:
    check_file_path(db_config_file)
    dbparams = read_dbconf_json(db_config_file)
    base = BaseAdaptor(**dbparams)
    return base.get_session_class()
  except Exception as e:
    raise ValueError(
      f"Failed to get db session class: {e}")


class MetadataContext:
  def __init__(
    self,
    portal_config_file: str,
    fetch_metadata_url_suffix: str,
    sync_metadata_url_suffix: str,
    metadata_validation_schema: str,
    db_config_file: str,
    default_project_user_email: Optional[str] = None,
    metadata_fetched: Optional[bool] = False,
    samples_required: Optional[bool] = False,
    raw_metadata_dict: Dict[str, list] = {},
    checked_required_column_dict: Dict[str, bool] = {},
    validated_metadata_dict: Dict[str, bool] = {},
    table_columns: Dict[str, list] = {
      "project": ["project_igf_id", "deliverable"],
      "project_user": ["project_igf_id", "email_id"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id", "project_igf_id"]},
    error_list: List[str] = []) \
      -> None:
    self.portal_config_file = portal_config_file
    self.fetch_metadata_url_suffix = fetch_metadata_url_suffix
    self.sync_metadata_url_suffix = sync_metadata_url_suffix
    self.checked_required_column_dict = checked_required_column_dict
    self.validated_metadata_dict = validated_metadata_dict
    self.metadata_fetched = metadata_fetched
    self.metadata_validation_schema = metadata_validation_schema
    self.session_class = _get_db_session_class(db_config_file)
    self.samples_required = samples_required
    self.raw_metadata_dict = raw_metadata_dict
    self.table_columns = table_columns
    self.default_project_user_email = default_project_user_email
    self.error_list = error_list


class BaseCommand(ABC):
  @abstractmethod
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Execute the method on the metadata contest.
    """
    pass

class FetchNewMetadataCommand(BaseCommand):
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Fetch new metadata from the database and update the metadata contest.
    """
    try:
      portal_config_file = metadata_context.portal_config_file
      fetch_metadata_url_suffix = metadata_context.fetch_metadata_url_suffix
      new_project_data_dict = \
        get_data_from_portal(
          url_suffix=fetch_metadata_url_suffix,
          portal_config_file=portal_config_file)
      if len(new_project_data_dict) > 0:
        metadata_context.raw_metadata_dict = new_project_data_dict
        metadata_context.metadata_fetched = True
    except Exception as e:
      raise ValueError(
        f"Failed to fetch new metadata from portal: {e}")


class CheckRawMetadataColumnsCommand(BaseCommand):
  """
  Check if the raw metadata has all the required columns.
  """
  @staticmethod
  def _check_columns(
    table_columns_dict: Dict[str, list],
    metadata_columns: List[str],
    sample_required: bool,
    sample_column_name: str = 'sample') -> List[str]:
    """
    Match the columns in the metadata with the required columns.

    :param table_columns_dict: Dictionary of table names and their required columns
    :param metadata_columns: List of columns in the metadata
    :param sample_required: Boolean indicating if sample columns are required
    :param sample_column_name: Name of the sample column
    :return: List of error messages if any required columns are missing
    """
    try:
      error_list = []
      for table_name, table_column_list in table_columns_dict.items():
        matched_columns = \
          len(set(table_column_list).intersection(set(metadata_columns))) == len(set(table_column_list))
        if not matched_columns:
          if table_name != sample_column_name:
            error_list.append(
              f"Missing required columns in {table_name} table: {table_column_list}")
          if table_name == sample_column_name and sample_required:
            error_list.append(
              f"Missing required columns in {table_name} table: {table_column_list}")
      return error_list
    except Exception as e:
      raise ValueError(
        f"Failed to check columns in metadata: {e}")

  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Run the command to check if the raw metadata has all the required columns.

    :param metadata_context: MetadataContext object containing the metadata
    :return: None
    """
    try:
      checked_required_column_dict = {}
      error_list = []
      if metadata_context.metadata_fetched:
        raw_meatadata_dict = metadata_context.raw_metadata_dict
        for metadata_id, metadata_list in raw_meatadata_dict.items():
          metadata_df = pd.DataFrame(metadata_list)
          metadata_df_columns = metadata_df.columns
          column_check_errors = \
            self._check_columns(
              table_columns_dict=metadata_context.table_columns,
              metadata_columns=metadata_df_columns,
              sample_required=metadata_context.samples_required)
          if len(column_check_errors) > 0:
            checked_required_column_dict[metadata_id] = False
            error_list.append(column_check_errors)
          else:
            checked_required_column_dict[metadata_id] = True
        metadata_context.error_list.extend(error_list)
        metadata_context.checked_required_column_dict = \
          checked_required_column_dict
    except Exception as e:
      raise ValueError(
        f"Failed to check metadata fields: {e}")


class ValidateMetadataCommand(BaseCommand):
  @staticmethod
  def _validate_metadata(
    metadata_id: int,
    metadata_entry: List[Dict[str, Any]],
    metadata_validation_schema: str) -> bool:
    """
    Validate the metadata against the schema.

    :param metadata_id: ID of the metadata
    :param metadata_list: List of metadata entries
    :param metadata_validation_schema: Path to the validation schema
    :return: A list of error messages if any validation errors are found
    """
    try:
      error_list = []
      schema = \
        read_json_data(
          metadata_validation_schema)
      schema = \
        schema[0]
      metadata_validator = \
        Draft4Validator(schema)
      json_data = \
        pd.DataFrame(metadata_entry).\
          fillna('').\
          to_dict(orient='records')
      validation_errors = \
        sorted(
          metadata_validator.iter_errors(json_data),
          key=lambda e: e.path)
      for err in validation_errors:
        if isinstance(err, str):
          error_list.append(err)
        else:
          if len(err.schema_path) > 2:
            error_list.append(
              f"{err.schema_path[2]}: {err.message}")
          else:
            error_list.append(
              f"{err.message}")
      return error_list
    except Exception as e:
      raise ValueError(
        f"Failed to validate metadata {metadata_id}: {e}")

  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Add new metadata to the contest.
    """
    try:
      error_list = []
      validated_metadata_dict = {}
      checked_required_column_dict = \
        metadata_context.checked_required_column_dict
      raw_meatadata_dict = \
        metadata_context.raw_metadata_dict
      metadata_validation_schema = \
        metadata_context.metadata_validation_schema
      for metadata_id, validation_status in checked_required_column_dict.items():
        if validation_status:
          metadata_entry = \
            raw_meatadata_dict.get(metadata_id)
          if not metadata_entry:
            raise ValueError(
              f"Metadata entry not found for ID: {metadata_id}")
          validation_error = \
            self._validate_metadata(
              metadata_id=metadata_id,
              metadata_entry=metadata_entry,
              metadata_validation_schema=metadata_validation_schema)
          if len(validation_error) > 0:
            error_list.append(validation_error)
            validated_metadata_dict[metadata_id] = False
          else:
            validated_metadata_dict[metadata_id] = True
      metadata_context.validated_metadata_dict = validated_metadata_dict
      metadata_context.error_list.extend(error_list)
    except Exception as e:
      raise ValueError(
        f"Failed to validate metadata: {e}")


class CheckAndRegisterMetadataCommand(BaseCommand):
  def _split_metadata(
    self,
    metadata_entry: List[Dict[str, str]],
    table_columns: Dict[str, list],
    samples_required: bool) \
      -> Tuple[List[str], List[str], List[str], List[str]]:
    """
    Split the metadata into different tables.
    """
    try:
      project_metadata_list = []
      sample_metadata_list = []
      user_metadata_list = []
      project_user_metadata = []
      metadata_df = pd.DataFrame(metadata_entry)
      metadata_df = \
        metadata_df.fillna('').\
        drop_duplicates()
      for table_name, table_column_list in table_columns.items():
        if table_name == 'project':
          project_metadata_list.extend(
            metadata_df[table_column_list].to_dict(orient='records'))
        elif table_name == 'sample' and samples_required:
          sample_metadata_list.extend(
            metadata_df[table_column_list].to_dict(orient='records'))
        elif table_name == 'user':
          user_metadata_list.extend(
            metadata_df[table_column_list].to_dict(orient='records'))
        elif table_name == 'project_user':
          project_user_metadata.extend(
            metadata_df[table_column_list].to_dict(orient='records'))
      return (
        project_metadata_list,
        user_metadata_list,
        project_user_metadata,
        sample_metadata_list)
    except Exception as e:
      raise ValueError(
        f"Failed to check existing metadata: {e}")

  def _check_and_filter_existing_metadata(
    self,
    project_metadata_list: List[Dict[str, str]],
    user_metadata_list: List[Dict[str, str]],
    project_user_metadata_list: List[Dict[str, str]],
    sample_metadata_list: List[Dict[str, str]],
    session_class: Any) \
      -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]], List[Dict[str, str]]]:
    try:
      # get unique project ids
      project_igf_ids = \
        list(set([metadata['project_igf_id'] for metadata in project_metadata_list]))
      # get unique user names
      user_emails = \
        list(set([metadata['email_id'] for metadata in user_metadata_list]))
      # get unique sample ids
      sample_igf_ids = \
        list(set([metadata['sample_igf_id'] for metadata in sample_metadata_list]))
      ## 
      base = \
        BaseAdaptor(**{'session_class': session_class})
      base.start_session()
      project_query = \
        base.session.\
          query(Project.project_igf_id).\
          filter(Project.project_igf_id.in_(project_igf_ids))
      project_records = \
        base.fetch_records(
          query=project_query,
          output_mode='object')
      user_email_query = \
        base.session.\
          query(User.email_id).\
          filter(User.email_id.in_(user_emails))
      user_email_records = \
        base.fetch_records(
          query=user_email_query,
          output_mode='object')
      if len(sample_igf_ids) > 0:
        sample_query = \
          base.session.\
            query(Sample.sample_igf_id).\
            filter(Sample.sample_igf_id.in_(sample_igf_ids))
        sample_records = \
          base.fetch_records(
            query=sample_query,
            output_mode='object')
      base.close_session()
      # filter out existing project metadata
      existing_project_igf_ids = \
        [record.project_igf_id for record in project_records]
      filtered_project_metadata = \
        [metadata for metadata in project_metadata_list \
          if metadata['project_igf_id'] not in existing_project_igf_ids]
      # filter out existing user metadata
      existing_user_emails = \
        [record.email_id for record in user_email_records]
      filtered_user_metadata = \
        [metadata for metadata in user_metadata_list \
          if metadata['email_id'] not in existing_user_emails]
      # filter out existing sample metadata
      filtered_sample_metadata = []
      if len(sample_igf_ids) > 0:
        existing_sample_igf_ids = \
          [record.sample_igf_id for record in sample_records]
        filtered_sample_metadata = \
          [metadata for metadata in sample_metadata_list \
            if metadata['sample_igf_id'] not in existing_sample_igf_ids]
      # filter out existing project user metadata
      filtered_project_user_metadata = \
        [metadata for metadata in project_user_metadata_list \
          if metadata['project_igf_id'] not in existing_project_igf_ids]
      return (
        filtered_project_metadata,
        filtered_user_metadata,
        filtered_project_user_metadata,
        filtered_sample_metadata)
    except Exception as e:
      raise ValueError(
        f"Failed to check existing metadata: {e}")

  def execute(self, metadata_context: MetadataContext) -> None:
    try:
      validated_metadata_dict = \
        metadata_context.validated_metadata_dict
      raw_meatadata_dict = \
        metadata_context.raw_metadata_dict
      table_columns = \
        metadata_context.table_columns
      samples_required = \
        metadata_context.samples_required
      for metadata_id, validation_status in validated_metadata_dict.items():
        if validation_status:
          metadata_entry = \
            raw_meatadata_dict.get(metadata_id)
          if not metadata_entry:
            raise ValueError(
              f"Metadata entry not found for ID: {metadata_id}")
          (project_metadata,
           user_metadata,
           project_user_metadata,
           sample_metadata) = \
            self._split_metadata(
              metadata_entry=metadata_entry,
              table_columns=table_columns,
              samples_required=samples_required)
          (filtered_project_metadata,
           filtered_user_metadata,
           filtered_project_user_metadata,
           filtered_sample_metadata) = \
            self._check_and_filter_existing_metadata(
              project_metadata_list=project_metadata,
              user_metadata_list=user_metadata,
              project_user_metadata_list=project_user_metadata,
              sample_metadata_list=sample_metadata,
              session_class=metadata_context.session_class)
    except Exception as e:
      raise ValueError(
        f"Failed to split metadata: {e}")


class SyncMetadataCommand(BaseCommand):
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Sync metadata with the portal.
    """
    # Implementation to sync metadata
    pass

class ChainCommand:
  def __init__(self, commands: List[BaseCommand]) -> None:
    self.commands = commands

  def execute(self, metadata_context: MetadataContext) -> None: 
    try:
      for command in self.commands:
        command.execute(metadata_context)
    except Exception as e:
      raise

class UnifiedMetadataRegistration:
  def __init__(
    self,
    portal_config_file: str,
    samples_required: bool = False,
    ) -> None:
    self.metadata_context = MetadataContext(
      portal_config_file=portal_config_file,
      samples_required=samples_required)
    self.commands = [
      FetchNewMetadataCommand(),
      CheckRawMetadataColumnsCommand(),
      ValidateMetadataCommand(),
      CheckAndRegisterMetadataCommand(),
      SyncMetadataCommand()]
    self.chain_command = ChainCommand(self.commands)

  def execute(self) -> None:
    """
    Execute the unified metadata registration process.
    """
    self.chain_command.execute(self.metadata_contest)
