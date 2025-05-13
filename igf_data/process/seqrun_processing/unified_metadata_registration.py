import os, json
import pandas as pd
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from igf_portal.api_utils import get_data_from_portal
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir,
  check_file_path)

class MetadataContext:
  def __init__(
    self,
    portal_config_file: str,
    fetch_metadata_url_suffix: str,
    sync_metadata_url_suffix: str,
    metadata_fetched: Optional[bool] = False,
    metadata_validated: Optional[bool] = False,
    metadata_added: Optional[bool] = False,
    metadata_synced: Optional[bool] = False,
    samples_required: Optional[bool] = False,
    raw_metadata_dict: Dict[str, list] = {},
    checked_required_column_dict: Dict[str, bool] = {},
    table_columns: Dict[str, list] = {
      "project": ["project_igf_id", "deliverable"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id",]},
    project_metadata_list: List[list] = [],
    sample_metadata_list: List[list] = [],
    user_metadata_list: List[list] = [],
    valid_metadata_ids: List[list] = [],
    registered_metadata_ids: List[list] = [],
    portal_metadata_synced_ids: List[list] = [],
    error_list: List[str] = []) \
      -> None:
    self.portal_config_file = portal_config_file
    self.fetch_metadata_url_suffix = fetch_metadata_url_suffix
    self.sync_metadata_url_suffix = sync_metadata_url_suffix
    self.checked_required_column_dict = checked_required_column_dict
    self.metadata_fetched = metadata_fetched
    self.metadata_validated = metadata_validated
    self.metadata_added = metadata_added
    self.metadata_synced = metadata_synced
    self.samples_required = samples_required
    self.raw_metadata_dict = raw_metadata_dict
    self.table_columns = table_columns
    self.project_metadata_list = project_metadata_list
    self.sample_metadata_list = sample_metadata_list
    self.user_metadata_list = user_metadata_list
    self.valid_metadata_ids = valid_metadata_ids
    self.registered_metadata_ids = registered_metadata_ids
    self.portal_metadata_synced_ids = portal_metadata_synced_ids
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
        f"Failed to validate and check metadata: {e}")


class AddNewMetadataCommand(BaseCommand):
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Add new metadata to the contest.
    """
    # Implementation to add new metadata
    pass

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
      AddNewMetadataCommand(),
      SyncMetadataCommand()]
    self.chain_command = ChainCommand(self.commands)

  def execute(self) -> None:
    """
    Execute the unified metadata registration process.
    """
    self.chain_command.execute(self.metadata_contest)
