from typing import List, Dict, Any
from abc import ABC, abstractmethod

class MetadataContext:
  def __init__(
    self,
    portal_config_file: str,
    samples_required: bool = False,
    raw_metadata_list: List[list] = [],
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
    error_list: List[list] = []) \
      -> None:
    self.portal_config_file = portal_config_file
    self.samples_required = samples_required
    self.raw_metadata_list = raw_metadata_list
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
    # Implementation to fetch new metadata
    pass

class ValidateAndCheckMetadataCommand(BaseCommand):
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Validate and check the metadata in the contest.
    """
    # Implementation to validate and check metadata
    pass

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
      ValidateAndCheckMetadataCommand(),
      AddNewMetadataCommand(),
      SyncMetadataCommand()]
    self.chain_command = ChainCommand(self.commands)

  def execute(self) -> None:
    """
    Execute the unified metadata registration process.
    """
    self.chain_command.execute(self.metadata_contest)
