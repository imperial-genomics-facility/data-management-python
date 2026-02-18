import os, json, time
from io import StringIO
from urllib.parse import urljoin
import pandas as pd
from typing import Any
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
  Sample
)
from jsonschema import (
  Draft4Validator
)
from igf_data.utils.fileutils import (
  read_json_data,
  check_file_path
)

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
    raw_cosmx_metadata_id: int,
    portal_config_file: str,
    fetch_metadata_url_suffix: str,
    sync_metadata_url_suffix: str,
    metadata_validation_schema: str,
    db_config_file: str,
    default_project_user_email: str,
    metadata_fetched: bool = False,
    samples_required: bool = False,
    fetched_metadata_dict: dict[str, int] = {},
    raw_metadata_dict: dict[int, list[Any]] = {},
    checked_required_column_dict: dict[int, bool] = {},
    validated_metadata_dict: dict[int, bool] = {},
    registered_metadata_dict: dict[int, bool] = {},
    synced_metadata_dict: dict[int, bool] = {},
    table_columns: dict[str, list[Any]] = {
      "project": ["project_igf_id", "deliverable"],
      "project_user": ["project_igf_id", "email_id"],
      "user": ["name", "email_id", "username", "category"],
      "sample": ["sample_igf_id", "project_igf_id"]},
    error_list: list[str] = []
    ) -> None:
    self.raw_cosmx_metadata_id = raw_cosmx_metadata_id
    self.portal_config_file = portal_config_file
    self.fetch_metadata_url_suffix = fetch_metadata_url_suffix
    self.sync_metadata_url_suffix = sync_metadata_url_suffix
    self.checked_required_column_dict = checked_required_column_dict
    self.validated_metadata_dict = validated_metadata_dict
    self.registered_metadata_dict = registered_metadata_dict
    self.fetched_metadata_dict = fetched_metadata_dict
    self.synced_metadata_dict = synced_metadata_dict
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
      fetch_url = urljoin(
        fetch_metadata_url_suffix,
        str(metadata_context.raw_cosmx_metadata_id)
      )
      new_project_data_dict = get_data_from_portal(
        url_suffix=fetch_url,
        portal_config_file=portal_config_file
      )
      if len(new_project_data_dict) > 0:
        metadata_context.raw_metadata_dict = new_project_data_dict
        metadata_context.metadata_fetched = True
        fetched_projects = list(new_project_data_dict.keys())
        metadata_context.fetched_metadata_dict = {
          fetched_projects[0]: metadata_context.raw_cosmx_metadata_id}
      else:
        metadata_context.metadata_fetched = False
        raise ValueError(
          "No metadata found on portal for id " +
          str(metadata_context.raw_cosmx_metadata_id)
        )
    except Exception as e:
      raise ValueError(
        f"Failed to fetch new metadata from portal: {e}"
      )


class CheckRawMetadataColumnsCommand(BaseCommand):
  """
  Check if the raw metadata has all the required columns.
  """
  @staticmethod
  def _check_columns(
    table_columns_dict: dict[str, list],
    metadata_columns: list[str],
    sample_required: bool,
    sample_column_name: str = 'sample') -> list[str]:
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
        matched_columns = len(
          set(table_column_list)
          .intersection(
            set(metadata_columns))
          ) == len(set(table_column_list))
        if not matched_columns:
          if table_name != sample_column_name:
            error_list.append(
              f"Missing required columns in {table_name} table: " +
              ",".join(table_column_list)
            )
          if table_name == sample_column_name and sample_required:
            error_list.append(
              f"Missing required columns in {table_name} table: " +
              ",".join(table_column_list)
            )
      return error_list
    except Exception as e:
      raise ValueError(
        f"Failed to check columns in metadata: {e}"
      )

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
        for metadata_id, metadata_csv in raw_meatadata_dict.items():
          csvStringIO = StringIO(metadata_csv)
          metadata_df = pd.read_csv(csvStringIO, header=0)
          metadata_df_columns = metadata_df.columns.tolist()
          column_check_errors = self._check_columns(
            table_columns_dict=metadata_context.table_columns,
            metadata_columns=metadata_df_columns,
            sample_required=metadata_context.samples_required
          )
          if len(column_check_errors) > 0:
            checked_required_column_dict[metadata_id] = False
            error_list.extend(column_check_errors)
            raise ValueError(
              "Failed required column checks, errors: " +
              ",".join(column_check_errors)
            )
          else:
            checked_required_column_dict[metadata_id] = True
        metadata_context.error_list.extend(error_list)
        metadata_context.checked_required_column_dict = \
          checked_required_column_dict
    except Exception as e:
      raise ValueError(
        f"Failed to check metadata fields: {e}"
      )


class ValidateMetadataCommand(BaseCommand):
  @staticmethod
  def _validate_metadata(
    metadata_id: str,
    metadata_entry: str,
    metadata_validation_schema: str
  ) -> list[str]:
    """
    Validate the metadata against the schema.

    :param metadata_id: ID of the metadata
    :param metadata_list: List of metadata entries
    :param metadata_validation_schema: Path to the validation schema
    :return: A list of error messages if any validation errors are found
    """
    try:
      error_list = []
      schema = read_json_data(
        metadata_validation_schema
      )
      schema = schema[0]
      metadata_validator = Draft4Validator(schema)
      csvStringIO = StringIO(metadata_entry)
      json_data = (
        pd.read_csv(csvStringIO, header=0)
        .fillna('')
        .to_dict(orient='records')
      )
      validation_errors = sorted(
        metadata_validator.iter_errors(json_data),
        key=lambda e: e.path
      )
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
        f"Failed to validate metadata {metadata_id}: {e}"
      )

  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Add new metadata to the contest.
    """
    try:
      error_list = []
      validated_metadata_dict = {}
      checked_required = metadata_context.checked_required_column_dict
      raw_meatadata_dict = metadata_context.raw_metadata_dict
      validation_schema = metadata_context.metadata_validation_schema
      for metadata_id, validation_status in checked_required.items():
        if validation_status:
          metadata_entry = raw_meatadata_dict.get(metadata_id)
          if not metadata_entry:
            raise ValueError(
              f"Metadata entry not found for ID: {metadata_id}")
          validation_error = self._validate_metadata(
            metadata_id=metadata_id,
            metadata_entry=metadata_entry,
            metadata_validation_schema=validation_schema
          )
          if len(validation_error) > 0:
            error_list.append(validation_error)
            validated_metadata_dict[metadata_id] = False
            raise ValueError(
              f"Metadata validation failed, errors: {validation_error}")
          else:
            validated_metadata_dict[metadata_id] = True
        else:
          validated_metadata_dict[metadata_id] = False
      metadata_context.validated_metadata_dict = validated_metadata_dict
      metadata_context.error_list.extend(error_list)
    except Exception as e:
      raise ValueError(
        f"Failed to validate metadata: {e}"
      )


class CheckAndRegisterMetadataCommand(BaseCommand):
  @staticmethod
  def _split_metadata(
    metadata_entry: list[dict[str, str]],
    table_columns: dict[str, list],
    samples_required: bool
    ) -> tuple[
      list[dict[str, str]],
      list[dict[str, str]],
      list[dict[str, str]],
      list[dict[str, str]]
    ]:
    """
    Split the metadata into different tables.
    """
    try:
      project_metadata_list = []
      sample_metadata_list = []
      user_metadata_list = []
      project_user_metadata = []
      csvStringIO = StringIO(metadata_entry)
      metadata_df = (
        pd.read_csv(csvStringIO, header=0)
        .fillna('')
        .drop_duplicates()
      )
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
        f"Failed to check existing metadata: {e}"
      )

  @staticmethod
  def _check_and_filter_existing_metadata(
    project_metadata_list: list[dict[str, str]],
    user_metadata_list: list[dict[str, str]],
    project_user_metadata_list: list[dict[str, str]],
    sample_metadata_list: list[dict[str, str]],
    session_class: Any
  ) -> tuple[
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[dict[str, str]],
    list[str]
  ]:
    try:
      errors = list()
      # get unique project ids
      project_igf_ids = list(
        set([
          metadata['project_igf_id']
          for metadata in project_metadata_list
        ])
      )
      # get unique user names
      user_emails = list(
        set([
          metadata['email_id']
          for metadata in user_metadata_list
        ])
      )
      # get unique user usernames
      user_usernames = list(
        set([
          metadata['username']
          for metadata in user_metadata_list
        ])
      )
      # get unique sample ids
      sample_igf_ids = list(
        set([
          metadata['sample_igf_id']
          for metadata in sample_metadata_list
        ])
      )
      ## 
      base = BaseAdaptor(
        **{'session_class': session_class}
      )
      base.start_session()
      project_query = (
        base.session
        .query(Project)
        .filter(Project.project_igf_id.in_(project_igf_ids))
      )
      project_records = base.fetch_records(
        query=project_query,
        output_mode='object'
      )
      user_email_query = (
        base.session
        .query(User)
        .filter(User.email_id.in_(user_emails))
      )
      user_email_records = base.fetch_records(
        query=user_email_query,
        output_mode='object'
      )
      user_username_query = (
        base.session
        .query(User)
        .filter(User.username.in_(user_usernames))
      )
      user_username_records = base.fetch_records(
        query=user_username_query,
        output_mode='object'
      )
      if len(sample_igf_ids) > 0:
        sample_query = (
          base.session
          .query(Sample)
          .filter(Sample.sample_igf_id.in_(sample_igf_ids))
        )
        sample_records = base.fetch_records(
          query=sample_query,
          output_mode='object'
        )
      base.close_session()
      # filter out existing project metadata
      existing_project_igf_ids = [
        record.project_igf_id
        for record in project_records
      ]
      filtered_project_metadata = [
        metadata for metadata in project_metadata_list
        if metadata['project_igf_id'] not in existing_project_igf_ids
      ]
      errors.append(
        "Skipping existing projects: " +
        ' ,'.join(existing_project_igf_ids)
      )
      # filter out existing user metadata
      existing_user_emails = [
        record.email_id for record in user_email_records
      ]
      existing_user_usernames = [
        record.username for record in user_username_records
      ]
      filtered_user_metadata = [
        metadata for metadata in user_metadata_list
        if metadata['email_id'] not in existing_user_emails
      ]
      filtered_user_metadata = [
        metadata for metadata in filtered_user_metadata
        if metadata['username'] not in existing_user_usernames
      ]
      errors.append(
        "Skipping existing emails " +
        ' ,'.join(existing_user_emails)
      )
      errors.append(
        "Skipping existing usernames " +
        ' ,'.join(existing_user_usernames)
      )
      # filter out existing sample metadata
      filtered_sample_metadata = []
      if len(sample_igf_ids) > 0:
        existing_sample_igf_ids = [
          record.sample_igf_id for record in sample_records
        ]
        filtered_sample_metadata = [
          metadata for metadata in sample_metadata_list
          if metadata['sample_igf_id'] not in existing_sample_igf_ids
        ]
        errors.append(
          "Skipping existing samples " +
          ' ,'.join(existing_sample_igf_ids)
        )
      # filter out existing project user metadata
      filtered_project_user_metadata = [
        metadata for metadata in project_user_metadata_list
        if metadata['project_igf_id'] not in existing_project_igf_ids
      ]
      return (
        filtered_project_metadata,
        filtered_user_metadata,
        filtered_project_user_metadata,
        filtered_sample_metadata,
        errors
      )
    except Exception as e:
      raise ValueError(
        f"Failed to check existing metadata: {e}"
      )

  @staticmethod
  def _update_projectuser_metadata(
    project_user_metadata_list: list[dict[str, str]],
    secondary_user_email: str,
    project_igf_id_col: str = 'project_igf_id',
    email_id_col: str = 'email_id',
    data_authority_column: str = 'data_authority'
  ) -> list[dict[Any, Any]]:
    """
    A function for updating project user metadata with data authority column and secondary user

    :param project_user_metadata_list: List of project user metadata
    :param secondary_user_email: Secondary user email id
    :param project_igf_id_col: Name of project col, default: 'project_igf_id'
    :param email_id_col: name of the email id col, default 'email_id'
    :param data_authority_column: Name of the data authority col, default 'data_authority'
    :returns: A list of dictionaries in the format List[Dict[str, str]]
    """
    try:
      df = pd.DataFrame(project_user_metadata_list)
      # don't update if data authority column is already present
      if data_authority_column not in df.columns:
        # add data authority column and set first user as the data authority
        df[data_authority_column] = (
          df[email_id_col] == df.groupby(project_igf_id_col)[email_id_col]
          .transform('first')
        )
      # add secondary user
      merged_df = pd.concat([
        df,
        pd.DataFrame([{
          project_igf_id_col: project,
          email_id_col: secondary_user_email,
          data_authority_column: False}
          for project in df[project_igf_id_col].unique().tolist()
        ])
      ])
      return merged_df.to_dict(orient="records")
    except Exception as e:
      raise ValueError(
        f"Failed to update project user metadata: {e}"
      )

  @staticmethod
  def _register_new_metadata(
    project_metadata_list: list[dict[str, str]],
    user_metadata_list: list[dict[str, str]],
    project_user_metadata_list: list[dict[str, str]],
    sample_metadata_list: list[dict[str, str]],
    session_class: Any) -> tuple[bool, list[str]]:
    """
    Register new metadata in the database.

    :param project_metadata_list: List of project metadata
    :param user_metadata_list: List of user metadata
    :param project_user_metadata_list: List of project user metadata
    :param sample_metadata_list: List of sample metadata
    :param session_class: Session class for the database
    :return: True if registration is successful, False otherwise and a list if errors
    """
    try:
      errors = []
      base = BaseAdaptor(
        **{'session_class': session_class}
      )
      base.start_session()
      pa = ProjectAdaptor(**{'session': base.session})
      ua = UserAdaptor(**{'session': base.session})
      sa = SampleAdaptor(**{'session': base.session})
      try:
        if len(project_metadata_list) > 0:
          pa.store_project_and_attribute_data(
            data=project_metadata_list,
            autosave=False)
        if len(user_metadata_list) > 0:
          ua.store_user_data(
            data=user_metadata_list,
            autosave=False)
        if len(project_user_metadata_list) > 0:
          pa.assign_user_to_project(
            data=project_user_metadata_list,
            autosave=False)
        if len(sample_metadata_list) > 0:
          sa.store_sample_and_attribute_data(
            data=sample_metadata_list,
            autosave=False)
        base.commit_session()
        base.close_session()
        return True, errors
      except Exception as e:
        base.rollback_session()
        base.close_session()
        errors.append(e)
        return False, errors
    except Exception as e:
      raise ValueError(
        f"Failed to register new metadata: {e}"
      )

  def execute(self, metadata_context: MetadataContext) -> None:
    try:
      error_list = []
      validated_metadata_dict = \
        metadata_context.validated_metadata_dict
      raw_meatadata_dict = \
        metadata_context.raw_metadata_dict
      table_columns = \
        metadata_context.table_columns
      samples_required = \
        metadata_context.samples_required
      secondary_user_email = \
        metadata_context.default_project_user_email
      registered_metadata_dict = dict()
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
           filtered_sample_metadata,
           errors) = \
            self._check_and_filter_existing_metadata(
              project_metadata_list=project_metadata,
              user_metadata_list=user_metadata,
              project_user_metadata_list=project_user_metadata,
              sample_metadata_list=sample_metadata,
              session_class=metadata_context.session_class)
          if len(errors) > 0:
            error_list.extend(errors)
          updated_project_user_metadata = []
          if len(filtered_project_user_metadata) > 0:
            updated_project_user_metadata = \
              self._update_projectuser_metadata(
                project_user_metadata_list=filtered_project_user_metadata,
                secondary_user_email=secondary_user_email)
          db_update_status, errors = \
            self._register_new_metadata(
              project_metadata_list=filtered_project_metadata,
              user_metadata_list=filtered_user_metadata,
              project_user_metadata_list=updated_project_user_metadata,
              sample_metadata_list=filtered_sample_metadata,
              session_class=metadata_context.session_class)
          if not db_update_status:
            registered_metadata_dict.update({metadata_id: False})
          else:
            registered_metadata_dict.update({metadata_id: True})
          if len(errors) > 0:
            error_list.extend(errors)
        else:
          registered_metadata_dict.update({metadata_id: False})
      if len(error_list) > 0:
        metadata_context.error_list.extend(error_list)
      metadata_context.registered_metadata_dict = registered_metadata_dict
    except Exception as e:
      raise ValueError(
        f"Failed to split metadata: {e}"
      )


class SyncMetadataCommand(BaseCommand):
  def execute(self, metadata_context: MetadataContext) -> None:
    """
    Mark registered metadata as synched in portal
    """
    try:
      synced_metadata_dict = dict()
      portal_config_file = metadata_context.portal_config_file
      sync_metadata_url_suffix = metadata_context.sync_metadata_url_suffix
      registered_metadata_dict = metadata_context.registered_metadata_dict
      fetched_metadata_dict = metadata_context.fetched_metadata_dict
      for metadata_id, metadata_status in registered_metadata_dict.items():
        if metadata_id not in fetched_metadata_dict:
          raise ValueError(
            f"Cosmx Metadata {metadata_id} is not fetched"
          )
        if not metadata_status:
          raise ValueError(
            f"Metadata {metadata_id} can not be marked synced on " +
             "portal as its not registered"
            )
        cosmx_raw_metadata = fetched_metadata_dict.get(metadata_id)
        metadata_url = urljoin(
          sync_metadata_url_suffix,
          str(cosmx_raw_metadata)
        )
        _ = get_data_from_portal(
          url_suffix=metadata_url,
          portal_config_file=portal_config_file,
          request_mode='get'
        )
        synced_metadata_dict.update({
          metadata_id: True}
        )
      metadata_context.synced_metadata_dict = synced_metadata_dict
    except Exception as e:
      raise ValueError(
        f"Failed to fetch new metadata from portal: {e}"
      )


class ChainCommand:
  def __init__(self, commands: list[BaseCommand]) -> None:
    self.commands = commands

  def execute(self, metadata_context: MetadataContext) -> None: 
    try:
      for command in self.commands:
        command.execute(metadata_context)
    except Exception as e:
      raise ValueError(
        f"Failed to execute command: error {e}"
      )


class UnifiedMetadataRegistration:
  def __init__(
    self,
    raw_cosmx_metadata_id: int,
    portal_config_file: str,
    fetch_metadata_url_suffix: str,
    sync_metadata_url_suffix: str,
    metadata_validation_schema: str,
    db_config_file: str,
    default_project_user_email: str,
    samples_required: bool = False,
    ) -> None:
    self.metadata_context = MetadataContext(
      raw_cosmx_metadata_id=raw_cosmx_metadata_id,
      portal_config_file=portal_config_file,
      fetch_metadata_url_suffix=fetch_metadata_url_suffix,
      sync_metadata_url_suffix=sync_metadata_url_suffix,
      metadata_validation_schema=metadata_validation_schema,
      db_config_file=db_config_file,
      default_project_user_email=default_project_user_email,
      samples_required=samples_required)
    self.commands = [
      FetchNewMetadataCommand(),
      CheckRawMetadataColumnsCommand(),
      ValidateMetadataCommand(),
      CheckAndRegisterMetadataCommand(),
      SyncMetadataCommand()]
    self.chain_command = ChainCommand(self.commands)

  def execute(self) -> list[str]:
    try:
      self.chain_command.execute(self.metadata_context)
      error_list = self.metadata_context.error_list
      return error_list
    except Exception as e:
      raise ValueError(
        f"Failed to fetch and register metadata: {e}"
      )
