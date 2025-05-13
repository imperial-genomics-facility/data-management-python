import unittest
from unittest.mock import patch
from igf_data.process.seqrun_processing.unified_metadata_registration import (
    UnifiedMetadataRegistration,
    FetchNewMetadataCommand,
    CheckRawMetadataColumnsCommand,
    ValidateMetadataCommand,
    AddNewMetadataCommand,
    SyncMetadataCommand,
    ChainCommand,
    MetadataContext)

class TestUnifiedMetadataRegistration(unittest.TestCase):
  def setUp(self):
    self.portal_config_file = "test_config.json"
    self.fetch_metadata_url_suffix = "test_fetch_suffix"
    self.sync_metadata_url_suffix = "test_sync_suffix"
    self.metadata_validation_schema = "data/validation_schema/minimal_metadata_validation.json"


  def test_MetadataContext(self):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema
      )
    self.assertIsInstance(metadata_context, MetadataContext)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={})
  def test_FetchNewMetadataCommand1(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema
      )
    fetch_command.execute(metadata_context=metadata_context)
    self.assertFalse(
      metadata_context.metadata_fetched)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={"1": [{"project_igf_id": "A"}]})
  def test_FetchNewMetadataCommand2(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema
      )
    fetch_command.execute(metadata_context=metadata_context)
    self.assertTrue(
      metadata_context.metadata_fetched)
    self.assertEqual(
      metadata_context.raw_metadata_dict,
      {"1": [{"project_igf_id": "A"}]})


  def test_CheckRawMetadataColumnsCommand_check_columns(self):
    table_columns_dict = {
      "project": ["project_igf_id"],
      "user": ["name", "email_id", "username"],
      "sample": ["sample_igf_id",]}
    metadata_columns = ["project_igf_id", "name", "email_id", "username", "sample_igf_id"]
    sample_required = True
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 0)
    metadata_columns = ["project_igf_id", "name", "email_id", "username"]
    sample_required = False
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 0)
    metadata_columns = ["project_igf_id", "name", "email_id"]
    sample_required = False
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 1)
    self.assertEqual(errors[0], f"Missing required columns in user table: {table_columns_dict.get('user')}")
    metadata_columns = ["project_igf_id", "name", "email_id", "username"]
    sample_required = True
    errors = CheckRawMetadataColumnsCommand._check_columns(
      metadata_columns=metadata_columns,
      table_columns_dict=table_columns_dict,
      sample_required=sample_required)
    self.assertEqual(len(errors), 1)
    self.assertEqual(errors[0], f"Missing required columns in sample table: {table_columns_dict.get('sample')}")

  def test_CheckRawMetadataColumnsCommand_execute(self):
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      metadata_fetched=True,
      raw_metadata_dict={}
      )
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(metadata_context.checked_required_column_dict, {})
    self.assertEqual(len(metadata_context.error_list), 0)
    metadata_context.raw_metadata_dict = {
      1: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "username": "D", "sample_igf_id": "E"}],
      2: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "sample_igf_id": "E"}]
    }
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.checked_required_column_dict), 2)
    self.assertTrue(1 in metadata_context.checked_required_column_dict)
    self.assertTrue(2 in metadata_context.checked_required_column_dict)
    self.assertEqual(metadata_context.checked_required_column_dict.get(1), True)
    self.assertEqual(metadata_context.checked_required_column_dict.get(2), False)
    self.assertEqual(len(metadata_context.error_list), 1)
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      metadata_fetched=True,
      raw_metadata_dict={1: [{"project_igf_id": "A", "deliverable": "COSMX", "name": "B", "email_id": "C", "username": "D"}]},
      samples_required=False)
    CheckRawMetadataColumnsCommand().execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.checked_required_column_dict), 1)
    self.assertTrue(1 in metadata_context.checked_required_column_dict)
    self.assertEqual(metadata_context.checked_required_column_dict.get(1), True)


  def test_ValidateMetadataCommand_validate_metadata(self):
    validate = ValidateMetadataCommand()
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c@d.com",
            "username": "aaa",
            "sample_igf_id": "IGF001"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 0)
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c@d.com",
            "username": "aaa"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 0)
    errors = \
      validate.\
        _validate_metadata(
          metadata_entry=[{
            "project_igf_id": "IGF001",
            "deliverable": "COSMX",
            "name": "Ba Da",
            "email_id": "c-d.com",
            "username": "aaa"}],
          metadata_validation_schema=self.metadata_validation_schema,
          metadata_id=1)
    self.assertEqual(len(errors), 1)

  def test_ValidateMetadataCommand_execute(self):
    validate = ValidateMetadataCommand()
    metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      metadata_validation_schema=self.metadata_validation_schema,
      error_list=[],
      raw_metadata_dict={
        1: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "email_id": "c-d.com",
          "username": "aaa"}],
        2: [{
          "project_igf_id": "IGF001",
          "deliverable": "COSMX",
          "name": "Ba Da",
          "username": "aaa"}]},
      checked_required_column_dict={1: True, 2: False})
    validate.execute(metadata_context=metadata_context)
    self.assertEqual(len(metadata_context.error_list), 1)
    validated_metadata_dict = metadata_context.validated_metadata_dict
    self.assertEqual(len(validated_metadata_dict), 1)
    self.assertTrue(1 in validated_metadata_dict)
    self.assertFalse(validated_metadata_dict.get(1))

if __name__ == '__main__':
  unittest.main()