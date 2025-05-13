import unittest
from unittest.mock import patch
from igf_data.process.seqrun_processing.unified_metadata_registration import (
    UnifiedMetadataRegistration,
    FetchNewMetadataCommand,
    ValidateAndCheckMetadataCommand,
    AddNewMetadataCommand,
    SyncMetadataCommand,
    ChainCommand,
    MetadataContext)

class TestUnifiedMetadataRegistration(unittest.TestCase):
  def setUp(self):
    self.portal_config_file = "test_config.json"
    self.fetch_metadata_url_suffix = "test_fetch_suffix"
    self.sync_metadata_url_suffix = "test_sync_suffix"
    self.metadata_context = MetadataContext(
      portal_config_file=self.portal_config_file,
      fetch_metadata_url_suffix=self.fetch_metadata_url_suffix,
      sync_metadata_url_suffix=self.sync_metadata_url_suffix,
      )

  def test_MetadataContext(self):
    self.assertIsInstance(self.metadata_context, MetadataContext)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={})
  def test_FetchNewMetadataCommand1(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    fetch_command.execute(metadata_context=self.metadata_context)
    self.assertFalse(
      self.metadata_context.metadata_fetched)

  @patch("igf_data.process.seqrun_processing.unified_metadata_registration.get_data_from_portal", return_value={"1": [{"project_igf_id": "A"}]})
  def test_FetchNewMetadataCommand2(self, *args):
    fetch_command = FetchNewMetadataCommand()
    self.assertIsInstance(fetch_command, FetchNewMetadataCommand)
    fetch_command.execute(metadata_context=self.metadata_context)
    self.assertTrue(
      self.metadata_context.metadata_fetched)
    self.assertEqual(
      self.metadata_context.raw_metadata_dict,
      {"1": [{"project_igf_id": "A"}]})

if __name__ == '__main__':
  unittest.main()