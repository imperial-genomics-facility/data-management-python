import unittest
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

        self.unified_metadata_registration = UnifiedMetadataRegistration(
            portal_config_file=self.portal_config_file,
            samples_required=True
        )

    def test_initialization(self):
        self.assertIsInstance(self.unified_metadata_registration, UnifiedMetadataRegistration)
        self.assertIsInstance(self.unified_metadata_registration.metadata_context, MetadataContext)
        self.assertIsInstance(self.unified_metadata_registration.commands, list)

if __name__ == '__main__':
  unittest.main()