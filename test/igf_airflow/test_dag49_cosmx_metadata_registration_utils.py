import unittest
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir
)
from unittest.mock import patch, MagicMock
from igf_airflow.utils.dag49_cosmx_metadata_registration_utils import (
    find_raw_metadata_id,
    register_cosmx_metadata
)

class Test_dag49_cosmx_metadata_registration_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.get_current_context")
  def test_find_raw_metadata_id(self, mock_get_context):
    mock_context = MagicMock()
    mock_context.dag_run.conf.raw_cosmx_metadata_id = 1
    mock_context.get.return_value = mock_context.dag_run
    mock_context.dag_run.conf.get.return_value = 1
    mock_get_context.return_value = mock_context
    raw_cosmx_metadata_id = find_raw_metadata_id.function()
    assert raw_cosmx_metadata_id == 1

  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.DATABASE_CONFIG_FILE", "test.conf")
  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.IGF_PORTAL_CONF", "test.conf")
  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.METADATA_VALIDATION_SCHEMA", "test.json")
  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.DEFAULT_EMAIL", "c@c.org")
  @patch("igf_airflow.utils.dag49_cosmx_metadata_registration_utils.UnifiedMetadataRegistration")
  def test_register_cosmx_metadata(self, mock_class, *args):
    mock_instance = MagicMock()
    mock_instance.execute.return_value = None
    mock_class.return_value = mock_instance
    register_cosmx_metadata.function(
      raw_cosmx_metadata_id=1
    )
    mock_instance.execute.assert_called_once()

if __name__=='__main__':
  unittest.main()