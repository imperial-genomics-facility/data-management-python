import unittest
from unittest.mock import (
  patch,
  MagicMock)
from igf_airflow.utils.dag45_metadata_registration_utils import (
  find_raw_metadata_id,
  register_metadata_from_portal)

class Test_dag45_metadata_registration_utilsA(unittest.TestCase):
  def setUp(self):
    pass

  def tearDown(self):
    pass
  
  @patch("igf_airflow.utils.dag45_metadata_registration_utils.get_current_context")
  def test_find_raw_metadata_id(self, mock_get_context):
    # Setup Airflow context mock
    mock_context = MagicMock()
    mock_context.dag_run.conf.raw_metadata_id = 1
    mock_context.get.return_value = mock_context.dag_run
    mock_context.dag_run.conf.get.return_value = 1
    mock_get_context.return_value = mock_context
    raw_metadata_id = find_raw_metadata_id.function()
    assert raw_metadata_id == 1

  @patch('igf_airflow.utils.dag45_metadata_registration_utils.Find_and_register_new_project_data_from_portal_db')
  @patch('igf_airflow.utils.dag45_metadata_registration_utils._parse_default_user_email_from_email_config')
  def test_register_metadata_from_portal(self, mock_parse, mock_class):
    # Create instance of the mocked class
    mock_instance = MagicMock()
    mock_instance.process_project_data_and_account.return_value = None
    mock_class.return_value = mock_instance
    register_metadata_from_portal.function(raw_metadata_id=1)
    mock_instance.process_project_data_and_account.assert_called_once()

if __name__=='__main__':
  unittest.main()