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


if __name__=='__main__':
  unittest.main()