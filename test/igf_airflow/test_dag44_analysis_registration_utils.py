import unittest
from unittest.mock import patch, MagicMock
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag44_analysis_registration_utils import (
    find_raw_metadata_id,
    fetch_raw_metadata_from_portal,
    check_raw_metadata_in_db,
    register_raw_metadata_in_db,
    mark_metadata_synced_on_portal)

class Test_dag44_analysis_registration_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
  
  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.dag44_analysis_registration_utils.get_current_context")
  def test_find_raw_metadata_id(self, mock_get_context):
    # Setup Airflow context mock
    mock_context = MagicMock()
    mock_context.dag_run.conf.raw_analysis_id = 1
    mock_context.get.return_value = mock_context.dag_run
    mock_context.dag_run.conf.get.return_value = 1
    mock_get_context.return_value = mock_context
    raw_analysis_info = find_raw_metadata_id.function()
    assert "raw_analysis_id" in raw_analysis_info
    assert raw_analysis_info.get("raw_analysis_id") == 1

  def test_fetch_raw_metadata_from_portal(self):
    assert False, "Test not implemented"

  def test_check_raw_metadata_in_db(self):
    assert False, "Test not implemented"

  def test_register_raw_metadata_in_db(self):
    assert False, "Test not implemented"

  def test_mark_metadata_synced_on_portal(self):
    assert False, "Test not implemented"

if __name__=='__main__':
  unittest.main()