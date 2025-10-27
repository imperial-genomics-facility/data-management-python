import unittest, pytest
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

  def test_find_raw_metadata_id(self):
    assert False, "Test not implemented"

  def test_fetch_raw_metadata_from_portal(self):
    assert False, "Test not implemented"

  def test_check_raw_metadata_in_db(self):
    assert False, "Test not implemented"

  def test_register_raw_metadata_in_db(self):
    assert False, "Test not implemented"

  def test_mark_metadata_synced_on_portal(self):
    assert False, "Test not implemented"