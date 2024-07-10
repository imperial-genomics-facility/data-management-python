import os
import unittest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag37_project_cleanup_step1_utils import (
  find_project_data_for_cleanup,
  upload_project_cleanup_data_to_portal)

class TestDag37_project_cleanup_step1_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.dag37_project_cleanup_step1_utils.get_current_context")
  def test_find_project_data_for_cleanup(self, *args):
    with patch("igf_airflow.utils.dag37_project_cleanup_step1_utils.find_projects_for_cleanup",
               return_value=["A", "B"]):
      task_list = \
        find_project_data_for_cleanup.function(
          next_task="AAA",
          no_task="BBB",
          xcom_key="CCC")
      self.assertIn("AAA", task_list)
    with patch("igf_airflow.utils.dag37_project_cleanup_step1_utils.find_projects_for_cleanup",
               return_value=[]):
      task_list = \
        find_project_data_for_cleanup.function(
          next_task="AAA",
          no_task="BBB",
          xcom_key="CCC")
      self.assertIn("BBB", task_list)

  @patch("igf_airflow.utils.dag37_project_cleanup_step1_utils.get_current_context")
  @patch("igf_airflow.utils.dag37_project_cleanup_step1_utils.upload_files_to_portal")
  @patch("igf_airflow.utils.dag37_project_cleanup_step1_utils._gzip_json_file")
  def test_upload_project_cleanup_data_to_portal(
        self,
        get_current_context,
        upload_files_to_portal,
        _gzip_json_file):
    upload_project_cleanup_data_to_portal.function("A", "B")
    upload_files_to_portal.assert_called_once()
    _gzip_json_file.assert_called_once()
    get_current_context.assert_called_once()


if __name__=='__main__':
  unittest.main()