import os
import json
import unittest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag39_project_cleanup_step3_utils import (
  cleanup_old_project_in_db,
  mark_project_deleted_on_portal,
  notify_user_about_project_cleanup_finished)

class TestDag39_project_cleanup_step3_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.dag39_project_cleanup_step3_utils.cleanup_igf_projects_on_db")
  @patch("igf_airflow.utils.dag39_project_cleanup_step3_utils.send_airflow_pipeline_logs_to_channels")
  def test_cleanup_old_project_in_db(
        self,
        cleanup_igf_projects_on_db,
        send_airflow_pipeline_logs_to_channels):
    json_data = {
      "user_name": "A",
      "user_email": "B",
      "projects": ["AA", "BB"],
      "deletion_date": "2023-09-01"}
    json_file = \
      os.path.join(self.temp_dir, "data.json")
    with open(json_file, "w") as fp:
      json.dump(json_data, fp)
    cleanup_old_project_in_db.\
      function(
        project_cleanup_data_file=json_file)
    cleanup_igf_projects_on_db.\
      assert_called_once()
    send_airflow_pipeline_logs_to_channels.\
      assert_called_once()

if __name__=='__main__':
  unittest.main()