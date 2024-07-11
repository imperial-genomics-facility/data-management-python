import os
import json
import unittest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag38_project_cleanup_step2_utils import (
  fetch_project_cleanup_data,
  notify_user_about_project_cleanup,
  mark_user_notified_on_portal,
)

class TestDag38_project_cleanup_step2_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.dag38_project_cleanup_step2_utils.get_current_context")
  @patch("igf_airflow.utils.dag38_project_cleanup_step2_utils.get_data_from_portal",
         return_value=[{"A": "B"}])
  def test_fetch_project_cleanup_data(
        self,
        get_current_context,
        get_data_from_portal):
    json_file = \
      fetch_project_cleanup_data.\
        function()
    get_data_from_portal.assert_called_once()
    with open(json_file, "r") as fp:
      json_data = json.load(fp)
    self.assertEqual(len(json_data), 1)
    self.assertIn("A", json_data[0])
    self.assertEqual(json_data[0].get("A"), "B")

  @patch("igf_airflow.utils.dag38_project_cleanup_step2_utils.format_and_send_generic_email_to_user")
  def test_notify_user_about_project_cleanup(
        self,
        format_and_send_generic_email_to_user):
    json_data = {
      "user_name": "A",
      "user_email": "B",
      "projects": ["AA", "BB"],
      "deletion_date": "2023-09-01"}
    json_file = \
        os.path.join(self.temp_dir, "data.json")
    with open(json_file, "w") as fp:
      json.dump(json_data, fp)
    notify_user_about_project_cleanup.\
      function(
        project_cleanup_data_file=json_file)
    format_and_send_generic_email_to_user.\
      assert_called_once()

  @patch("igf_airflow.utils.dag38_project_cleanup_step2_utils.get_data_from_portal")
  @patch("igf_airflow.utils.dag38_project_cleanup_step2_utils.get_current_context")
  def test_mark_user_notified_on_portal(
        self,
        get_data_from_portal,
        get_current_context):
    mark_user_notified_on_portal.\
      function()
    get_current_context.assert_called_once()
    get_data_from_portal.assert_called_once()



if __name__=='__main__':
  unittest.main()