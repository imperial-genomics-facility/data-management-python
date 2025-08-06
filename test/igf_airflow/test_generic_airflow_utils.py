import os
import json
import unittest
from unittest.mock import patch, MagicMock
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.generic_airflow_utils import (
  check_and_seed_analysis_pipeline,
  send_airflow_pipeline_logs_to_channels,
  send_airflow_failed_logs_to_channels,
  generate_email_text_for_analysis,
  calculate_md5sum_for_analysis_dir,
  collect_analysis_dir,
  send_email_via_smtp,
  copy_analysis_to_globus_dir,
  fetch_analysis_yaml_and_dump_to_a_file,
  get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf)

class TestGneric_airflow_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_check_and_seed_analysis_pipeline(self):
    assert False, "Test not implemented"

  def test_send_airflow_pipeline_logs_to_channels(self):
    assert False, "Test not implemented"

  def test_send_airflow_failed_logs_to_channels(self):
    assert False, "Test not implemented"

  def test_generate_email_text_for_analysis(self):
    assert False, "Test not implemented"

  def test_calculate_md5sum_for_analysis_dir(self):
    assert False, "Test not implemented"

  def test_collect_analysis_dir(self):
    assert False, "Test not implemented"

  def test_send_email_via_smtp(self):
    assert False, "Test not implemented"

  def test_copy_analysis_to_globus_dir(self):
    assert False, "Test not implemented"

  def test_fetch_analysis_yaml_and_dump_to_a_file(self):
    assert False, "Test not implemented"

  @patch("igf_airflow.utils.generic_airflow_utils.get_project_igf_id_for_analysis", return_value="project1")
  @patch("igf_airflow.utils.generic_airflow_utils.get_current_context")
  def test_get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(self, mock_get_context, mock_get_project):
    # Setup Airflow context mock
    mock_context = MagicMock()
    mock_context.dag_run.conf.analysis_id = 1
    mock_context.get.return_value = mock_context.dag_run
    mock_context.dag_run.conf.get.return_value = 1
    mock_get_context.return_value = mock_context
    analysis_id, project_igf_id = \
      get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
        database_config_file="/tmp")
    assert analysis_id == 1
    assert project_igf_id == "project1"

if __name__=='__main__':
  unittest.main()