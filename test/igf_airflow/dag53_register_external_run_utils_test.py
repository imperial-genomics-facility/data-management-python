import os
import unittest
from unittest.mock import patch, MagicMock

MODULE = "igf_airflow.utils.dag53_register_external_run_utils"


class Test_dag53_register_external_run_utils(unittest.TestCase):
  def setUp(self):
    self.external_seqrun_id = "EXT_RUN_001"
    self.mock_dag_run = MagicMock()
    self.mock_dag_run.conf = {
      "external_seqrun_id": self.external_seqrun_id
    }
    self.mock_context = {
      "dag_run": self.mock_dag_run
    }

  @patch(f"{MODULE}.send_airflow_failed_logs_to_channels")
  @patch(f"{MODULE}.upload_files_to_portal")
  @patch(f"{MODULE}._load_interop_overview_data_to_seqrun_attribute")
  @patch(f"{MODULE}.get_temp_dir", return_value="/test_temp")
  @patch(f"{MODULE}.RunInfo_xml")
  @patch(f"{MODULE}.register_new_seqrun_to_db")
  @patch(f"{MODULE}._load_interop_data_to_db")
  @patch(f"{MODULE}._create_interop_report",
         return_value=("/path/notebook.html", None, "/path/overview.csv", None, "/path/work_dir"))
  @patch(f"{MODULE}.get_current_context")
  @patch(f"{MODULE}.Variable")
  def test_create_qc_and_load_external_run_success(
        self,
        mock_variable,
        mock_get_context,
        mock_create_interop_report,
        mock_load_interop_data,
        mock_register_seqrun,
        mock_runinfo_xml,
        mock_get_temp_dir,
        mock_load_interop_overview,
        mock_upload_files,
        mock_send_logs):
    mock_get_context.return_value = self.mock_context
    mock_runinfo_instance = MagicMock()
    mock_runinfo_instance.get_formatted_read_stats.return_value = "1x50"
    mock_runinfo_xml.return_value = mock_runinfo_instance
    mock_open_func = unittest.mock.mock_open()
    with patch(f"{MODULE}.HPC_SEQRUN_PATH", "/seqrun"), \
         patch(f"{MODULE}.INTEROP_REPORT_TEMPLATE", "/template"), \
         patch(f"{MODULE}.INTEROP_REPORT_IMAGE", "/image"), \
         patch(f"{MODULE}.INTEROP_REPORT_BASE_PATH", "/interop_base"), \
         patch(f"{MODULE}.DATABASE_CONFIG_FILE", "/db_config"), \
         patch(f"{MODULE}.IGF_PORTAL_CONF", "/portal_conf"), \
         patch(f"{MODULE}.PORTAL_ADD_SEQRUN_URL", "/api/v1/raw_seqrun/add_new_seqrun"), \
         patch(f"{MODULE}.PORTAL_ADD_INTEROP_REPORT_URL", "/api/v1/interop_data/add_report"), \
         patch(f"{MODULE}.MS_TEAMS_CONF", "/ms_teams_conf"), \
         patch("builtins.open", mock_open_func):
      from igf_airflow.utils.dag53_register_external_run_utils import (
        create_qc_and_load_external_run
      )
      create_qc_and_load_external_run.function()
    mock_create_interop_report.assert_called_once_with(
      run_id=self.external_seqrun_id,
      run_dir_base_path="/seqrun",
      report_template="/template",
      report_image="/image"
    )
    mock_load_interop_data.assert_called_once_with(
      run_id=self.external_seqrun_id,
      interop_output_dir="/path/work_dir",
      interop_report_base_path="/interop_base",
      dbconfig_file="/db_config"
    )
    mock_register_seqrun.assert_called_once_with(
      dbconfig_file="/db_config",
      seqrun_id=self.external_seqrun_id,
      seqrun_base_path="/seqrun"
    )
    mock_runinfo_xml.assert_called_once_with(
      xml_file=os.path.join("/seqrun", self.external_seqrun_id, "RunInfo.xml")
    )
    mock_get_temp_dir.assert_called_once_with(use_ephemeral_space=True)
    self.assertEqual(mock_upload_files.call_count, 2)
    first_upload_call = mock_upload_files.call_args_list[0]
    self.assertEqual(
      first_upload_call.kwargs["url_suffix"],
      "/api/v1/raw_seqrun/add_new_seqrun"
    )
    second_upload_call = mock_upload_files.call_args_list[1]
    self.assertEqual(
      second_upload_call.kwargs["url_suffix"],
      "/api/v1/interop_data/add_report"
    )
    self.assertEqual(
      second_upload_call.kwargs["data"],
      {"run_name": self.external_seqrun_id, "tag": "InterOp"}
    )
    mock_load_interop_overview.assert_called_once_with(
      seqrun_igf_id=self.external_seqrun_id,
      dbconfig_file="/db_config",
      interop_overview_file="/path/overview.csv"
    )
    mock_send_logs.assert_not_called()

  @patch(f"{MODULE}.send_airflow_failed_logs_to_channels")
  @patch(f"{MODULE}.get_current_context")
  @patch(f"{MODULE}.Variable")
  def test_create_qc_and_load_external_run_no_seqrun_id(
        self,
        mock_variable,
        mock_get_context,
        mock_send_logs):
    mock_dag_run = MagicMock()
    mock_dag_run.conf = {}
    mock_get_context.return_value = {"dag_run": mock_dag_run}
    with patch(f"{MODULE}.MS_TEAMS_CONF", "/ms_teams_conf"):
      from igf_airflow.utils.dag53_register_external_run_utils import (
        create_qc_and_load_external_run
      )
      with self.assertRaises(ValueError):
        create_qc_and_load_external_run.function()
    mock_send_logs.assert_called_once()

  @patch(f"{MODULE}.send_airflow_failed_logs_to_channels")
  @patch(f"{MODULE}.get_current_context")
  @patch(f"{MODULE}.Variable")
  def test_create_qc_and_load_external_run_no_dag_run(
        self,
        mock_variable,
        mock_get_context,
        mock_send_logs):
    mock_get_context.return_value = {"dag_run": None}
    with patch(f"{MODULE}.MS_TEAMS_CONF", "/ms_teams_conf"):
      from igf_airflow.utils.dag53_register_external_run_utils import (
        create_qc_and_load_external_run
      )
      with self.assertRaises(ValueError):
        create_qc_and_load_external_run.function()
    mock_send_logs.assert_called_once()

  @patch(f"{MODULE}.send_airflow_failed_logs_to_channels")
  @patch(f"{MODULE}._create_interop_report",
         side_effect=Exception("interop report failed"))
  @patch(f"{MODULE}.get_current_context")
  @patch(f"{MODULE}.Variable")
  def test_create_qc_and_load_external_run_interop_report_failure(
        self,
        mock_variable,
        mock_get_context,
        mock_create_interop_report,
        mock_send_logs):
    mock_get_context.return_value = self.mock_context
    with patch(f"{MODULE}.HPC_SEQRUN_PATH", "/seqrun"), \
         patch(f"{MODULE}.INTEROP_REPORT_TEMPLATE", "/template"), \
         patch(f"{MODULE}.INTEROP_REPORT_IMAGE", "/image"), \
         patch(f"{MODULE}.MS_TEAMS_CONF", "/ms_teams_conf"):
      from igf_airflow.utils.dag53_register_external_run_utils import (
        create_qc_and_load_external_run
      )
      with self.assertRaises(ValueError):
        create_qc_and_load_external_run.function()
    mock_send_logs.assert_called_once()
    self.assertIn(
      "interop report failed",
      mock_send_logs.call_args.kwargs["message_prefix"]
    )


if __name__ == '__main__':
  unittest.main()
