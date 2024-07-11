import os
import json
import unittest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.generic_airflow_tasks import (
    mark_analysis_running,
    mark_analysis_finished,
    mark_analysis_failed,
    send_email_to_user,
    calculate_md5sum_for_main_work_dir,
    copy_data_to_globus,
    fetch_analysis_design_from_db,
    create_main_work_dir,
    load_analysis_results_to_db,
    move_per_sample_analysis_to_main_work_dir,
    collect_all_analysis
)

class TestgGneric_airflow_tasksA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch("igf_airflow.utils.generic_airflow_tasks.send_airflow_failed_logs_to_channels")
  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.Variable")
  @patch("igf_airflow.utils.generic_airflow_tasks.check_and_seed_analysis_pipeline", return_value=True)
  def test_mark_analysis_runningA(self, *args):
    task_list = mark_analysis_running.function(next_task="taskA", last_task="taskB")
    self.assertEqual(task_list[0], "taskA")

  @patch("igf_airflow.utils.generic_airflow_tasks.send_airflow_failed_logs_to_channels")
  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.Variable")
  @patch("igf_airflow.utils.generic_airflow_tasks.check_and_seed_analysis_pipeline", return_value=False)
  def test_mark_analysis_runningB(self, *args):
    task_list = mark_analysis_running.function(next_task="taskA", last_task="taskB")
    self.assertEqual(task_list[0], "taskB")

  @patch("igf_airflow.utils.generic_airflow_tasks.send_airflow_failed_logs_to_channels")
  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.Variable")
  @patch("igf_airflow.utils.generic_airflow_tasks.check_and_seed_analysis_pipeline", return_value=True)
  def test_mark_analysis_finishedA(self, *args):
    self.assertIsNone(mark_analysis_finished.function())

  @patch("igf_airflow.utils.generic_airflow_tasks.send_airflow_failed_logs_to_channels")
  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.Variable")
  @patch("igf_airflow.utils.generic_airflow_tasks.check_and_seed_analysis_pipeline", return_value=True)
  def test_mark_analysis_failedA(self, *args):
    self.assertIsNone(mark_analysis_failed.function())

  @patch("igf_airflow.utils.generic_airflow_tasks.send_airflow_failed_logs_to_channels")
  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.generate_email_text_for_analysis", return_value=["A", "B"])
  @patch("igf_airflow.utils.generic_airflow_tasks.send_email_via_smtp")
  def test_send_email_to_user(
        self,
        send_email_via_smtp,
        generate_email_text_for_analysis,
        get_current_context,
        send_airflow_failed_logs_to_channels):
    email_config = os.path.join(self.temp_dir, "email_conf.json")
    data = [{"username": "AAAA"}]
    with open(email_config, "w") as fp:
      json.dump(data, fp)
    with patch("igf_airflow.utils.generic_airflow_tasks.EMAIL_CONFIG", email_config):
      self.assertIsNone(send_email_to_user.function())
      get_current_context.assert_called_once()
      generate_email_text_for_analysis.assert_called_once()
      send_email_via_smtp.assert_called_once()

  @patch("igf_airflow.utils.generic_airflow_tasks.calculate_md5sum_for_analysis_dir", return_values="AAA")
  def test_calculate_md5sum_for_main_work_dir(self, *args):
    dir_name = \
      calculate_md5sum_for_main_work_dir.\
        function(main_work_dir="BBB")
    self.assertEqual(dir_name, "BBB")

  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.copy_analysis_to_globus_dir", return_value="AAA")
  def test_copy_data_to_globus(
        self,
        get_current_context,
        copy_analysis_to_globus_dir):
    with patch("igf_airflow.utils.generic_airflow_tasks.DATABASE_CONFIG_FILE", "EEE"):
      with patch("igf_airflow.utils.generic_airflow_tasks.DATABASE_CONFIG_FILE", "EEE"):
        copy_data_to_globus.\
          function(analysis_dir_dict={
            "target_dir_path": "BBB",
            "date_tag": "CCC"})
        copy_analysis_to_globus_dir.assert_called_once()
        get_current_context.assert_called_once()

  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.fetch_analysis_yaml_and_dump_to_a_file",
         return_value="AAA")
  def test_fetch_analysis_design_from_db(
        self,
        get_current_context,
        fetch_analysis_yaml_and_dump_to_a_file):
    design_info = \
      fetch_analysis_design_from_db.function()
    self.assertIn('analysis_design', design_info)
    self.assertEqual(design_info.get('analysis_design'), "AAA")
    get_current_context.assert_called_once()
    fetch_analysis_yaml_and_dump_to_a_file.assert_called_once()


  def test_create_main_work_dir(self):
    work_dir = \
      create_main_work_dir.function(task_tag="test")
    self.assertEqual(os.path.basename(work_dir), "test")
    self.assertTrue(os.path.exists(work_dir))

  @patch("igf_airflow.utils.generic_airflow_tasks.get_current_context")
  @patch("igf_airflow.utils.generic_airflow_tasks.collect_analysis_dir",
         return_value=["AA", "BB", "CC"])
  def test_load_analysis_results_to_db(
        self,
        get_current_context,
        collect_analysis_dir):
    loaded_dir_info = \
      load_analysis_results_to_db.\
        function(main_work_dir="AAA")
    self.assertIn('target_dir_path', loaded_dir_info)
    self.assertIn('date_tag', loaded_dir_info)
    self.assertEqual(loaded_dir_info.get('target_dir_path'), "AA")
    self.assertEqual(loaded_dir_info.get('date_tag'), "CC")
    collect_analysis_dir.assert_called_once()

  @patch("igf_airflow.utils.generic_airflow_tasks.check_file_path")
  @patch("igf_airflow.utils.generic_airflow_tasks.shutil.move")
  def test_move_per_sample_analysis_to_main_work_dir(self, *args):
    output_dict = \
      move_per_sample_analysis_to_main_work_dir.\
        function(
          work_dir="/path/target",
          analysis_output={
            "sample_id": "AA",
            "output_dir": "/path/source"})
    self.assertIn("sample_id", output_dict)
    self.assertIn("output", output_dict)
    self.assertEqual(output_dict.get("sample_id"), "AA")
    self.assertEqual(output_dict.get("output"), "/path/target/source")





if __name__=='__main__':
  unittest.main()