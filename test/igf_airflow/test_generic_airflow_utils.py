import os
import json
import unittest
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from unittest.mock import patch, MagicMock
from igf_data.igfdb.igfTables import (
  Project,
  Analysis)
from igf_data.utils.fileutils import (
  get_temp_dir,
  get_date_stamp_for_file_name,
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
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    self.base = BaseAdaptor(**dbparam)
    self.engine = self.base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  # def test_check_and_seed_analysis_pipeline(self):
  #   assert False, "Test not implemented"

  # def test_send_airflow_pipeline_logs_to_channels(self):
  #   assert False, "Test not implemented"

  # def test_send_airflow_failed_logs_to_channels(self):
  #   assert False, "Test not implemented"

  # def test_generate_email_text_for_analysis(self):
  #   assert False, "Test not implemented"

  # def test_calculate_md5sum_for_analysis_dir(self):
  #   assert False, "Test not implemented"

  # def test_collect_analysis_dir(self):
  #   assert False, "Test not implemented"

  # def test_send_email_via_smtp(self):
  #   assert False, "Test not implemented"

  def test_copy_analysis_to_globus_dir(self):
    base = BaseAdaptor(**{'session_class':self.base.get_session_class()})
    base.start_session()
    project = \
      Project(
        project_id=1,
        project_igf_id="project1")
    base.session.add(project)
    base.session.flush()
    analysis = \
      Analysis(
        analysis_id=1,
        analysis_type="test",
        analysis_name="analysis1",
        project_id=1)
    base.session.add(analysis)
    base.session.flush()
    base.session.commit()
    base.close_session()
    temp_dir = self.temp_dir
    globus_root_dir = os.path.join(temp_dir, 'globus')
    os.makedirs(globus_root_dir, exist_ok=True)
    analysis_dir = os.path.join(temp_dir, 'results')
    os.makedirs(analysis_dir, exist_ok=True)
    results_file = os.path.join(analysis_dir, 't.txt')
    with open(results_file, 'w') as fp:
      fp.write("AAA")
    date_stamp = \
      get_date_stamp_for_file_name()
    target_dir_path = \
      copy_analysis_to_globus_dir(
        globus_root_dir=globus_root_dir,
        dbconfig_file=self.dbconfig,
        analysis_id=1,
        analysis_dir=analysis_dir,
        date_tag=date_stamp,
        analysis_dir_prefix='analysis')
    expected_target_dir = \
      os.path.join(
        globus_root_dir,
        "project1",
        "analysis",
        "test",
        "analysis1",
        date_stamp,
        'results')
    assert target_dir_path == expected_target_dir
    assert os.path.exists(os.path.join(target_dir_path, 't.txt'))
    analysis_dir = os.path.join(temp_dir, 'results2')
    os.makedirs(analysis_dir, exist_ok=True)
    results_file = os.path.join(analysis_dir, 't.txt')
    with open(results_file, 'w') as fp:
      fp.write("AAA")
    target_dir_path = \
      copy_analysis_to_globus_dir(
        globus_root_dir=globus_root_dir,
        dbconfig_file=self.dbconfig,
        analysis_id=1,
        analysis_dir=analysis_dir,
        date_tag=date_stamp,
        globus_dir_list=['slide1',],
        analysis_dir_prefix='analysis')
    expected_target_dir = \
      os.path.join(
        globus_root_dir,
        "project1",
        "analysis",
        'slide1',
        'results2')
    assert target_dir_path == expected_target_dir
    assert os.path.exists(os.path.join(target_dir_path, 't.txt'))

  # def test_fetch_analysis_yaml_and_dump_to_a_file(self):
  #   assert False, "Test not implemented"

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