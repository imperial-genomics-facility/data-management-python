import os
import json
import pytest
import unittest
from unittest.mock import patch, MagicMock
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_airflow.utils.dag44_analysis_registration_utils import (
    find_raw_metadata_id,
    check_registered_analysis_in_db,
    fetch_raw_metadata_from_portal,
    check_raw_metadata_in_db,
    register_analysis_in_db,
    register_raw_analysis_metadata_in_db,
    mark_metadata_synced_on_portal)

class Test_dag44_analysis_registration_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    ## 1. register test project 1
    project_data = [{'project_igf_id': 'projectA'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_data = [{'project_igf_id': 'projectB'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_data = [{'project_igf_id': 'projectC'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    ## 2. register pipeline 1
    pipeline_data = [{
      "pipeline_name": "pipeline1",
      "pipeline_db": "sqlite:////data/aln.db",
      "pipeline_init_conf": {},
      "pipeline_run_conf": {}
    }]
    pl = PipelineAdaptor(**{'session': base.session})
    pl.store_pipeline_data(data=pipeline_data)
    ## 3. register analysis 1
    analysis_data = [{
      'project_igf_id': 'projectA',
      'analysis_name': 'analysis_1',
      'analysis_type': 'pipeline1',
      'analysis_description': '[{"sample_igf_id": "XYZ"}]'}]
    aa = \
      AnalysisAdaptor(**{'session': base.session})
    aa.store_analysis_data(data=analysis_data)
    pipeline_seed_data = [{
      'pipeline_id': 1,
      'seed_id': 1,
      'seed_table': 'analysis'}]
    pl.create_pipeline_seed(pipeline_seed_data)
    base.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

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

  @patch("igf_airflow.utils.dag44_analysis_registration_utils.get_data_from_portal",
         return_value={'project_id': 1, 'pipeline_id': 2, 'analysis_name': 'a', 'analysis_yaml': 'b:'})
  def test_fetch_raw_metadata_from_portal(self, *args):
    raw_metadata_info = \
      fetch_raw_metadata_from_portal.function(raw_analysis_id=1)
    assert "raw_metadata_file" in raw_metadata_info


  def test_check_registered_analysis_in_db(self):
    status = \
      check_registered_analysis_in_db(
        project_id=1,
        analysis_name='analysis_1',
        dbconf_json=self.dbconfig)
    assert status is False
    status = \
      check_registered_analysis_in_db(
        project_id=2,
        analysis_name='analysis_2',
        dbconf_json=self.dbconfig)
    assert status is True

  @patch("igf_airflow.utils.dag44_analysis_registration_utils.DATABASE_CONFIG_FILE", "data/dbconfig.json")
  def test_check_raw_metadata_in_db(self, *args):
    json_file_1 = \
      os.path.join(self.temp_dir, "file1.json")
    with open(json_file_1, "w") as fp:
      json.dump({
        'project_id': 1,
        'analysis_name': 'analysis_1',
        'pipeline_id': 1,
        'analysis_yaml': 'b:'}, fp)
    json_file_2 = \
      os.path.join(self.temp_dir, "file2.json")
    with open(json_file_2, "w") as fp:
      json.dump({
        'project_id': 2,
        'analysis_name': 'analysis_2',
        'pipeline_id': 1,
        'analysis_yaml': 'b:'}, fp)
    valid_raw_metadata_info = \
      check_raw_metadata_in_db.function(
        raw_metadata_file=json_file_1)
    assert "valid_raw_metadata_file" in valid_raw_metadata_info
    assert valid_raw_metadata_info.get("valid_raw_metadata_file") == ""
    valid_raw_metadata_info = \
      check_raw_metadata_in_db.function(
        raw_metadata_file=json_file_2)
    assert "valid_raw_metadata_file" in valid_raw_metadata_info
    assert valid_raw_metadata_info.get("valid_raw_metadata_file") == json_file_2

  def test_register_analysis_in_db(self):
    status = \
      register_analysis_in_db(
        project_id=1,
        pipeline_id=1,
        analysis_name='analysis_1',
        analysis_yaml='a:',
        dbconf_json=self.dbconfig)
    assert status is False
    status = \
      register_analysis_in_db(
        project_id=2,
        pipeline_id=1,
        analysis_name='analysis_2',
        analysis_yaml='a:',
        dbconf_json=self.dbconfig)
    assert status is True
    aa = \
      AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name='analysis_2',
        project_id=2)
    aa.close_session()
    assert analysis_id is not None
    with pytest.raises(Exception):
      status = \
        register_analysis_in_db(
          project_id=3,
          pipeline_id=2,
          analysis_name='analysis_1',
          analysis_yaml='new_data',
          dbconf_json=self.dbconfig)


  @patch("igf_airflow.utils.dag44_analysis_registration_utils.DATABASE_CONFIG_FILE", "data/dbconfig.json")
  def test_register_raw_analysis_metadata_in_db(self, *args):
    json_file_1 = \
      os.path.join(self.temp_dir, "file1.json")
    with open(json_file_1, "w") as fp:
      json.dump({
        'project_id': 1,
        'analysis_name': 'analysis_1',
        'pipeline_id': 1,
        'analysis_yaml': 'b:'}, fp)
    json_file_2 = \
      os.path.join(self.temp_dir, "file2.json")
    with open(json_file_2, "w") as fp:
      json.dump({
        'project_id': 2,
        'analysis_name': 'analysis_2',
        'pipeline_id': 1,
        'analysis_yaml': 'b:'}, fp)
    status_info = \
      register_raw_analysis_metadata_in_db.function(
        valid_raw_metadata_file=json_file_1)
    assert "status" in status_info
    assert status_info.get("status") is False
    status_info = \
      register_raw_analysis_metadata_in_db.function(
        valid_raw_metadata_file=json_file_2)
    assert "status" in status_info
    assert status_info.get("status") is True
    aa = \
      AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name='analysis_2',
        project_id=2)
    aa.close_session()
    assert analysis_id is not None

  @patch("igf_airflow.utils.dag44_analysis_registration_utils.get_data_from_portal")
  @patch("igf_airflow.utils.dag44_analysis_registration_utils.send_airflow_pipeline_logs_to_channels")
  @patch("igf_airflow.utils.dag44_analysis_registration_utils.send_airflow_failed_logs_to_channels")
  def test_mark_metadata_synced_on_portal(self, mock_get_data_from_portal, *args):
    mark_metadata_synced_on_portal.function(
      raw_analysis_id=1,
      registration_status=True)
    mock_get_data_from_portal.called_once()
    

if __name__=='__main__':
  unittest.main()