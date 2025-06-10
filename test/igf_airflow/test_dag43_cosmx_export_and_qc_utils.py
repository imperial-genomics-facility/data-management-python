import os
import json
import yaml
import subprocess
import unittest
import pandas as pd
from pathlib import Path
from yaml import load, SafeLoader, dump, SafeDumper
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag43_cosmx_export_and_qc_utils import (
    run_ftp_export_factory,
    prepare_run_ftp_export,
    run_ftp_export,
    prep_extract_ftp_export,
    extract_ftp_export,
    collect_extracted_data,
    collect_all_slides,
    prep_validate_export_md5,
    validate_export_md5,
    generate_count_qc_report,
    generate_fov_qc_report,
    generate_db_data,
    copy_slide_data_to_globus,
    register_db_data,
    collect_qc_reports_and_upload_to_portal
)

class Test_dag43_cosmx_export_and_qc_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_run_ftp_export_factory(self):
    design_data = {
      "run_metadata": [
        {"cosmx_run_id": "A1", "export_directory_path": "A1_ftp"},
        {"cosmx_run_id": "A2", "export_directory_path": "A2_ftp"}],
      "analysis_metadata": {
        "run_type": "RNA"}}
    design_file = Path(self.temp_dir) / 'design.yaml'
    with open(design_file, 'w') as fp:
      fp.write(dump(design_data, Dumper=SafeDumper))
    factory_data = run_ftp_export_factory.function(design_file=str(design_file), work_dir='/tmp')
    assert isinstance (factory_data, list)
    assert len(factory_data) == 2
    assert "cosmx_run_id" in factory_data[0]
    assert "export_directory_path" in factory_data[0]
    assert factory_data[0]["cosmx_run_id"] == "A1"
    assert factory_data[0]["export_directory_path"] == "A1_ftp"


  def test_prepare_run_ftp_export(self):
    run_config = {
      "cosmx_run_id": "A1", "export_directory_path": "A1_ftp"}
    prep_export_data = \
      prepare_run_ftp_export.function(run_entry=run_config, work_dir='/tmp')
    assert isinstance(prep_export_data, dict)
    assert "run_entry" in prep_export_data
    assert "cosmx_ftp_export_name" in prep_export_data
    assert prep_export_data["cosmx_ftp_export_name"] == "A1_ftp"
    assert isinstance(prep_export_data["run_entry"], dict)
    assert "export_dir" in prep_export_data["run_entry"]
    assert prep_export_data["run_entry"]["export_dir"] == "/TEST_EXPORT_DIR/A1_ftp"

  def test_run_ftp_export(self):
    bash_cmd = \
      run_ftp_export.function(cosmx_ftp_export_name="A1_ftp")
    assert isinstance(bash_cmd, str)
    assert "python TEST_SCRIPT -r TEST_PROFILE -q0 -s0 -f A1_ftp" in bash_cmd

  def test_prep_extract_ftp_export(self):
    run_config = {
      "cosmx_run_id": "A1",
      "export_directory_path": "A1_ftp",
      "export_dir": "/TEST_EXPORT_DIR/A1_ftp",
      "work_dir": "/tmp"}
    extracted_data = \
      prep_extract_ftp_export.function(run_entry=run_config)
    assert isinstance(extracted_data, dict)
    assert "run_entry" in extracted_data
    assert "export_dir" in extracted_data
    assert extracted_data["export_dir"] == "/TEST_EXPORT_DIR/A1_ftp"


  def test_extract_ftp_export(self):
    assert False, "Test not implemented"

  def test_collect_extracted_data(self):
    assert False, "Test not implemented"

  def test_collect_all_slides(self):
    assert False, "Test not implemented"

  def test_prep_validate_export_md5(self):
    assert False, "Test not implemented"

  def test_validate_export_md5(self):
    assert False, "Test not implemented"

  def test_generate_count_qc_report(self):
    assert False, "Test not implemented"

  def test_generate_fov_qc_report(self):
    assert False, "Test not implemented"

  def test_generate_db_data(self):
    assert False, "Test not implemented"

  def test_copy_slide_data_to_globus(self):
    assert False, "Test not implemented"

  def test_register_db_data(self):
    assert False, "Test not implemented"

  def test_collect_qc_reports_and_upload_to_portal(self):
    assert False, "Test not implemented"

if __name__=='__main__':
  unittest.main()