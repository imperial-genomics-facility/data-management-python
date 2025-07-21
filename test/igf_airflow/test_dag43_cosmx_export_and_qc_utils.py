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
    copy_slide_data_to_globus,
    register_db_data,
    collect_slide_metadata,
    generate_additional_qc_report1,
    generate_additional_qc_report2,
    upload_reports_to_portal
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
    extract_cmd = \
      extract_ftp_export.function(
        export_dir="/TEST_EXPORT_DIR/A1_ftp",
        work_dir="/tmp")
    assert isinstance(extract_cmd, str)
    assert "EXPORT_DIR=/TEST_EXPORT_DIR/A1_ftp" in extract_cmd
    assert "WORK_DIR=/tmp" in extract_cmd
    assert r"find $RAWFILES_DIR -type f -exec chmod 600 {} \;" in extract_cmd
    assert r"find $FLATFILES_DIR -type f -exec chmod 600 {} \;" in extract_cmd


  def test_collect_extracted_data(self):
    run_config = {
      "cosmx_run_id": "A1",
      "export_directory_path": "A1_ftp",
      "export_dir": "/TEST_EXPORT_DIR/A1_ftp",
      "work_dir": "/tmp"}
    collected_data = \
      collect_extracted_data.function(run_entry=run_config)
    assert isinstance(collected_data, dict)
    assert "cosmx_run_id" in collected_data
    assert "export_dir" in collected_data
    assert collected_data["export_dir"] == "/TEST_EXPORT_DIR/A1_ftp"

  def test_collect_all_slides(self):
    data_export_dir = Path(self.temp_dir) / 'export_1'
    flatfiles_dir = data_export_dir / 'FlatFiles'
    slide_1_dir = flatfiles_dir / 'slide1'
    slide_2_dir = flatfiles_dir / 'slide2'
    os.makedirs(slide_1_dir)
    os.makedirs(slide_2_dir)
    run_config_list = [{
      "cosmx_run_id": "A1",
      "export_directory_path": "export_1",
      "export_dir": data_export_dir,
      "work_dir": "/tmp"}]
    slide_data_list = \
      collect_all_slides.function(
        run_entry_list=run_config_list)
    assert len(slide_data_list) == 2
    assert isinstance(slide_data_list[0], dict)
    assert "cosmx_run_id" in slide_data_list[0]
    assert "slide_dir" in slide_data_list[0]
    assert slide_data_list[0]["cosmx_run_id"] == "A1"
    assert slide_data_list[0]["slide_dir"] in ("slide1", "slide2")

  def test_prep_validate_export_md5(self):
    run_config = {
      "cosmx_run_id": "A1",
      "export_directory_path": "export_1",
      "export_dir": "/TEST_EXPORT_DIR/export_1",
      "work_dir": "/tmp"}
    validated_data = \
      prep_validate_export_md5.function(
        run_entry=run_config)
    assert isinstance(validated_data, dict)
    assert "run_entry" in validated_data
    assert "export_dir" in validated_data
    assert validated_data["export_dir"] == "/TEST_EXPORT_DIR/export_1"

  def test_validate_export_md5(self):
    export_dir = "/TEST_EXPORT_DIR/export_1"
    bash_cmd = \
      validate_export_md5.function(export_dir=export_dir)
    assert isinstance(bash_cmd, str)
    assert "FLATFILE_DIR=/TEST_EXPORT_DIR/export_1/FlatFiles" in bash_cmd

  def test_collect_slide_metadata(self):
    assert False, "Test not implemented"

  def test_generate_count_qc_report(self):
    assert False, "Test not implemented"

  def test_generate_fov_qc_report(self):
    assert False, "Test not implemented"

  def test_copy_slide_data_to_globus(self):
    assert False, "Test not implemented"

  def test_register_db_data(self):
    assert False, "Test not implemented"

  def test_generate_additional_qc_report1(self):
    assert generate_additional_qc_report1([{}]) is not None

  def test_generate_additional_qc_report2(self):
    assert generate_additional_qc_report2([{}]) is not None

  def test_upload_reports_to_portal(self):
    assert upload_reports_to_portal([{}]) is not None


if __name__=='__main__':
  unittest.main()