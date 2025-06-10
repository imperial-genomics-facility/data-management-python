import os
import json
import yaml
import subprocess
import unittest
import pandas as pd
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
    assert False, "Test not implemented"

  def test_prepare_run_ftp_export(self):
    assert False, "Test not implemented"

  def test_run_ftp_export(self):
    assert False, "Test not implemented"

  def test_prep_extract_ftp_export(self):
    assert False, "Test not implemented"

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