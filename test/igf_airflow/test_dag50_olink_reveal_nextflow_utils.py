import os
import unittest
import pytest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from yaml import load, SafeLoader
from igf_airflow.utils.dag50_olink_reveal_nextflow_utils import (
    prepare_olink_nextflow_script,
    run_olink_nextflow_script
)

class Test_dag50_olink_reveal_nextflow_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_prepare_olink_nextflow_script(self):
    assert False

  def test_run_olink_nextflow_script(self):
    assert False

if __name__=='__main__':
  unittest.main()