import os
import unittest
import pytest
import csv
from unittest.mock import patch
from pathlib import Path
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from yaml import load, SafeLoader, dump, SafeDumper
from igf_airflow.utils.dag50_olink_reveal_nextflow_utils import (
    prepare_olink_nextflow_script,
    run_olink_nextflow_script
)

class Test_dag50_olink_reveal_nextflow_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)

  @patch(
    "igf_airflow.utils.dag50_olink_reveal_nextflow_utils.DATABASE_CONFIG_FILE",
    "TEST"
  )
  @patch(
    "igf_airflow.utils.dag50_olink_reveal_nextflow_utils.OLINK_NEXTFLOW_CONF_TEMPLATE",
    "template/nextflow_template/olink_nextflow_v0.0.1.conf"
  )
  @patch(
    "igf_airflow.utils.dag50_olink_reveal_nextflow_utils.OLINK_NEXTFLOW_RUNNER_TEMPLATE",
    "template/nextflow_template/olink_nextflow_runner_v0.0.1.sh"
  )
  @patch(
    "igf_airflow.utils.dag50_olink_reveal_nextflow_utils.get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf",
    return_value=[1, "Test_project"]
  )
  def test_prepare_olink_nextflow_script(self, *args):
    design_yaml = """seqrun_metadata:
  - SEQRUN_ID_1
analysis_metadata:
  run_base_dir: /run_dir
  reveal_fixed_lod_csv: /OLINK/Reveal_Fixed_LOD.csv
  sample_type: Generic
  dataAnalysisRefIds: R10003
  panelDataArchive: /OLINK/NPXMap_PanelDataArchive_2.0.0.dat
  indexPlate: A
  plate_design_csv:
    - well_id;sample_id;sample_type
    - A1;A200;SAMPLE
    - A12;Olink_external_control3;CONTROL
    - D12;Olink_external_control6;PLATE_CONTROL
    - G11;Olink_external_control1;NEGATIVE_CONTROL
"""
    design_yaml_file = Path(self.temp_dir) / "analysis_design.yaml"
    with open(design_yaml_file, "w") as fp:
      fp.write(design_yaml)
    design_dict = {"analysis_design": design_yaml_file}
    analysis_script = prepare_olink_nextflow_script.function(
      design_dict=design_dict,
      work_dir=self.temp_dir
    )
    assert os.path.exists(analysis_script)
    with open(analysis_script, "r") as fp:
      script_data = fp.read() ## small file
    assert "olink_nextflow_v0.0.1.conf" in script_data
    conf_path = Path(self.temp_dir) / "olink_nextflow_v0.0.1.conf"
    assert conf_path.as_posix() in script_data
    with open(conf_path.as_posix(), "r") as fp:
      conf_data = fp.read() ## small file
    assert "run_id = \"SEQRUN_ID_1\"" in conf_data
    assert "run_dir = \"/run_dir/SEQRUN_ID_1\"" in conf_data
    plate_design_csv = Path(self.temp_dir) / "plate_design.csv"
    assert f"plate_design_csv = \"{plate_design_csv.as_posix()}\"" in conf_data
    assert "project_name = \"Test_project\""  in conf_data
    with open(plate_design_csv.as_posix(), "r") as fp:
      csv_data = fp.read()
    assert len(csv_data.split("\n")) == 5
    assert "well_id;sample_id;sample_type\nA1;A200;SAMPLE" in csv_data
    assert "G11;Olink_external_control1;NEGATIVE_CONTROL" in csv_data



  def test_run_olink_nextflow_script(self):
    script_file = "/tmp/t.sh"
    analysis_cmd = run_olink_nextflow_script.function(
      script_file
    )
    assert analysis_cmd == f"set -eo pipefail;\nbash {script_file}"

if __name__=='__main__':
  unittest.main()