import os
import json
import yaml
import subprocess
import unittest, pytest
import pandas as pd
from pathlib import Path
from dateutil.parser import parse
from unittest.mock import patch, MagicMock
from yaml import load, SafeLoader, dump, SafeDumper
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
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
    upload_reports_to_portal,
    match_slide_ids_with_project_id,
    fetch_cosmx_metadata_info,
    load_cosmx_data_to_db,
    fetch_slide_annotations_from_design_file
)
from igf_data.igfdb.igfTables import (
  Project,
  Analysis
)

class Test_dag43_cosmx_export_and_qc_utilsA(unittest.TestCase):
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
      prep_extract_ftp_export.function(
        run_entry=run_config,
        export_finished=1)
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
      collect_extracted_data.function(
        run_entry=run_config,
        validation_finished=1)
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
    assert "slide_id" in slide_data_list[0]
    assert slide_data_list[0]["cosmx_run_id"] == "A1"
    assert slide_data_list[0]["slide_id"] in ("slide1", "slide2")

  def test_prep_validate_export_md5(self):
    run_config = {
      "cosmx_run_id": "A1",
      "export_directory_path": "export_1",
      "export_dir": "/TEST_EXPORT_DIR/export_1",
      "work_dir": "/tmp"}
    validated_data = \
      prep_validate_export_md5.function(
        run_entry=run_config,
        extract_finished=1)
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

  # @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_project_igf_id_for_analysis", return_value="project1")
  # @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_current_context")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf", return_value=[1, "project1"])
  def test_match_slide_ids_with_project_id(self, *args):
    base = BaseAdaptor(**{'session_class':self.base.get_session_class()})
    base.start_session()
    project = \
      Project(project_id=1, project_igf_id="project1")
    base.session.add(project)
    base.session.flush()
    analysis = \
      Analysis(
        analysis_id=1,
        analysis_type="test",
        analysis_name="test",
        project_id=1)
    base.session.add(analysis)
    base.session.flush()
    base.session.commit()
    base.close_session()

    # Setup Airflow context mock
    # mock_context = MagicMock()
    # mock_context.dag_run.conf.analysis_id = 1
    # mock_context.get.return_value = mock_context.dag_run
    # mock_context.dag_run.conf.get.return_value = 1
    # mock_get_context.return_value = mock_context

    slide_data_list = [
      {"cosmx_run_id": "test",
       "export_dir": "test",
       "slide_id": "project1_slideA"}]
    status = \
      match_slide_ids_with_project_id.function(slide_data_list)
    assert status
    slide_data_list = [
      {"cosmx_run_id": "test",
       "export_dir": "test",
       "slide_id": "project2_slideA"}]
    with pytest.raises(ValueError):
      status = \
        match_slide_ids_with_project_id.function(slide_data_list)

  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf", return_value=[1, "project1"])
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE", return_value="project1")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_project_igf_id_for_analysis", return_value="project1")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_QC_REPORT_IMAGE1", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.check_file_path")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.Notebook_runner")
  # @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_current_context")
  def test_collect_slide_metadata(self, mock_nb_runner, *args):
    # Setup Airflow context mock
    # mock_context = MagicMock()
    # mock_context.dag_run.conf.analysis_id = 1
    # mock_context.get.return_value = mock_context.dag_run
    # mock_context.dag_run.conf.get.return_value = 1
    # mock_get_context.return_value = mock_context

    mock_nb_context = MagicMock()
    mock_nb_context.execute_notebook_in_singularity.return_value = ["test", "test"]
    mock_nb_runner.return_value = mock_nb_context

    os.makedirs(Path(self.temp_dir) / "DecodedFiles")
    os.makedirs(Path(self.temp_dir) / "FlatFiles")
    slide_entry = {
      "cosmx_run_id": "cosmx_run_id",
      "slide_id": "slide_id",
      "export_dir": self.temp_dir,
    }
    new_slide_entry = \
      collect_slide_metadata.function(
        slide_entry=slide_entry,
        matched_slide_ids=1)
    assert "slide_metadata_json" in new_slide_entry
    assert "flatfiles_dir" in new_slide_entry


  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf", return_value=[1, "project1"])
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_SLIDE_METADATA_EXTRACTION_TEMPLATE", return_value="project1")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_project_igf_id_for_analysis", return_value="project1")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_COUNT_QC_REPORT_TEMPLATE", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_QC_REPORT_IMAGE1", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.Notebook_runner")
  # @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_current_context")
  def test_generate_count_qc_report(self, mock_nb_runner, *args):
    # Setup Airflow context mock
    # mock_context = MagicMock()
    # mock_context.dag_run.conf.analysis_id = 1
    # mock_context.get.return_value = mock_context.dag_run
    # mock_context.dag_run.conf.get.return_value = 1
    # mock_get_context.return_value = mock_context

    mock_nb_context = MagicMock()
    mock_nb_context.execute_notebook_in_singularity.return_value = ["test", "test"]
    mock_nb_runner.return_value = mock_nb_context

    os.makedirs(Path(self.temp_dir) / "FlatFiles")

    slide_entry = {
      "cosmx_run_id": "cosmx_run_id",
      "slide_id": "slide_id",
      "export_dir": self.temp_dir,
      "slide_metadata_json": self.temp_dir
    }
    new_slide_entry = \
      generate_count_qc_report.function(slide_entry=slide_entry)
    assert "json_output" in new_slide_entry

  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf", return_value=[1, "project1"])
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.check_file_path")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.copy_local_file")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_COUNT_FOV_REPORT_TEMPLATE", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.COSMX_QC_REPORT_IMAGE1", return_value="/tmp")
  @patch("igf_airflow.utils.dag43_cosmx_export_and_qc_utils.Notebook_runner")
  def test_generate_fov_qc_report(self, mock_nb_runner, *args):
    mock_nb_context = MagicMock()
    mock_nb_context.execute_notebook_in_singularity.return_value = ["test", "test"]
    mock_nb_runner.return_value = mock_nb_context
    ## slide metadata file
    slide_metadata_json = \
      Path(self.temp_dir) / "metadata.json"
    os.makedirs(Path(self.temp_dir) / "FlatFiles")
    with open(slide_metadata_json, "w") as fp:
      json.dump({"panel_name": "AAA"}, fp)

    slide_entry = {
      "cosmx_run_id": "cosmx_run_id",
      "slide_id": "slide_id",
      "export_dir": self.temp_dir,
      "slide_metadata_json": slide_metadata_json
    }
    new_slide_entry = \
      generate_fov_qc_report.function(slide_entry=slide_entry)
    assert "slide_metadata_json" in new_slide_entry
    assert new_slide_entry.get("slide_metadata_json") is not None
    assert new_slide_entry.get("slide_metadata_json") == slide_metadata_json

  def test_copy_slide_data_to_globus(self):
    assert False, "Test not implemented"

  def test_register_db_data(self):
    assert False, "Test not implemented"

  def test_fetch_slide_annotations_from_design_file(self):
    design_file = Path(self.temp_dir) / "design1.yaml"
    design_data = {
      "run_metadata": [
        {"cosmx_run_id": "IGF_PROJECT_ID_AND_RUN_ID",
         "export_directory_path": "PATH_OF_DATA_EXPORT"}],
      "analysis_metadata": {
        "run_type": "RNA",
        "annotation": [
          {"cosmx_slide_id": "COSMX_SLIDE_ID_1",
           "tissue_annotation": "TISSUE_ANNOTATION",
           "tissue_ontology": "TISSUE_ONTOLOGY",
           "tissue_condition": "TISSUE_CONDITION"}]}}
    with open(design_file, "w") as fp:
      json.dump(design_data, fp)
    tissue_annotation, tissue_ontology, tissue_condition = \
      fetch_slide_annotations_from_design_file(
        design_file=design_file.as_posix(),
        cosmx_slide_id="COSMX_SLIDE_ID_1")
    assert tissue_annotation == "TISSUE_ANNOTATION"
    design_data = {
      "run_metadata": [
        {"cosmx_run_id": "IGF_PROJECT_ID_AND_RUN_ID",
         "export_directory_path": "PATH_OF_DATA_EXPORT"}],
      "analysis_metadata": {
        "run_type": "RNA"}}
    with open(design_file, "w") as fp:
      json.dump(design_data, fp)
    tissue_annotation, tissue_ontology, tissue_condition = \
      fetch_slide_annotations_from_design_file(
        design_file=design_file.as_posix(),
        cosmx_slide_id="COSMX_SLIDE_ID_1")
    assert tissue_annotation == "UNKNOWN"

  def test_load_cosmx_data_to_db(self):
    assert False, "Test not implemented"

  def test_fetch_cosmx_metadata_info(self):
    test_metadata_file = Path(self.temp_dir) / "test_slide_metadata.json"
    test_metadata = {
      "Run Number": "_xyz_abc.123",
      "Instrument": "aabbcc",
      "Expt Name": "ControlCenter",
      "SW Version": "",
      "FOV digits": "5",
      "Folder Structure Version": "2",
      "Analyte": "RNA",
      "Readout": "v1.0",
      "RNA nCoder file": "",
      "SpotFiles": "true",
      "Ch Order": "B, G, Y, R",
      "Ch HDR  ": "1,1,1,1,8",
      "Ch Thresh.": "2250,1500,1875,2250",
      "SNR Filter": "false",
      "SNR Thresh.": "2,2,2,2",
      "Reg Ch": "G",
      "Slot ID": "20250211_141001_S3",
      "Cycles": "8",
      "FOVs": "122",
      "FOV Range": "1-122",
      "Reporters": "27",
      "Reporter ID": "1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53",
      "Darks": "2",
      "Dark ID": "26,54",
      "Z-steps": "8",
      "Z-step Size (um)": "0.8",
      "NA": "1.1",
      "Image Rows": "4256",
      "Image Cols": "4256",
      "Image Binning": "1",
      "ImPixel_nm": "120.280945",
      "Upsampling Rate": "10",
      "Protein ZProj Method": "MAX",
      "TrimEdgesPx": "0",
      "BkgSubMethod": "DIRECT",
      "ShadingMethod": "INSTRUMENT",
      "LocalizationMethod": "MOMENT",
      "BkgSubPrctl": "1",
      "ImageCorrection": "false",
      "RadialDistortionCorrection": "true",
      "assay_type": "rna",
      "version": "v6",
      "Run_Tissue_name": "XXXXXX",
      "Panel": "(1.1) (1.1) Human RNA Xk Discovery"}
    with open(test_metadata_file, "w") as fp:
      json.dump(test_metadata, fp)
    fov_range, cosmx_platform_igf_id, slide_name, slide_run_date, panel_info, assay_type, version, metadata_json_entry = \
      fetch_cosmx_metadata_info(
        cosmx_metadata_json=test_metadata_file.as_posix())
    assert fov_range == "1-122"
    assert cosmx_platform_igf_id == "aabbcc"
    assert panel_info == "(1.1) (1.1) Human RNA Xk Discovery"
    assert assay_type == "rna"
    assert version == "v6"
    assert "Run_Tissue_name"  in metadata_json_entry
    assert metadata_json_entry.get("Run_Tissue_name") == "XXXXXX"
    assert slide_name == "XXXXXX"
    assert slide_run_date == parse("20250211141001")



  def test_generate_additional_qc_report1(self):
    assert generate_additional_qc_report1([{}]) is not None

  def test_generate_additional_qc_report2(self):
    assert generate_additional_qc_report2([{}]) is not None

  def test_upload_reports_to_portal(self):
    assert upload_reports_to_portal([{}]) is not None


if __name__=='__main__':
  unittest.main()