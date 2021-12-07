from tempfile import tempdir
import unittest, os, yaml
import pandas as pd
from igf_data.igfdb.igfTables import Base, Analysis
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import find_all_analysis_yaml_files
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import get_new_file_list
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import get_analysis_ids_from_csv
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import fetch_analysis_records
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import parse_analysis_yaml_and_load_to_db
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import get_dag_conf_for_analysis


class Dag18_upload_and_trigger_analysis_utils_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.yaml_file = os.path.join(self.temp_dir, 'a.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write('A')
    with open(os.path.join(self.temp_dir, 'a.yam'), 'w') as fp:
      fp.write('A')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_find_all_analysis_yaml_files(self):
    yaml_files = \
      find_all_analysis_yaml_files(self.temp_dir)
    self.assertEqual(len(yaml_files), 1)
    self.assertTrue(os.path.join(self.temp_dir, 'a.yaml') in yaml_files)

class Dag18_upload_and_trigger_analysis_utils_testB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.yaml_file = os.path.join(self.temp_dir, 'a.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write('A')
    with open(os.path.join(self.temp_dir, 'b.yaml'), 'w') as fp:
      fp.write('A')
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    fa = FileAdaptor(**dbparam)
    self.engine = fa.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = fa.get_session_class()
    fa.start_session()
    file_data = \
      pd.DataFrame([{'file_path': os.path.join(self.temp_dir, 'b.yaml')}])
    fa.store_file_and_attribute_data(
      data=file_data,
      autosave=True)
    fa.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_get_new_file_list(self):
    yaml_files = \
      find_all_analysis_yaml_files(self.temp_dir)
    new_files = \
      get_new_file_list(
        all_files=yaml_files,
        db_config_file=self.dbconfig)
    self.assertEqual(len(new_files), 1)
    self.assertTrue(os.path.join(self.temp_dir, 'a.yaml') in new_files)


class Dag18_upload_and_trigger_analysis_utils_testC(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    yaml_data = [{
      "analysis_name":"analysis_1",
      "analysis_type":"tenx_single_cell_immune_profiling",
      "project_igf_id": "project_a",
      "analysis_description": [{
        "r1-length": 26,
        "feature_type": "gene expression",
        "genome_build": "HG38",
        "reference": "HG38_gencode_v38_6.1.1",
        "cell_annotation_csv": "PanglaoDB_markers_27_Mar_2020.tsv",
        "sample_igf_id": "sample_a",
        "cell_marker_mode": "VDJ" 
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-t",
        "sample_igf_id": "sample_b"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-b",
        "sample_igf_id": "sample_c"
      }]
    },{
      "analysis_name":"analysis_2",
      "analysis_type":"tenx_single_cell_immune_profiling",
      "project_igf_id": "project_a",
      "analysis_description": [{
        "r1-length": 26,
        "feature_type": "gene expression",
        "genome_build": "HG38",
        "reference": "HG38_gencode_v38_6.1.1",
        "cell_annotation_csv": "PanglaoDB_markers_27_Mar_2020.tsv",
        "sample_igf_id": "sample_d",
        "cell_marker_mode": "VDJ"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-t",
        "sample_igf_id": "sample_e"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-b",
        "sample_igf_id": "sample_f"
      }]
    }]
    self.yaml_file = \
      os.path.join(self.temp_dir, 'data.yaml')
    with open(self.yaml_file, 'w') as fp:
      yaml.dump(yaml_data, fp)
    project_data = [{"project_igf_id":"project_a"}]
    pa = ProjectAdaptor(**{'session_class': self.session_class})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    pa.close_session()
    self.analysis_trigger_file = \
      os.path.join(self.temp_dir, 'trigger.csv')
    with open(self.analysis_trigger_file, 'w') as fp:
      fp.write('project_a,analysis_1')

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_parse_analysis_yaml_and_load_to_db(self):
    parse_analysis_yaml_and_load_to_db(
      analysis_yaml_file=self.yaml_file,
      db_config_file=self.dbconfig)
    aa = AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    results = \
      aa.fetch_analysis_record_by_analysis_name_and_project_igf_id(
        analysis_name="analysis_1",
        project_igf_id="project_a",
        output_mode='dataframe')
    aa.close_session()
    self.assertEqual(len(results.index), 1)
    self.assertTrue('analysis_type' in results.columns)
    self.assertEqual(results['analysis_type'].values[0], 'tenx_single_cell_immune_profiling')

  def test_get_analysis_ids_from_csv(self):
    trigger_data = \
      get_analysis_ids_from_csv(
        self.analysis_trigger_file)
    self.assertEqual(len(trigger_data), 1)
    self.assertTrue('analysis_1' in pd.DataFrame(trigger_data)['analysis_name'].values)

  def test_fetch_analysis_records(self):
    updated_analysis_records, errors = \
      fetch_analysis_records([
        {"project_igf_id":"project_a","analysis_name": "analysis_1"},
        {"project_igf_id":"project_a","analysis_name": "analysis_2"}],
        self.dbconfig)
    self.assertEqual(len(errors), 2)
    self.assertEqual(len(updated_analysis_records), 0)
    parse_analysis_yaml_and_load_to_db(
      analysis_yaml_file=self.yaml_file,
      db_config_file=self.dbconfig)
    updated_analysis_records, errors = \
      fetch_analysis_records([
        {"project_igf_id":"project_a","analysis_name": "analysis_1"},
        {"project_igf_id":"project_a","analysis_name": "analysis_2"}],
        self.dbconfig)
    self.assertEqual(len(errors), 0)
    self.assertEqual(len(updated_analysis_records), 2)
    df = pd.DataFrame(updated_analysis_records)
    self.assertEqual(
      df[df['analysis_name']=='analysis_1']['analysis_type'].values[0],
      'tenx_single_cell_immune_profiling')

  def test_get_dag_conf_for_analysis(self):
    analysis_list = [{
      "analysis_name":"analysis_1",
      "analysis_id":1,
      "analysis_type":"tenx_single_cell_immune_profiling",
      "project_igf_id": "project_a",
      "analysis_description": [{
        "r1-length": 26,
        "feature_type": "gene expression",
        "genome_build": "HG38",
        "reference": "HG38_gencode_v38_6.1.1",
        "cell_annotation_csv": "PanglaoDB_markers_27_Mar_2020.tsv",
        "sample_igf_id": "sample_a",
        "cell_marker_mode": "VDJ" 
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-t",
        "sample_igf_id": "sample_b"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-b",
        "sample_igf_id": "sample_c"
      }]
    },{
      "analysis_name":"analysis_2",
      "analysis_id":2,
      "analysis_type":"tenx_single_cell_immune_profiling",
      "project_igf_id": "project_a",
      "analysis_description": [{
        "r1-length": 26,
        "feature_type": "gene expression",
        "genome_build": "HG38",
        "reference": "HG38_gencode_v38_6.1.1",
        "cell_annotation_csv": "PanglaoDB_markers_27_Mar_2020.tsv",
        "sample_igf_id": "sample_d",
        "cell_marker_mode": "VDJ"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-t",
        "sample_igf_id": "sample_e"
      },{
        "r1-length": 26,
        "genome_build": "HG38",
        "reference": "refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0",
        "feature_type": "vdj-b",
        "sample_igf_id": "sample_f"
      }]
    }]
    analysis_detail = \
      get_dag_conf_for_analysis(
        analysis_list,
        analysis_name="analysis_1",
        index=0)
    self.assertTrue(isinstance(analysis_detail, dict))
    self.assertTrue('analysis_id' in analysis_detail)
    self.assertEqual(analysis_detail['analysis_id'], 1)
    with self.assertRaises(Exception):
      analysis_detail = \
        get_dag_conf_for_analysis(
          analysis_list,
          analysis_name="analysis_1",
          index=1)

if __name__=='__main__':
  unittest.main()