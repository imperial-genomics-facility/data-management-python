import os
import json
import yaml
import zipfile
import unittest
import pandas as pd
from yaml import Loader, Dumper
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import (
    get_temp_dir,
    check_file_path,
    remove_dir)
from igf_airflow.utils.dag33_geomx_processing_util import (
    fetch_analysis_yaml_and_dump_to_a_file,
    extract_geomx_config_files_from_zip,
    get_fastq_for_samples_and_dump_in_json_file,
    read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs,
    create_sample_translation_file_for_geomx_script,
    fetch_geomx_params_from_analysis_design,
    create_geomx_dcc_run_script,
    calculate_md5sum_for_analysis_dir,
    compare_dcc_output_dir_with_design_file,
    collect_analysis_dir)

class TestDag33_geomx_processing_util_utilsA(unittest.TestCase):
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
    ## add project data
    project_data = [{'project_igf_id': 'projectA'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    ## add analysis data
    aa = \
      AnalysisAdaptor(**{'session': base.session})
    self.analysis_design_data = """
    sample_metadata:
      sampleA:
        condition: control
      sampleB:
        condition: treatment
    analysis_metadata:
      ref:
        species: homo_sapiens
        release: 106
        build: GRCh38
      diffexp:
        contrasts:
          control-vs-treatment:
            - control
            - treatment
        model: ~condition
    """
    analysis_data1 = [{
      'project_igf_id': 'projectA',
      'analysis_name': 'analysis_1',
      'analysis_type': 'pipelineA',
      'analysis_description': json.dumps(yaml.load(self.analysis_design_data, Loader=Loader))}]
    aa.store_analysis_data(
      data=analysis_data1)
    analysis_data2 = [{
      'project_igf_id': 'projectA',
      'analysis_name': 'analysis_2',
      'analysis_type': 'pipelineB',
      'analysis_description': json.dumps(yaml.load(self.analysis_design_data, Loader=Loader))}]
    aa.store_analysis_data(
      data=analysis_data2)
    ## add pipeline data
    pipeline_data = [{
      "pipeline_name": "pipelineA",
      "pipeline_type": "AIRFLOW",
      "pipeline_db": "postgres"}]
    pl = \
      PipelineAdaptor(**{'session': base.session})
    pl.store_pipeline_data(
      data=pipeline_data)
    pipeline_seed_data = [{
      'pipeline_name': 'pipelineA',
      'seed_id': 1,
      'seed_table': 'analysis'}]
    pl.create_pipeline_seed(
      data=pipeline_seed_data)
    base.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_analysis_yaml_and_dump_to_a_file(self):
    yaml_file = \
      fetch_analysis_yaml_and_dump_to_a_file(
        analysis_id=1,
        pipeline_name='pipelineA',
        dbconfig_file=self.dbconfig)
    self.assertIsNone(check_file_path(yaml_file))
    with open(yaml_file, 'r') as fp:
      config_yaml_data = yaml.load(fp, Loader=Loader)
    self.assertTrue('sample_metadata' in config_yaml_data)
    self.assertTrue('sampleA' in config_yaml_data.get('sample_metadata'))
    self.assertTrue('condition' in config_yaml_data.get('sample_metadata').get('sampleA'))
    self.assertEqual(config_yaml_data.get('sample_metadata').get('sampleA').get('condition'), 'control')
    self.assertTrue('analysis_metadata' in config_yaml_data)
    self.assertTrue('ref' in config_yaml_data.get('analysis_metadata'))
    with self.assertRaises(Exception):
      fetch_analysis_yaml_and_dump_to_a_file(
        analysis_id=2,
        pipeline_name='pipelineA',
        dbconfig_file=self.dbconfig)

  def test_collect_analysis_dir(self):
    output_dir = os.path.join(self.temp_dir, "output")
    os.makedirs(output_dir)
    hpc_dir = os.path.join(self.temp_dir, "hpc")
    os.makedirs(hpc_dir)
    for i in ('a.txt', 'b.txt', 'c.txt'):
      with open(os.path.join(output_dir, i), 'w') as fp:
        fp.write('aaa')
    target_dir_path, project_igf_id, date_tag = \
      collect_analysis_dir(
        analysis_id=1,
        dag_name='pipeline_1',
        dir_path=output_dir,
        db_config_file=self.dbconfig,
        hpc_base_path=hpc_dir)
    self.assertTrue(os.path.exists(target_dir_path))
    self.assertEqual('projectA', project_igf_id)

class TestDag33_geomx_processing_util_utilsB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.ini_file = os.path.join(self.temp_dir, 'test.ini')
    self.lab_file = os.path.join(self.temp_dir, 'test.LabWorksheet.txt')
    with open(self.ini_file, 'w') as  fp:
      fp.write('INIFILE')
    with open(self.lab_file, 'w') as  fp:
      fp.write('LABFILE')
    self.zip_file = os.path.join(self.temp_dir, 'test_geomx.zip')
    with zipfile.ZipFile(self.zip_file, 'w') as myzip:
      myzip.write(filename=self.ini_file, arcname=os.path.basename(self.ini_file))
      myzip.write(filename=self.lab_file, arcname=os.path.basename(self.lab_file))

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_extract_geomx_config_files_from_zip(self):
    config_file, labworksheet_file = \
      extract_geomx_config_files_from_zip(zip_file=self.zip_file)
    #self.assertEqual(os.path.basename(config_file), os.path.basename(self.ini_file))
    #self.assertEqual(os.path.basename(labworksheet_file), os.path.basename(self.lab_file))
    self.assertEqual(os.path.basename(config_file), 'geomx_project.ini')
    self.assertEqual(os.path.basename(labworksheet_file), 'geomx_project_LabWorksheet.txt')
    with open(config_file) as fp:
      config_file_data1 =  fp.read()
    with open(self.ini_file) as fp:
      config_file_data2 =  fp.read()
    self.assertEqual(config_file_data1, config_file_data2)
    with open(labworksheet_file) as fp:
      labworksheet_file_data1 =  fp.read()
    with open(self.lab_file) as fp:
      labworksheet_file_data2 =  fp.read()
    self.assertEqual(labworksheet_file_data1, labworksheet_file_data2)
    with self.assertRaises(Exception):
      extract_geomx_config_files_from_zip(zip_file='NOT_A_zip')


class TestDag33_geomx_processing_util_utilsC(unittest.TestCase):
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
    platform_data = [{
      "platform_igf_id" : "M03291",
      "model_name" : "MISEQ",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id": "M03291",
      "flowcell_type": "MISEQ",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE"}]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'flowcell_id': '000000000-BRN47',
      'platform_igf_id': 'M03291',
      'flowcell': 'MISEQ'}]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{'project_igf_id':'projectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id': 'sampleA',
      'project_igf_id': 'projectA',
      'species_name': 'HG38'
    },{
      'sample_igf_id': 'sampleB',
      'project_igf_id': 'projectA',
      'species_name': 'UNKNOWN'
    }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleA',
      'experiment_igf_id': 'sampleA_MISEQ',
      'library_name': 'sampleA',
      'library_source': 'TRANSCRIPTOMIC',
      'library_strategy': 'RNA-SEQ',
      'experiment_type': 'POLYA-RNA',
      'library_layout': 'PAIRED',
      'platform_name': 'MISEQ',
    },{
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleB',
      'experiment_igf_id': 'sampleB_MISEQ',
      'library_name': 'sampleB',
      'library_source': 'UNKNOWN',
      'library_strategy': 'UNKNOWN',
      'experiment_type': 'UNKNOWN',
      'library_layout': 'UNKNOWN',
      'platform_name': 'MISEQ',
    }]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id': 'sampleA_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'sampleA_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    },{
      'experiment_igf_id': 'sampleB_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'sampleB_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    file_data = [
      {'file_path': '/path/SampleA/sampleA_S1_L001_R1_001.fastq.gz'},
      {'file_path': '/path/SampleA/sampleA_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/SampleA/sampleA_S1_L001_I1_001.fastq.gz'},
      {'file_path': '/path/SampleA/sampleA_S1_L001_I2_001.fastq.gz'},
      {'file_path': '/path/SampleB/sampleB_S2_L001_R1_001.fastq.gz'},
      {'file_path': '/path/SampleB/sampleA_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/SampleB/sampleB_S2_L001_I1_001.fastq.gz'},
      {'file_path': '/path/SampleB/sampleB_S2_L001_I2_001.fastq.gz'}]
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }]
    collection_files_data = [{
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleA/sampleA_S1_L001_R1_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleA/sampleA_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleA/sampleA_S1_L001_I1_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleA/sampleA_S1_L001_I2_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleB/sampleB_S2_L001_R1_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleB/sampleA_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleB/sampleB_S2_L001_I1_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/SampleB/sampleB_S2_L001_I2_001.fastq.gz'
    }]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()
    self.yaml_data = """
    sample_metadata:
      sampleA:
        condition: control
      sampleB:
        condition: treatment
    analysis_metadata:
      ref:
        species: homo_sapiens
        release: 106
        build: GRCh38
      diffexp:
        contrasts:
          control-vs-treatment:
            - control
            - treatment
        model: ~condition
    """
    self.yaml_file = \
      os.path.join(
        self.temp_dir,
        'analysis_design.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write(self.yaml_data)

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)


  def test_get_fastq_for_samples_and_dump_in_json_file(self):
    with open(os.path.join(self.temp_dir, 'test.yaml'), 'w') as fp:
      fp.write(self.yaml_data)
    fastq_list_json = \
      get_fastq_for_samples_and_dump_in_json_file(
        design_file=os.path.join(self.temp_dir, 'test.yaml'),
        db_config_file=self.dbconfig)
    with open(fastq_list_json, 'r') as fp:
      json_data = json.load(fp)
    self.assertEqual(len(json_data), 8)
    sample_df = pd.DataFrame(json_data)
    self.assertTrue('/path/SampleB/sampleB_S2_L001_R1_001.fastq.gz' in sample_df['file_path'].values)

  def test_read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(self):
    with open(os.path.join(self.temp_dir, 'test.yaml'), 'w') as fp:
      fp.write(self.yaml_data)
    fastq_list_json = \
      get_fastq_for_samples_and_dump_in_json_file(
        design_file=os.path.join(self.temp_dir, 'test.yaml'),
        db_config_file=self.dbconfig)
    symlink_dir = \
      read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(
        fastq_list_json=fastq_list_json)
    self.assertEqual(len(os.listdir(symlink_dir)), 4)
    self.assertTrue('sampleA_S1000_L001_R2_001.fastq.gz' in os.listdir(symlink_dir))

  def test_create_sample_translation_file_for_geomx_script(self):
    design_file = os.path.join(self.temp_dir, 'test.yaml')
    yaml_data = """
    sample_metadata:
      sampleA:
        dsp_id: dsp1
      sampleB:
        dsp_id: dsp2
    analysis_metadata:
      ref:
        species: homo_sapiens
        release: 106
        build: GRCh38
    """
    with open(design_file, 'w') as fp:
      fp.write(yaml_data)
    sample_translation_file = \
      create_sample_translation_file_for_geomx_script(
        design_file=design_file)
    self.assertIsNone(check_file_path(sample_translation_file))
    df = pd.read_csv(sample_translation_file, header=None)
    df.columns = ['dsp_id', 'sample_id']
    self.assertEqual(df[df['sample_id']=='sampleA']['dsp_id'].values.tolist()[0], 'dsp1')

  def test_fetch_geomx_params_from_analysis_design(self):
    design_file = os.path.join(self.temp_dir, 'test.yaml')
    yaml_data = """
    sample_metadata:
      sampleA:
        dsp_id: dsp1
      sampleB:
        dsp_id: dsp2
    analysis_metadata:
      geomx_dcc_params:
        - "--threads=8"
    """
    with open(design_file, 'w') as fp:
      fp.write(yaml_data)
    params_list = \
      fetch_geomx_params_from_analysis_design(
        design_file=design_file)
    self.assertEqual(len(params_list), 1)
    self.assertTrue("--threads=8" in params_list)

  def test_create_geomx_dcc_run_script(self):
    design_file = os.path.join(self.temp_dir, 'test.yaml')
    yaml_data = """
    sample_metadata:
      sampleA:
        dsp_id: dsp1
      sampleB:
        dsp_id: dsp2
    analysis_metadata:
      geomx_dcc_params:
        - "--threads=8"
    """
    with open(design_file, 'w') as fp:
      fp.write(yaml_data)
    with open(os.path.join(self.temp_dir, "geomx_ngs_pipeline_exe"), 'w') as fp:
      fp.write('a')
    os.makedirs(os.path.join(self.temp_dir, "test_dir"))
    with open(os.path.join(self.temp_dir, 'test.ini'), 'w') as fp:
      fp.write('a')
    script_file, output_dir = \
        create_geomx_dcc_run_script(
            geomx_script_template="template/geomx/geomx_ngs_pipeline.sh",
            geomx_ngs_pipeline_exe=os.path.join(self.temp_dir, "geomx_ngs_pipeline_exe"),
            design_file=design_file,
            symlink_dir=os.path.join(self.temp_dir, "test_dir"),
            config_file_dict={'config_ini_file': os.path.join(self.temp_dir, 'test.ini')})
    self.assertIsNone(check_file_path(script_file))
    self.assertIsNone(check_file_path(output_dir))
    with open(script_file, 'r') as fp:
      script_data = fp.read()
    self.assertTrue(os.path.join(self.temp_dir, "geomx_ngs_pipeline_exe") in script_data)
    self.assertTrue(os.path.join(self.temp_dir, 'test.ini') in script_data)
    self.assertTrue(f'--out={output_dir}' in script_data)

  def test_compare_dcc_output_dir_with_design_file(self):
    design_file = os.path.join(self.temp_dir, 'test.yaml')
    yaml_data = """
    sample_metadata:
      sampleA:
        dsp_id: dsp1
      sampleB:
        dsp_id: dsp2
    analysis_metadata:
      geomx_dcc_params:
        - "--threads=8"
    """
    with open(design_file, 'w') as fp:
      fp.write(yaml_data)
    output_dir = os.path.join(self.temp_dir, "output")
    os.makedirs(output_dir)
    for i in ('sampleA.dcc', 'sampleB.dcc'):
      with open(os.path.join(output_dir, i), 'w') as fp:
        fp.write('aaa')
    self.assertIsNone(
      compare_dcc_output_dir_with_design_file(
        dcc_output_dir=output_dir,
        design_file=design_file))
    for i in ('sampleC.dcc',):
      with open(os.path.join(output_dir, i), 'w') as fp:
        fp.write('aaa')
    with self.assertRaises(Exception):
      compare_dcc_output_dir_with_design_file(
        dcc_output_dir=output_dir,
        design_file=design_file)

  def test_calculate_md5sum_for_analysis_dir(self):
    output_dir = os.path.join(self.temp_dir, "output")
    os.makedirs(output_dir)
    for i in ('a.txt', 'b.txt', 'c.txt'):
      with open(os.path.join(output_dir, i), 'w') as fp:
        fp.write('aaa')
    calculate_md5sum_for_analysis_dir(dir_path=output_dir)
    md5_list= os.path.join(output_dir, "file_manifest.md5")
    self.assertIsNone(check_file_path(md5_list))
    with open(md5_list, 'r') as fp:
      md5_data = fp.read()
    self.assertTrue(os.path.join(output_dir, 'a.txt') in md5_data)
    self.assertTrue(os.path.join(output_dir, 'b.txt') in md5_data)
    self.assertTrue(os.path.join(output_dir, 'c.txt') in md5_data)



if __name__=='__main__':
  unittest.main()