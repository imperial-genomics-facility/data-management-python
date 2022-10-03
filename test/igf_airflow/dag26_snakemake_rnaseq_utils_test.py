import os
import json
import yaml
import unittest
import pandas as pd
from yaml import Loader, Dumper
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.igfTables import Analysis
from igf_data.igfdb.igfTables import Pipeline_seed
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import parse_design_and_build_inputs_for_snakemake_rnaseq
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import parse_analysus_design_and_get_metadata
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import prepare_sample_and_units_tsv_for_snakemake_rnaseq
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import check_and_seed_analysis_pipeline
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import fetch_analysis_design
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import calculate_analysis_name
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import load_analysis_and_build_collection
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import copy_analysis_to_globus_dir


class TestDag26_snakemake_rnaseq_utilsA(unittest.TestCase):
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
      {'file_path': '/path/sampleA_S1_L001_R1_001.fastq.gz'},
      {'file_path': '/path/sampleA_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/sampleA_S1_L001_I1_001.fastq.gz'},
      {'file_path': '/path/sampleA_S1_L001_I2_001.fastq.gz'},
      {'file_path': '/path/sampleB_S2_L001_R1_001.fastq.gz'},
      {'file_path': '/path/sampleB_S2_L001_R2_001.fastq.gz'},
      {'file_path': '/path/sampleB_S2_L001_I1_001.fastq.gz'},
      {'file_path': '/path/sampleB_S2_L001_I2_001.fastq.gz'}]
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
      'file_path': '/path/sampleA_S1_L001_R1_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L001_I1_001.fastq.gz'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleA_S1_L001_I2_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S2_L001_R1_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S2_L001_R2_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S2_L001_I1_001.fastq.gz'
    }, {
      'name': 'sampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/sampleB_S2_L001_I2_001.fastq.gz'
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

  def test_parse_analysus_design_and_get_metadata(self):
    sample_metadata, analysis_metadata = \
      parse_analysus_design_and_get_metadata(
        input_design_yaml=self.yaml_data)
    self.assertTrue('sampleA' in sample_metadata)
    self.assertTrue(isinstance(sample_metadata.get('sampleA'), dict))
    self.assertTrue('condition' in sample_metadata.get('sampleA'))
    self.assertTrue('ref' in analysis_metadata)

  def test_prepare_sample_and_units_tsv_for_snakemake_rnaseq(self):
    sample_metadata, analysis_metadata = \
      parse_analysus_design_and_get_metadata(
        input_design_yaml=self.yaml_data)
    fastq_list = \
      get_fastq_and_run_for_samples(
        dbconfig_file=self.dbconfig,
        sample_igf_id_list=list(sample_metadata.keys()))
    samples_tsv_list, unites_tsv_list = \
      prepare_sample_and_units_tsv_for_snakemake_rnaseq(
        sample_metadata=sample_metadata,
        fastq_list=fastq_list)
    samples_tsv_df = \
      pd.DataFrame(samples_tsv_list)
    self.assertTrue('sample_name' in samples_tsv_df.columns)
    self.assertTrue('condition' in samples_tsv_df.columns)
    sampleA = samples_tsv_df[samples_tsv_df['sample_name'] == 'sampleA']
    self.assertEqual(len(sampleA.index), 1)
    self.assertEqual(sampleA['condition'].values[0], 'control')
    unites_tsv_df = \
      pd.DataFrame(unites_tsv_list)
    self.assertTrue('sample_name' in unites_tsv_df.columns)
    self.assertTrue('fq1' in unites_tsv_df.columns)
    self.assertTrue('fq2' in unites_tsv_df.columns)
    self.assertTrue('unit_name' in unites_tsv_df.columns)
    sampleA = \
      unites_tsv_df[unites_tsv_df['sample_name'] == 'sampleA']
    self.assertEqual(len(sampleA.index), 1)
    sampleB = \
      unites_tsv_df[unites_tsv_df['sample_name'] == 'sampleB']
    self.assertEqual(len(sampleB.index), 1)
    self.assertEqual(sampleA['fq1'].values[0], '/path/sampleA_S1_L001_R1_001.fastq.gz')
    self.assertEqual(sampleA['fq2'].values[0], '/path/sampleA_S1_L001_R2_001.fastq.gz')
    self.assertEqual(sampleA['unit_name'].values[0], '000000000-BRN47_1')
    self.assertEqual(sampleB['fq1'].values[0], '/path/sampleB_S2_L001_R1_001.fastq.gz')
    self.assertEqual(sampleB['fq2'].values[0], '/path/sampleB_S2_L001_R2_001.fastq.gz')

  def test_parse_design_and_build_inputs_for_snakemake_rnaseq(self):
    config_yaml_file, samples_tsv_file, units_tsv_file = \
      parse_design_and_build_inputs_for_snakemake_rnaseq(
        input_design_yaml=self.yaml_data,
        dbconfig_file=self.dbconfig,
        work_dir=self.temp_dir)
    self.assertTrue(os.path.exists(config_yaml_file))
    self.assertTrue(os.path.exists(samples_tsv_file))
    self.assertTrue(os.path.exists(units_tsv_file))
    with open(config_yaml_file, 'r') as fp:
      config_yaml_data = yaml.load(fp, Loader=Loader)
    self.assertTrue('samples' in config_yaml_data)
    self.assertTrue('units' in config_yaml_data)
    self.assertTrue('ref' in config_yaml_data)
    samples_tsv_df = \
      pd.read_csv(
        samples_tsv_file,
        sep="\t")
    unites_tsv_df = \
      pd.read_csv(
        units_tsv_file,
        sep="\t")
    self.assertTrue('sample_name' in samples_tsv_df.columns)
    self.assertTrue('condition' in samples_tsv_df.columns)
    sampleA = samples_tsv_df[samples_tsv_df['sample_name'] == 'sampleA']
    self.assertEqual(len(sampleA.index), 1)
    self.assertEqual(sampleA['condition'].values[0], 'control')
    self.assertTrue('sample_name' in unites_tsv_df.columns)
    self.assertTrue('fq1' in unites_tsv_df.columns)
    self.assertTrue('fq2' in unites_tsv_df.columns)
    self.assertTrue('unit_name' in unites_tsv_df.columns)
    sampleA = \
      unites_tsv_df[unites_tsv_df['sample_name'] == 'sampleA']
    self.assertEqual(len(sampleA.index), 1)
    sampleB = \
      unites_tsv_df[unites_tsv_df['sample_name'] == 'sampleB']
    self.assertEqual(len(sampleB.index), 1)
    self.assertEqual(sampleA['fq1'].values[0], '/path/sampleA_S1_L001_R1_001.fastq.gz')
    self.assertEqual(sampleA['fq2'].values[0], '/path/sampleA_S1_L001_R2_001.fastq.gz')
    self.assertEqual(sampleA['unit_name'].values[0], '000000000-BRN47_1')
    self.assertEqual(sampleB['fq1'].values[0], '/path/sampleB_S2_L001_R1_001.fastq.gz')
    self.assertEqual(sampleB['fq2'].values[0], '/path/sampleB_S2_L001_R2_001.fastq.gz')

class TestDag26_snakemake_rnaseq_utilsB(unittest.TestCase):
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

  def test_check_and_seed_analysis_pipeline(self):
    ## don't change seed if its listed in no_change_status
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=1,
        pipeline_name='pipelineA',
        dbconf_json_path=self.dbconfig,
        new_status='RUNNING',
        seed_table='analysis',
        no_change_status=['RUNNING', 'SEEDED'])
    self.assertFalse(seed_status)
    aa = \
      AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    analysis = \
      aa.fetch_records(
        query=\
          aa.session.query(Analysis).
          filter(Analysis.analysis_name=='analysis_1').
          filter(Analysis.project_id==1),
        output_mode='one')
    pa = PipelineAdaptor(**{'session': aa.session})
    pipe_seed = \
      pa.check_seed_id_status(
        seed_id=analysis.analysis_id,
        seed_table='analysis')
    self.assertEqual(pipe_seed[0].get('status'), 'SEEDED')
    aa.close_session()
    ## change seed is its not listed
    seed_status = \
      check_and_seed_analysis_pipeline(
        analysis_id=1,
        pipeline_name='pipelineA',
        dbconf_json_path=self.dbconfig,
        new_status='RUNNING',
        seed_table='analysis',
        no_change_status=['RUNNING', 'FAILED', 'FINISHED'])
    self.assertTrue(seed_status)
    aa = \
      AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    analysis = \
      aa.fetch_records(
        query=\
          aa.session.query(Analysis).
          filter(Analysis.analysis_name=='analysis_1').
          filter(Analysis.project_id==1),
        output_mode='one')
    pa = PipelineAdaptor(**{'session': aa.session})
    pipe_seed = \
      pa.check_seed_id_status(
        seed_id=analysis.analysis_id,
        seed_table='analysis')
    self.assertEqual(pipe_seed[0].get('status'), 'RUNNING')
    aa.close_session()
    ## fail with missing pipeline name
    with self.assertRaises(Exception):
      seed_status = \
        check_and_seed_analysis_pipeline(
          analysis_id=1,
          pipeline_name='pipelineB',
          dbconf_json_path=self.dbconfig,
          new_status='RUNNING',
          seed_table='analysis',
          no_change_status=['FAILED', 'FINISHED'])
    ## fail with incorrect pipeline and analysis combination
    with self.assertRaises(Exception):
      seed_status = \
        check_and_seed_analysis_pipeline(
          analysis_id=2,
          pipeline_name='pipelineA',
          dbconf_json_path=self.dbconfig,
          new_status='RUNNING',
          seed_table='analysis',
          no_change_status=['FAILED', 'FINISHED'])
  def test_fetch_analysis_design(self):
    input_design_yaml = \
      fetch_analysis_design(
        analysis_id=1,
        pipeline_name='pipelineA',
        dbconfig_file=self.dbconfig)
    self.assertIsNotNone(input_design_yaml)
    self.assertEqual(
      yaml.load(input_design_yaml, Loader=Loader),
      yaml.load(self.analysis_design_data, Loader=Loader))


class TestDag26_snakemake_rnaseq_utilsC(unittest.TestCase):
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
    project_data = [{'project_igf_id':'projectA'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    aa = \
      AnalysisAdaptor(**{'session': base.session})
    analysis_data = [{
      'project_igf_id': 'projectA',
      'analysis_name': 'analysis 1 (day=23)',
      'analysis_type': 'pipeline_1',
      'analysis_description': '{"analysis_design":""}'}]
    aa.store_analysis_data(
      data=analysis_data)
    base.close_session()
    self.input_data_dir = get_temp_dir()
    self.output_data_dir = get_temp_dir()
    self.globus_dir = get_temp_dir()
    self.analysis_files = [
      'analysis.tsv',
      'analysis/analysis_tsv'
    ]
    os.makedirs(os.path.join(self.input_data_dir, 'analysis'))
    for i in self.analysis_files:
      file_path = \
        os.path.join(self.input_data_dir, i)
      with open(file_path, 'w') as fp:
        fp.write('aaaa')

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.input_data_dir)
    remove_dir(self.output_data_dir)
    remove_dir(self.globus_dir)

  def test_calculate_analysis_name(self):
    collection_name = \
      calculate_analysis_name(
        analysis_id=1,
        date_tag='2022_01_01',
        dbconfig_file=self.dbconfig)
    self.assertEqual(collection_name, f"analysis_1_day_23_1_2022_01_01")

  def test_load_analysis_and_build_collection(self):
    target_dir_path = \
      load_analysis_and_build_collection(
        collection_name='analysis_1',
        collection_type='type_1',
        collection_table='analysis',
        dbconfig_file=self.dbconfig,
        analysis_id=1,
        pipeline_name='pipeline_1',
        result_dir=self.input_data_dir,
        hpc_base_path=self.output_data_dir,
        date_tag='2022_01_01',
        analysis_dir_prefix='analysis')
    self.assertTrue(os.path.exists(target_dir_path))
    for i in self.analysis_files:
      target_path = \
        os.path.join(target_dir_path, i)
      self.assertTrue(os.path.exists(target_path))

  def test_copy_analysis_to_globus_dir(self):
    target_dir_path = \
      load_analysis_and_build_collection(
        collection_name='analysis_1',
        collection_type='type_1',
        collection_table='analysis',
        dbconfig_file=self.dbconfig,
        analysis_id=1,
        pipeline_name='pipeline_1',
        result_dir=self.input_data_dir,
        hpc_base_path=self.output_data_dir,
        date_tag='2022_01_01',
        analysis_dir_prefix='analysis')
    globus_analysis_dir = \
      copy_analysis_to_globus_dir(
        globus_root_dir=self.globus_dir,
        dbconfig_file=self.dbconfig,
        analysis_id=1,
        analysis_dir=target_dir_path,
        pipeline_name='pipeline_1',
        date_tag='2022_01_01',
        analysis_dir_prefix='analysis')
    self.assertTrue(os.path.exists(globus_analysis_dir))
    self.assertEqual(
      globus_analysis_dir,
      os.path.join(
        self.globus_dir,
        'projectA',
        'analysis',
        'pipeline_1',
        '2022_01_01',
        os.path.basename(target_dir_path) )
    )
    for i in self.analysis_files:
      target_path = \
        os.path.join(globus_analysis_dir, i)
      self.assertTrue(os.path.exists(target_path))




if __name__=='__main__':
  unittest.main()