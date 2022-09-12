import os
import yaml
import unittest
import pandas as pd
from yaml import Loader, Dumper
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import parse_design_and_build_inputs_for_snakemake_rnaseq
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import parse_analysus_design_and_get_metadata
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import prepare_sample_and_units_tsv_for_snakemake_rnaseq

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
    yaml_data = """
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
      fp.write(yaml_data)

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_parse_analysus_design_and_get_metadata(self):
    sample_metadata, analysis_metadata = \
      parse_analysus_design_and_get_metadata(
        input_design_yaml=self.yaml_file)
    self.assertTrue('sampleA' in sample_metadata)
    self.assertTrue(isinstance(sample_metadata.get('sampleA'), dict))
    self.assertTrue('condition' in sample_metadata.get('sampleA'))
    self.assertTrue('ref' in analysis_metadata)

  def test_prepare_sample_and_units_tsv_for_snakemake_rnaseq(self):
    sample_metadata, analysis_metadata = \
      parse_analysus_design_and_get_metadata(
        input_design_yaml=self.yaml_file)
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
        input_design_yaml=self.yaml_file,
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

if __name__=='__main__':
  unittest.main()