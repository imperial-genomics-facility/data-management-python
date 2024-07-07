import os
import json
import yaml
import subprocess
import unittest
import pandas as pd
from yaml import load, dump, SafeLoader, Dumper
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from igf_airflow.utils.dag42_curioseq_wrapper_utils import (
  merge_list_of_r1_and_r2_files,
  fetch_and_merge_fastqs_for_samples
)

DESIGN_YAML = """sample_metadata:
  IGFsampleA:
    barcode_file: /path/image
    experiment_date: 2024-07-03
    genome: GRCm39
  IGFsampleB:
    barcode_file: /path/image
    experiment_date: 2024-07-03
    genome: GRCm39
analysis_metadata:
  curioseeker_config:
    igenomes_base: /PATH/PIPELINE_RESOURCES/REF_GENOME/CURIOSEEKER
    """

class TestDag42_curioseq_wrapper_utilsA(unittest.TestCase):
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
    project_data = [{'project_igf_id':'IGFQprojectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id': 'IGFsampleA',
      'project_igf_id': 'IGFQprojectA',
      'species_name': 'HG38'
    }, {
      'sample_igf_id': 'IGFsampleB',
      'project_igf_id': 'IGFQprojectA',
      'species_name': 'UNKNOWN'
    }, {
      'sample_igf_id': 'IGFsampleC',
      'project_igf_id': 'IGFQprojectA',
      'species_name': 'HG38'
    }, {
      'sample_igf_id': 'IGFsampleD',
      'project_igf_id': 'IGFQprojectA',
      'species_name': 'UNKNOWN'
    }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id': 'IGFQprojectA',
      'sample_igf_id': 'IGFsampleA',
      'experiment_igf_id': 'IGFsampleA_MISEQ',
      'library_name': 'IGFsampleA',
      'library_source': 'TRANSCRIPTOMIC',
      'library_strategy': 'RNA-SEQ',
      'experiment_type': 'POLYA-RNA',
      'library_layout': 'PAIRED',
      'platform_name': 'MISEQ',
    }, {
      'project_igf_id': 'IGFQprojectA',
      'sample_igf_id': 'IGFsampleB',
      'experiment_igf_id': 'IGFsampleB_MISEQ',
      'library_name': 'IGFsampleB',
      'library_source': 'UNKNOWN',
      'library_strategy': 'UNKNOWN',
      'experiment_type': 'UNKNOWN',
      'library_layout': 'UNKNOWN',
      'platform_name': 'MISEQ',
    }, {
      'project_igf_id': 'IGFQprojectA',
      'sample_igf_id': 'IGFsampleC',
      'experiment_igf_id': 'IGFsampleC_MISEQ',
      'library_name': 'IGFsampleC',
      'library_source': 'TRANSCRIPTOMIC',
      'library_strategy': 'RNA-SEQ',
      'experiment_type': 'POLYA-RNA',
      'library_layout': 'PAIRED',
      'platform_name': 'MISEQ',
    }, {
      'project_igf_id': 'IGFQprojectA',
      'sample_igf_id': 'IGFsampleD',
      'experiment_igf_id': 'IGFsampleD_MISEQ',
      'library_name': 'IGFsampleD',
      'library_source': 'UNKNOWN',
      'library_strategy': 'UNKNOWN',
      'experiment_type': 'UNKNOWN',
      'library_layout': 'UNKNOWN',
      'platform_name': 'MISEQ',
    }]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id': 'IGFsampleA_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }, {
      'experiment_igf_id': 'IGFsampleA_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'lane_number': '2'
    }, {
      'experiment_igf_id': 'IGFsampleB_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }, {
      'experiment_igf_id': 'IGFsampleC_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }, {
      'experiment_igf_id': 'IGFsampleD_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    file_data = [
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_I1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_I2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_I1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_I2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_R2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_R2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_I1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_I2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S1_L001_R2_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_I1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_I2_001.fastq.gz')}]
    for f_entry  in file_data:
      file_path = f_entry.get("file_path")
      ## make dir
      os.makedirs(
        os.path.dirname(file_path),
        exist_ok=True)
      ## create fastq
      fastq_path = file_path.replace(".gz", "")
      with open(fastq_path,'w') as fq:
        fq.write(f"@header\nATCG{os.path.basename(file_path)}\n+\n####\n")
      ## gzip it
      subprocess.check_call(["gzip", fastq_path])
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }]
    collection_files_data = [{
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_I1_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_I2_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R1_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R2_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_I1_001.fastq.gz')
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_2',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_I2_001.fastq.gz')
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_R1_001.fastq.gz')
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_R2_001.fastq.gz')
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz')
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz')
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_R1_001.fastq.gz')
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_R2_001.fastq.gz')
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_I1_001.fastq.gz')
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleC/IGFsampleC_S1_L001_I2_001.fastq.gz')
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_R1_001.fastq.gz')
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S1_L001_R2_001.fastq.gz')
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_I1_001.fastq.gz')
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': os.path.join(self.temp_dir, 'IGFSampleD/IGFsampleD_S2_L001_I2_001.fastq.gz')
    }]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()
    self.yaml_data = \
      load(DESIGN_YAML, Loader=SafeLoader)
    self.yaml_file = \
      os.path.join(
        self.temp_dir,
        'analysis_design.yaml')
    with open(self.yaml_file, 'w') as fp:
      dump(self.yaml_data, fp)

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_merge_list_of_r1_and_r2_files(self):
    r1_list =  [
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz')]
    r2_list = [
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz')]
    merged_r1_fastq, merged_r2_fastq = \
        merge_list_of_r1_and_r2_files(
          r1_list=r1_list,
          r2_list=r2_list)
    self.assertEqual(merged_r1_fastq, r1_list[0])
    self.assertEqual(merged_r2_fastq, r2_list[0])
    r1_list =  [
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz'),
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R1_001.fastq.gz')]
    r2_list = [
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz'),
      os.path.join(self.temp_dir, 'IGFSampleA/IGFsampleA_S1_L002_R2_001.fastq.gz')]
    merged_r1_fastq, merged_r2_fastq = \
        merge_list_of_r1_and_r2_files(
          r1_list=r1_list,
          r2_list=r2_list)
    self.assertEqual(os.path.basename(merged_r1_fastq), "merged_R1_001.fastq.gz")
    self.assertEqual(os.path.basename(merged_r2_fastq), "merged_R2_001.fastq.gz")
    r1_fastq_length = \
      subprocess.check_output(f"zcat {merged_r1_fastq}|wc -l", shell=True)
    self.assertEqual(int(r1_fastq_length.decode().strip()), 8)
    r1_fastq_data = \
      subprocess.check_output(f"zcat {merged_r1_fastq}", shell=True)
    r1_fastq_data = \
      r1_fastq_data.decode()
    self.assertTrue(
      "IGFsampleA_S1_L001_R1_001.fastq.gz" in r1_fastq_data and \
      "IGFsampleA_S1_L002_R1_001.fastq.gz" in r1_fastq_data)
    self.assertFalse(
      "IGFsampleA_S1_L001_R2_001.fastq.gz" in r1_fastq_data or \
      "IGFsampleA_S1_L002_R2_001.fastq.gz" in r1_fastq_data)
    r2_fastq_length = \
      subprocess.check_output(f"zcat {merged_r2_fastq}|wc -l", shell=True)
    self.assertEqual(int(r2_fastq_length.decode().strip()), 8)
    r2_fastq_data = \
      subprocess.check_output(f"zcat {merged_r2_fastq}", shell=True)
    r2_fastq_data = \
      r2_fastq_data.decode()
    self.assertFalse(
      "IGFsampleA_S1_L001_R1_001.fastq.gz" in r2_fastq_data or \
      "IGFsampleA_S1_L002_R1_001.fastq.gz" in r2_fastq_data)
    self.assertTrue(
      "IGFsampleA_S1_L001_R2_001.fastq.gz" in r2_fastq_data and \
      "IGFsampleA_S1_L002_R2_001.fastq.gz" in r2_fastq_data)


  def test_fetch_and_merge_fastqs_for_samples(self):
    output_dict = \
      fetch_and_merge_fastqs_for_samples(
        sample_dict={"IGFsampleA": {"barcode_file": "/path"}},
        db_config_file=self.dbconfig)
    self.assertTrue("IGFsampleA" in output_dict)
    self.assertTrue("barcode_file" in output_dict.get("IGFsampleA"))
    self.assertTrue("R1" in output_dict.get("IGFsampleA"))
    self.assertTrue("R2" in output_dict.get("IGFsampleA"))
    r1_fastq = output_dict.get("IGFsampleA").get("R1")
    r2_fastq = output_dict.get("IGFsampleA").get("R2")
    self.assertTrue(r1_fastq is not None and r2_fastq is not None)
    r1_fastq_length = \
      subprocess.check_output(f"zcat {r1_fastq}|wc -l", shell=True)
    self.assertEqual(int(r1_fastq_length.decode().strip()), 8)
    r1_fastq_data = \
      subprocess.check_output(f"zcat {r1_fastq}", shell=True)
    r1_fastq_data = \
      r1_fastq_data.decode()
    self.assertTrue(
      "IGFsampleA_S1_L001_R1_001.fastq.gz" in r1_fastq_data and \
      "IGFsampleA_S1_L002_R1_001.fastq.gz" in r1_fastq_data)
    self.assertFalse(
      "IGFsampleA_S1_L001_R2_001.fastq.gz" in r1_fastq_data or \
      "IGFsampleA_S1_L002_R2_001.fastq.gz" in r1_fastq_data)
    r2_fastq_length = \
      subprocess.check_output(f"zcat {r2_fastq}|wc -l", shell=True)
    self.assertEqual(int(r2_fastq_length.decode().strip()), 8)
    r2_fastq_data = \
      subprocess.check_output(f"zcat {r2_fastq}", shell=True)
    r2_fastq_data = \
      r2_fastq_data.decode()
    self.assertFalse(
      "IGFsampleA_S1_L001_R1_001.fastq.gz" in r2_fastq_data or \
      "IGFsampleA_S1_L002_R1_001.fastq.gz" in r2_fastq_data)
    self.assertTrue(
      "IGFsampleA_S1_L001_R2_001.fastq.gz" in r2_fastq_data and \
      "IGFsampleA_S1_L002_R2_001.fastq.gz" in r2_fastq_data)
    output_dict = \
      fetch_and_merge_fastqs_for_samples(
        sample_dict={"IGFsampleB": {"barcode_file": "/path"}},
        db_config_file=self.dbconfig)
    self.assertTrue("IGFsampleB" in output_dict)
    self.assertTrue("barcode_file" in output_dict.get("IGFsampleB"))
    self.assertTrue("R1" in output_dict.get("IGFsampleB"))
    self.assertTrue("R2" in output_dict.get("IGFsampleB"))
    r1_fastq = output_dict.get("IGFsampleB").get("R1")
    r2_fastq = output_dict.get("IGFsampleB").get("R2")
    self.assertTrue(r1_fastq is not None and r2_fastq is not None)
    self.assertEqual(os.path.basename(r1_fastq), 'IGFsampleB_S2_L001_R1_001.fastq.gz')
    self.assertEqual(os.path.basename(r2_fastq), 'IGFsampleB_S2_L001_R2_001.fastq.gz')


if __name__=='__main__':
  unittest.main()