import os
import json
import yaml
import zipfile
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
from igf_airflow.utils.dag41_spaceranger_visium_utils import (
    get_per_sample_analysis_groups,
    prepare_spaceranger_count_run_dir_and_script_file,
    prepare_spaceranger_aggr_run_dir_and_script
)

DESIGN_YAML = """sample_metadata:
  IGFsampleA:
    image: /path/image
    darkimage: null
    colorizedimage: null
    cytaimage: null
    slide: null
    area: null
    dapi-index: null
  IGFsampleB:
    image: /path/image
    darkimage: null
    colorizedimage: null
    cytaimage: null
    slide: null
    area: null
    dapi-index: null
analysis_metadata:
  spaceranger_count_config:
    - "--transcriptome=/path"
    - "--probe-set=/path"
    - "--filter-probes=true"
    - "--reorient-images=true"
    - "--create-bam=true"
  spaceranger_aggr_config:
    - "--normalize=mapped"
    """

class TestDag41_spaceranger_visium_utilsA(unittest.TestCase):
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
      {'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_I1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_I2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_R1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_R1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_I1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_I2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_R1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleD/IGFsampleD_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_I1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_I2_001.fastq.gz'}]
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
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
      'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_R1_001.fastq.gz'
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_I1_001.fastq.gz'
    }, {
      'name': 'IGFsampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleA/IGFsampleA_S1_L001_I2_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_R1_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz'
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_R1_001.fastq.gz'
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_I1_001.fastq.gz'
    }, {
      'name': 'IGFsampleC_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleC/IGFsampleC_S1_L001_I2_001.fastq.gz'
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_R1_001.fastq.gz'
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleD/IGFsampleD_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_I1_001.fastq.gz'
    }, {
      'name': 'IGFsampleD_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleD/IGFsampleD_S2_L001_I2_001.fastq.gz'
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

  def test_get_spaceranger_analysis_design_and_get_groups(self):
    unique_sample_groups = \
      get_per_sample_analysis_groups(self.yaml_file)
    self.assertEqual(len(unique_sample_groups), 2)
    self.assertEqual(type(unique_sample_groups[0]), dict)
    self.assertTrue(
      "IGFsampleA" in unique_sample_groups[0].get("sample_metadata") or \
      "IGFsampleA" in unique_sample_groups[1].get("sample_metadata"))
    self.assertTrue(
      "IGFsampleB" in unique_sample_groups[0].get("sample_metadata") or \
      "IGFsampleB" in unique_sample_groups[1].get("sample_metadata"))
    self.assertFalse(
      "IGFsampleC" in unique_sample_groups[0].get("sample_metadata") or \
      "IGFsampleC" in unique_sample_groups[1].get("sample_metadata"))
    self.assertTrue("spaceranger_count_config" in unique_sample_groups[0].get("analysis_metadata"))
    self.assertTrue("--transcriptome=/path" in unique_sample_groups[0].get("analysis_metadata").get("spaceranger_count_config"))

  def test_prepare_spaceranger_count_run_dir_and_script_file(self):
    unique_sample_groups = \
      get_per_sample_analysis_groups(
        self.yaml_file)
    sample_id, script_file, output_dir = \
      prepare_spaceranger_count_run_dir_and_script_file(
        sample_metadata=unique_sample_groups[0].get("sample_metadata"),
        analysis_metadata=unique_sample_groups[0].get("analysis_metadata"),
        db_config_file=self.dbconfig,
        run_script_template="template/spaceranger_template/spaceranger_count_run_script_v1.sh")
    self.assertTrue(os.path.exists(script_file))
    with open(script_file, 'r') as fp:
      script_rendered_data = fp.read()
    # sample_id = \
    #   list(unique_sample_groups[0].get("sample_metadata").keys())[0]
    self.assertTrue(
      sample_id in ["IGFsampleA", "IGFsampleB"])
    self.assertTrue(f"--id={sample_id}" in script_rendered_data)
    self.assertTrue(f"cd {os.path.basename(script_file)}")
    self.assertTrue("--fastqs=/path/IGFSampleA" in script_rendered_data or \
                    "--fastqs=/path/IGFSampleB" in script_rendered_data)
    self.assertFalse(f"/path/IGFSampleC" in script_rendered_data)
    self.assertTrue("--image=/path/image" in script_rendered_data)
    self.assertFalse("--darkimage" in script_rendered_data)
    self.assertEqual(os.path.basename(output_dir), sample_id)


class TestDag41_spaceranger_visium_utilsB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    sampleA_dir = os.path.join(self.temp_dir, "sampleA")
    os.makedirs(os.path.join(sampleA_dir, "outs", "spatial"), exist_ok=True)
    for f in ["molecule_info.h5", "cloupe.cloupe"]:
      f_path = os.path.join(sampleA_dir, "outs", f)
      with open(f_path, "w") as fp:
        fp.write("AAA")
    sampleB_dir = os.path.join(self.temp_dir, "sampleB")
    os.makedirs(os.path.join(sampleB_dir, "outs", "spatial"), exist_ok=True)
    for f in ["molecule_info.h5", "cloupe.cloupe"]:
      f_path = os.path.join(sampleB_dir, "outs", f)
      with open(f_path, "w") as fp:
        fp.write("AAA")
    self.spaceranger_count_dict = {
      "sampleA": sampleA_dir,
      "sampleB": sampleB_dir}


  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_prepare_spaceranger_aggr_run_dir_and_script(self):
    aggr_script_path, output_dir = \
      prepare_spaceranger_aggr_run_dir_and_script(
        spaceranger_count_dict=self.spaceranger_count_dict,
        spaceranger_aggr_script_template="template/spaceranger_template/spaceranger_aggr_run_script_v1.sh")
    self.assertTrue(os.path.exists(aggr_script_path))
    self.assertTrue(os.path.exists(os.path.dirname(output_dir)))
    script_data = open(aggr_script_path, "r").read()
    self.assertTrue("--id=ALL" in script_data)
    self.assertTrue(f"cd {os.path.dirname(output_dir)}" in script_data)

if __name__=='__main__':
  unittest.main()