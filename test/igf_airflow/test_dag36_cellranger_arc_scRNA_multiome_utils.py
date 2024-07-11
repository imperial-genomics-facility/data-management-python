import os
import json
import yaml
import zipfile
import unittest
import pandas as pd
from airflow.models.taskinstance import TaskInstance
from unittest.mock import patch
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
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    parse_analysis_design_and_get_metadata)
from igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils import (
    prepare_cellranger_arc_run_dir_and_script_file,
    create_library_information_for_multiome_sample_group,
    configure_cellranger_arc_aggr,
    prepare_cellranger_arc_script,
    run_single_sample_scanpy_for_arc,
    merged_scanpy_report_for_arc)

DESIGN_YAML = """sample_metadata:
  IGFsampleA:
    library_type: Gene Expression
    cellranger_group: GRP1
  IGFsampleB:
    library_type: Chromatin Accessibility
    cellranger_group: GRP1
  IGFsampleC:
    library_type: Gene Expression
    cellranger_group: GRP2
  IGFsampleD:
    library_type: Chromatin Accessibility
    cellranger_group: GRP2
analysis_metadata:
  scanpy_config:
    TEMPLATE_FILE: /path/scanpy_single_sample_analysis_v0.0.6.3.ipynb
    IMAGE_FILE: /path/scanpy-notebook-image_v0.0.4.sif
    MITO_PREFIX: MT-
    RUN_SCRUBLET: true
    RUN_CELLCYCLE_SCORE: true
    CELL_MARKER_LIST: /path/PanglaoDB_markers_27_Mar_2020.tsv
    CELL_MARKER_SPECIES: HG38
    S_GENES: ''
    G2M_GENES: ''
    CELL_MARKER_MODE: NON-VDJ
  cellranger_arc_config:
    reference: /path/ref
    parameters:
      - --extra_params
  cellranger_arc_aggr_config:
    reference: /path/ref
    parameters:
      - --extra_params"""

class TestDag36_cellranger_arc_scRNA_multiome_utilsA(unittest.TestCase):
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
    self.ref_file = \
      os.path.join(self.temp_dir, 'ref')
    with open(self.ref_file, 'w') as fp:
      fp.write('AAAA')
    self.yaml_data = \
      load(DESIGN_YAML, Loader=SafeLoader)
    self.yaml_data['analysis_metadata']['cellranger_arc_config']['reference'] = \
      self.ref_file
    self.yaml_data['analysis_metadata']['cellranger_arc_aggr_config']['reference'] = \
      self.ref_file
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

  def test_prepare_cellranger_arc_run_dir_and_script_file(self):
    temp_dir = get_temp_dir()
    library_csv_file, script_file = \
      prepare_cellranger_arc_run_dir_and_script_file(
        sample_group='GRP1',
        work_dir=temp_dir,
        design_file=self.yaml_file,
        db_config_file=self.dbconfig,
        run_script_template='template/cellranger_template/cellranger_arc_run_script_v1.sh')
    df = pd.read_csv(library_csv_file)
    self.assertEqual(len(df.index), 2)
    self.assertEqual(df.columns.tolist(), ['fastqs', 'sample', 'library_type'])
    self.assertTrue('IGFsampleA' in df['sample'].values.tolist())
    self.assertTrue('IGFsampleB' in df['sample'].values.tolist())
    self.assertTrue('/path/IGFSampleA' in df['fastqs'].values.tolist())
    self.assertTrue('/path/IGFSampleB' in df['fastqs'].values.tolist())
    self.assertEqual(
      df[df['fastqs']=='/path/IGFSampleB']['library_type'].values[0],
      'Chromatin Accessibility')
    with open(script_file, 'r') as fp:
      script_data=fp.read()
    self.assertTrue(f'--libraries={library_csv_file}' in script_data)
    self.assertTrue(f'--reference={self.ref_file} --extra_params' in script_data)

  def test_create_library_information_for_multiome_sample_group(self):
    with open(self.yaml_file, 'r') as fp:
      input_design_yaml=fp.read()
    sample_metadata, _ = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=input_design_yaml)
    sample_library_list = \
      create_library_information_for_multiome_sample_group(
        sample_group='GRP1',
        sample_metadata=sample_metadata,
        db_config_file=self.dbconfig)
    df = \
      pd.DataFrame(sample_library_list)
    self.assertEqual(len(df.index), 2)
    self.assertEqual(df.columns.tolist(), ['fastqs', 'sample', 'library_type'])
    self.assertTrue('IGFsampleA' in df['sample'].values.tolist())
    self.assertTrue('IGFsampleB' in df['sample'].values.tolist())
    self.assertTrue('/path/IGFSampleA' in df['fastqs'].values.tolist())
    self.assertTrue('/path/IGFSampleB' in df['fastqs'].values.tolist())
    self.assertEqual(
      df[df['fastqs']=='/path/IGFSampleB']['library_type'].values[0],
      'Chromatin Accessibility')

  def test_prepare_cellranger_arc_script(self):
    design_dict = {
      'analysis_design': self.yaml_file}
    with patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.DATABASE_CONFIG_FILE",
               self.dbconfig):
      with patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.CELLRANGER_ARC_SCRIPT_TEMPLATE",
                 "template/cellranger_template/cellranger_arc_run_script_v1.sh"):
        output_dict = \
          prepare_cellranger_arc_script.function(
            sample_group='GRP1',
            design_dict=design_dict)
        self.assertIn("sample_group", output_dict)
        self.assertIn("run_script", output_dict)
        self.assertIn("output_dir", output_dict)
        self.assertEqual(output_dict.get("sample_group"), 'GRP1')
        self.assertTrue(os.path.exists(output_dict.get("run_script")))

  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.get_current_context")
  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.get_project_igf_id_for_analysis",
         return_value="AAA")
  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.fetch_analysis_name_for_analysis_id",
         return_value="BBB")
  def test_run_single_sample_scanpy_for_arc(self, *args):
    design_dict = {
      'analysis_design': self.yaml_file}
    output_notebook_path = os.path.join(self.temp_dir, 'source.ipynb')
    scanpy_h5ad = os.path.join(self.temp_dir, 'source.h5a')
    os.makedirs(os.path.join(self.temp_dir, "outs"))
    with open(output_notebook_path, "w") as fp:
      fp.write("A")
    with open(scanpy_h5ad, "w") as fp:
      fp.write("A")
    with patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.prepare_and_run_scanpy_notebook",
         return_value=(output_notebook_path, scanpy_h5ad)):
      output_dict = \
        run_single_sample_scanpy_for_arc.function(
          sample_group='GRP1',
          cellranger_output_dir=self.temp_dir,
          design_dict=design_dict)
      self.assertIn("sample_group", output_dict)
      self.assertIn("cellranger_output_dir", output_dict)
      self.assertIn("notebook_report", output_dict)
      self.assertIn("scanpy_h5ad", output_dict)


  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.get_current_context")
  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.get_project_igf_id_for_analysis",
         return_value="AAA")
  @patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.fetch_analysis_name_for_analysis_id",
         return_value="BBB")
  def test_merged_scanpy_report_for_arc(self, *args):
    design_dict = {
      'analysis_design': self.yaml_file}
    output_notebook_path = os.path.join(self.temp_dir, 'source.ipynb')
    scanpy_h5ad = os.path.join(self.temp_dir, 'source.h5a')
    os.makedirs(os.path.join(self.temp_dir, "outs"))
    with open(output_notebook_path, "w") as fp:
      fp.write("A")
    with open(scanpy_h5ad, "w") as fp:
      fp.write("A")
    with patch("igf_airflow.utils.dag36_cellranger_arc_scRNA_multiome_utils.prepare_and_run_scanpy_notebook",
         return_value=(output_notebook_path, scanpy_h5ad)):
      output_dict = \
        merged_scanpy_report_for_arc.function(
          cellranger_aggr_output_dir=self.temp_dir,
          design_dict=design_dict)
      self.assertIn("sample_group", output_dict)
      self.assertIn("cellranger_output_dir", output_dict)
      self.assertIn("notebook_report", output_dict)
      self.assertIn("scanpy_h5ad", output_dict)



class TestDag36_cellranger_arc_scRNA_multiome_utilsB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.ref_file = \
      os.path.join(self.temp_dir, 'ref')
    with open(self.ref_file, 'w') as fp:
      fp.write('AAAA')
    self.template = 'template/cellranger_template/cellranger_arc_aggr_run_script_v1.sh'
    self.cellranger_output_dict = {
      'sampleA': os.path.join(self.temp_dir, 'sampleA/outs'),
      'sampleB': os.path.join(self.temp_dir, 'sampleB/outs'),
      'sampleC': os.path.join(self.temp_dir, 'sampleC/outs')}
    for _, dir_path in self.cellranger_output_dict.items():
      file_path = \
        os.path.join(
          dir_path,
          'atac_fragments.tsv.gz')
      os.makedirs(dir_path, exist_ok=True)
      with open(file_path, 'w') as fp:
        fp.write('A')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_configure_cellranger_arc_aggr(self):
    output_dict = \
      configure_cellranger_arc_aggr(
        run_script_template='template/cellranger_template/cellranger_arc_aggr_run_script_v1.sh',
        cellranger_arc_aggr_config_ref=self.ref_file,
        cellranger_arc_aggr_config_params=[],
        cellranger_output_dict=self.cellranger_output_dict)
    self.assertTrue('sample_name' in output_dict)
    self.assertEqual(output_dict['sample_name'], 'ALL')
    self.assertTrue('run_script' in output_dict)
    self.assertTrue(os.path.exists(output_dict['run_script']))
    self.assertTrue('library_csv' in output_dict)
    df = pd.read_csv(output_dict['library_csv'])
    self.assertEqual(
      df.columns.tolist(),
      ['library_id',
       'atac_fragments',
       'per_barcode_metrics',
       'gex_molecule_info'])
    self.assertEqual(len(df.index), 3)
    self.assertTrue('sampleA' in df['library_id'].values.tolist())
    self.assertTrue('sampleB' in df['library_id'].values.tolist())
    self.assertTrue('sampleC' in df['library_id'].values.tolist())
    sampleA = df[df['library_id']=='sampleA']
    self.assertEqual(
      sampleA['atac_fragments'].values[0],
      os.path.join(
        self.temp_dir,
        'sampleA/outs',
        'atac_fragments.tsv.gz'))
    self.assertEqual(
      sampleA['per_barcode_metrics'].values[0],
      os.path.join(
        self.temp_dir,
        'sampleA/outs',
        "per_barcode_metrics.csv"))
    self.assertEqual(
      sampleA['gex_molecule_info'].values[0],
      os.path.join(
        self.temp_dir,
        'sampleA/outs',
        "gex_molecule_info.h5"))
    self.assertTrue('run_dir' in output_dict)
    self.assertTrue('output_dir' in output_dict)
    self.assertEqual(output_dict['sample_name'], 'ALL')



if __name__=='__main__':
  unittest.main()