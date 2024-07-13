import os
import json
import yaml
import zipfile
import unittest
import pandas as pd
from unittest.mock import patch
from yaml import Loader, Dumper
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
  check_file_path,
  remove_dir)
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    parse_analysis_design_and_get_metadata)
from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import (
  prepare_cellranger_run_dir_and_script_file,
  create_library_information_for_sample_group,
  configure_cellranger_aggr,
  get_analysis_group_list,
  prepare_cellranger_script,
  run_cellranger_script,
  run_single_sample_scanpy,
  prepare_and_run_scanpy_notebook,
  move_single_sample_result_to_main_work_dir,
  collect_and_branch,
  run_cellranger_aggr_script,
  merged_scanpy_report,
  move_aggr_result_to_main_work_dir,
  load_cellranger_results_to_db)


DESIGN_YAML = """sample_metadata:
  IGFsampleA:
    feature_types: Gene Expression
    cellranger_group: grp1
  IGFsampleB:
    feature_types: VDJ-T
    cellranger_group: grp2
analysis_metadata:
  cellranger_multi_config:
    - "[gene-expression]"
    - "reference,/REF"
    - "r1-length,28"
    - "r2-length,90"
    - "chemistry,auto"
    - "expect-cells,60000"
    - "force-cells,6000"
    - "include-introns,true"
    - "no-secondary,false"
    - "no-bam,false"
    - "check-library-compatibility,true"
    - "min-assignment-confidence,0.9"
    - "cmo-set,/path/custom/cmo.csv"
    - "[vdj]"
    - "reference,/path"
    - "r1-length,28"
    - "r2-length,90"
    - "[samples]"
    - "sample_id,cmo_ids"
    - "IGF3,CMO3"
  scanpy_config:
    active: true
    mito_prefix: MT-
    run_scrublet: true
    run_cellcycle_score: true
    cell_marker_list: /path/PangaloDB
    cell_marker_species: HG38
    s_genes: ''
    g2m_genes: ''
    cell_marker_mode: NON-VDJ
  scvelo:
    active: true"""

class TestDag34_cellranger_multi_scRNA_utilA(unittest.TestCase):
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
    },{
      'sample_igf_id': 'IGFsampleB',
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
    },{
      'project_igf_id': 'IGFQprojectA',
      'sample_igf_id': 'IGFsampleB',
      'experiment_igf_id': 'IGFsampleB_MISEQ',
      'library_name': 'IGFsampleB',
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
    },{
      'experiment_igf_id': 'IGFsampleB_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'IGFsampleB_MISEQ_000000000-BRN47_1',
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
      {'file_path': '/path/IGFSampleB/IGFsampleA_S1_L001_R2_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz'},
      {'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz'}]
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
      'file_path': '/path/IGFSampleB/IGFsampleA_S1_L001_R2_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I1_001.fastq.gz'
    }, {
      'name': 'IGFsampleB_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'file_path': '/path/IGFSampleB/IGFsampleB_S2_L001_I2_001.fastq.gz'
    }]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()
    self.yaml_data = DESIGN_YAML
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

  def test_create_library_information_for_sample_group(self):
    sample_metadata, analysis_metadata = \
      parse_analysis_design_and_get_metadata(
        input_design_yaml=self.yaml_data)
    sample_library_list = \
        create_library_information_for_sample_group(
          sample_group='grp1',
          sample_metadata=sample_metadata,
          db_config_file=self.dbconfig)
    self.assertEqual(len(sample_library_list), 1)
    self.assertTrue(isinstance(sample_library_list, list))
    self.assertTrue(isinstance(sample_library_list[0], dict))
    self.assertEqual(sample_library_list[0].get('fastq_id'), 'IGFsampleA')
    self.assertEqual(sample_library_list[0].get('fastqs'), '/path/IGFSampleA')
    self.assertEqual(sample_library_list[0].get('feature_types'), 'Gene Expression')
    sample_library_list = \
        create_library_information_for_sample_group(
          sample_group='grp2',
          sample_metadata=sample_metadata,
          db_config_file=self.dbconfig)
    self.assertEqual(len(sample_library_list), 1)
    self.assertTrue(isinstance(sample_library_list, list))
    self.assertTrue(isinstance(sample_library_list[0], dict))
    self.assertEqual(sample_library_list[0].get('fastq_id'), 'IGFsampleB')
    self.assertEqual(sample_library_list[0].get('fastqs'), '/path/IGFSampleB')
    self.assertEqual(sample_library_list[0].get('feature_types'), 'VDJ-T')
    with self.assertRaises(Exception):
      sample_library_list = \
        create_library_information_for_sample_group(
          sample_group='grp5',
          sample_metadata=sample_metadata,
          db_config_file=self.dbconfig)

  def test_prepare_cellranger_run_dir_and_script_file(self):
    temp_dir = get_temp_dir()
    library_csv_file, script_file = \
      prepare_cellranger_run_dir_and_script_file(
        sample_group="grp1",
        work_dir=temp_dir,
        output_dir=os.path.join(temp_dir, "grp1"),
        design_file=self.yaml_file,
        db_config_file=self.dbconfig,
        run_script_template='template/cellranger_template/cellranger_multi_run_script_v1.sh')
    self.assertTrue(os.path.exists(library_csv_file))
    gene_expression_list = list()
    libraries_list = list()
    with open(library_csv_file, 'r') as fp:
      for i in fp:
        if i.startswith('['):
          data_list = list()
          ge_start = False
          lib_start = False
        if ge_start:
          gene_expression_list.\
            append(i.strip())
        if lib_start:
          libraries_list.\
            append(i.strip())
        if i.startswith('[gene-expression]'):
          ge_start = True
        if i.startswith('[libraries]'):
          lib_start = True
    self.assertEqual(len(gene_expression_list), 12)
    self.assertEqual(len(libraries_list), 2)
    (sample_id, fastq_dir, feature) = \
      libraries_list[1].split(',')
    self.assertEqual(sample_id, 'IGFsampleA')
    self.assertEqual(fastq_dir, '/path/IGFSampleA')
    self.assertEqual(feature, 'Gene Expression')
    self.assertTrue(os.path.exists(script_file))
    with open(script_file, 'r') as fp:
      data = fp.read()
    self.assertTrue(f'--csv={library_csv_file}' in data)
    self.assertTrue('--id=grp1' in data)
    self.assertTrue(f'--output-dir={temp_dir}' in data)

  def test_get_analysis_group_list(self):
    design_dict = {
      "analysis_design": self.yaml_file}
    unique_sample_groups = \
      get_analysis_group_list.function(
        design_dict=design_dict)
    self.assertEqual(len(unique_sample_groups), 2)
    self.assertIn("grp1", unique_sample_groups)
    with self.assertRaises(Exception):
      unique_sample_groups = \
      get_analysis_group_list.function(
        design_dict=design_dict,
        required_tag_name="feature_types",
        required_tag_value="Gene Expression")

  def test_prepare_cellranger_script(self):
    design_dict = {
      "analysis_design": self.yaml_file}
    with patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.DATABASE_CONFIG_FILE",
               self.dbconfig):
      with patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.CELLRANGER_MULTI_SCRIPT_TEMPLATE",
                 'template/cellranger_template/cellranger_multi_run_script_v1.sh'):
        output_dict = \
          prepare_cellranger_script.\
            function(
              sample_group='grp1',
              design_dict=design_dict)
        script_file = output_dict.get("run_script")
        output_dir = output_dict.get("output_dir")
        sample_group = output_dict.get("sample_group")
        self.assertTrue(os.path.exists(script_file))
        with open(script_file, 'r') as fp:
          data = fp.read()
        self.assertIn(f'--id={sample_group}', data)
        self.assertEqual(sample_group, "grp1")

  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.bash_script_wrapper",
         return_value=["A", "B"])
  def test_run_cellranger_script(
        self,
        bash_script_wrapper):
    script_dict = {
      "sample_group": "AAA",
      "run_script": "BBB",
      "output_dir": self.temp_dir}
    run_cellranger_script.\
      function(
        script_dict=script_dict)
    bash_script_wrapper.\
      assert_called_once()

  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.get_current_context")
  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.get_project_igf_id_for_analysis",
        return_value="A")
  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.fetch_analysis_name_for_analysis_id",
         return_value="A")
  def test_run_single_sample_scanpy(
        self,
        get_current_context,
        get_project_igf_id_for_analysis,
        fetch_analysis_name_for_analysis_id):
    design_dict = {
      "analysis_design": self.yaml_file}
    os.makedirs(
      os.path.join(
        self.temp_dir,
        'outs',
        'per_sample_outs',
        'grp1',
        'count'))
    output_notebook_path = \
      os.path.join(
        self.temp_dir,
        "notebook.ipynb")
    scanpy_h5ad = \
      os.path.join(
        self.temp_dir,
        "scanpy.h5")
    for f in [output_notebook_path, scanpy_h5ad]:
      with open(f, "w") as fp:
        fp.write("A")
    with patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.prepare_and_run_scanpy_notebook",
               return_value=[output_notebook_path, scanpy_h5ad]):
      design_dict = \
        run_single_sample_scanpy.\
          function(
            sample_group='grp1',
            cellranger_output_dir=self.temp_dir,
            design_dict=design_dict)
      notebook_report = design_dict.get("notebook_report")
      self.assertEqual(
        os.path.basename(notebook_report),
        os.path.basename(output_notebook_path))
      get_current_context.\
        assert_called_once()
      get_project_igf_id_for_analysis.\
        assert_called_once()
      fetch_analysis_name_for_analysis_id.\
        assert_called_once()

  def test_prepare_and_run_scanpy_notebook(self):
    output_notebook_path = \
      os.path.join(
        self.temp_dir,
        "notebook.ipynb")
    scanpy_h5ad = \
      os.path.join(
        self.temp_dir,
        'scanpy_A.h5ad')
    template_file = \
      os.path.join(
        self.temp_dir,
        "template")
    image_file = \
      os.path.join(
        self.temp_dir,
        "image")
    for f in [output_notebook_path,
              scanpy_h5ad,
              image_file,
              template_file]:
      with open(f, "w") as fp:
        fp.write("A")
    from igf_airflow.utils.dag34_cellranger_multi_scRNA_utils import Notebook_runner
    with patch.object(
      Notebook_runner,
      "execute_notebook_in_singularity", return_value=[output_notebook_path, "A"]):
      t_output_notebook_path, t_scanpy_h5ad = \
        prepare_and_run_scanpy_notebook(
          project_igf_id="A",
          analysis_name="A",
          cellranger_group_id="A",
          cellranger_counts_dir=self.temp_dir,
          scanpy_config={
            "IMAGE_FILE": image_file,
            "TEMPLATE_FILE": template_file})
      self.assertEqual(t_output_notebook_path, output_notebook_path)

  def test_move_single_sample_result_to_main_work_dir(self):
    cellranger_output_dir = \
      os.path.join(self.temp_dir, "source")
    os.makedirs(cellranger_output_dir)
    with open(os.path.join(cellranger_output_dir, "a.txt"), "w") as fp:
      fp.write("A")
    main_work_dir = \
      os.path.join(self.temp_dir, "work")
    os.makedirs(main_work_dir)
    scanpy_output_dict = {
      "cellranger_output_dir": cellranger_output_dir,
      "sample_group": "A"}
    move_single_sample_result_to_main_work_dir.\
      function(
      main_work_dir=main_work_dir,
      scanpy_output_dict=scanpy_output_dict)
    self.assertTrue(
      os.path.exists(
        os.path.join(
          main_work_dir,
          "source")))

  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.get_current_context")
  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.get_project_igf_id_for_analysis",
        return_value="A")
  @patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.fetch_analysis_name_for_analysis_id",
         return_value="A")
  def test_merged_scanpy_report(
        self,
        get_current_context,
        get_project_igf_id_for_analysis,
        fetch_analysis_name_for_analysis_id):
    design_dict = {
      "analysis_design": self.yaml_file}
    os.makedirs(
      os.path.join(
        self.temp_dir,
        'outs',
        'per_sample_outs',
        'grp1',
        'count'))
    output_notebook_path = \
      os.path.join(
        self.temp_dir,
        "notebook.ipynb")
    scanpy_h5ad = \
      os.path.join(
        self.temp_dir,
        "scanpy.h5")
    for f in [output_notebook_path, scanpy_h5ad]:
      with open(f, "w") as fp:
        fp.write("A")
    with patch("igf_airflow.utils.dag34_cellranger_multi_scRNA_utils.prepare_and_run_scanpy_notebook",
               return_value=[output_notebook_path, scanpy_h5ad]):
      design_dict = \
        merged_scanpy_report.\
          function(
            cellranger_aggr_output_dir=self.temp_dir,
            design_dict=design_dict)
      notebook_report = design_dict.get("notebook_report")
      self.assertEqual(
        os.path.basename(notebook_report),
        os.path.basename(output_notebook_path))
      get_current_context.\
        assert_called_once()
      get_project_igf_id_for_analysis.\
        assert_called_once()
      fetch_analysis_name_for_analysis_id.\
        assert_called_once()



class TestDag34_cellranger_multi_scRNA_utilB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.template = 'template/cellranger_template/cellranger_aggr_run_script_v1.sh'
    self.cellranger_output_dict = {
      'sampleA': os.path.join(self.temp_dir, 'sampleA/outs/count'),
      'sampleB': os.path.join(self.temp_dir, 'sampleB/outs/count'),
      'sampleC': os.path.join(self.temp_dir, 'sampleC/outs/count')}
    for _, dir_path in self.cellranger_output_dict.items():
      file_path = \
        os.path.join(
          dir_path,
          'sample_molecule_info.h5')
      os.makedirs(dir_path, exist_ok=True)
      with open(file_path, 'w') as fp:
        fp.write('A')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_configure_cellranger_aggr(self):
    output_dict = \
      configure_cellranger_aggr(
        run_script_template=self.template,
        cellranger_output_dict=self.cellranger_output_dict)
    self.assertTrue('sample_name' in output_dict)
    self.assertEqual(output_dict.get("sample_name"), "ALL")
    self.assertTrue('run_script' in output_dict)
    run_script = output_dict.get("run_script")
    self.assertTrue('library_csv' in output_dict)
    library_csv = output_dict.get("library_csv")
    self.assertTrue('run_dir' in output_dict)
    run_dir = output_dict.get("run_dir")
    self.assertTrue(os.path.exists(run_script))
    with open(run_script, 'r') as fp:
      script_data = fp.read()
    self.assertTrue(f'--csv={library_csv}' in script_data)
    self.assertTrue(f'--output-dir={run_dir}' in script_data)
    self.assertTrue('--id=ALL' in script_data)
    self.assertTrue(os.path.exists(library_csv))
    df = pd.read_csv(library_csv, header=0)
    self.assertTrue('sample_id' in df.columns)
    self.assertTrue('molecule_h5' in df.columns)
    self.assertEqual(len(df.index), 3)
    self.assertTrue('sampleA' in df['sample_id'].values)
    self.assertEqual(
      df[df['sample_id']=='sampleA']['molecule_h5'].values[0],
      os.path.join(
        self.cellranger_output_dict.get('sampleA'),
        'sample_molecule_info.h5'))


if __name__=='__main__':
  unittest.main()