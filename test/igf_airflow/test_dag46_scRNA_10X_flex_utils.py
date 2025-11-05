import os
import unittest
import pytest
from unittest.mock import patch
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from yaml import load, SafeLoader
from igf_airflow.utils.dag46_scRNA_10X_flex_utils import (
    _get_cellranger_sample_group,
    prepare_cellranger_flex_script)
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

DESIGN_YAML = """sample_metadata:
  IGFsampleA:
    feature_types: Gene Expression
    cellranger_group: grp1
  IGFsampleB:
    feature_types: Antibody Capture
    cellranger_group: grp1
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

class Test_dag46_scRNA_10X_flex_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.design_yaml = DESIGN_YAML
    self.yaml_file = \
      os.path.join(
        self.temp_dir,
        'analysis_design.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write(self.design_yaml)
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

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)


  def test_get_cellranger_sample_group(self):
    design_json = load(self.design_yaml, Loader=SafeLoader)
    sample_metadata = design_json.get("sample_metadata")
    analysis_metadata = design_json.get("analysis_metadata")
    sample_groups = \
      _get_cellranger_sample_group(
        sample_metadata=sample_metadata)
    assert len(sample_groups) == 1
    sample_metadata.update({"IGFsampleA": {  
      "feature_types": "Gene Expression",
      "cellranger_group": "grp2"}
    })
    assert "grp1" in sample_groups
    with pytest.raises(Exception):
      sample_groups = \
      _get_cellranger_sample_group(
        sample_metadata=sample_metadata)
      
  def test_prepare_cellranger_flex_script(self):
    design_dict = {
      "analysis_design": self.yaml_file}
    work_dir = \
      get_temp_dir(
        self.temp_dir,
        prefix="work")
    with patch("igf_airflow.utils.dag46_scRNA_10X_flex_utils.DATABASE_CONFIG_FILE",
               self.dbconfig):
      with patch("igf_airflow.utils.dag46_scRNA_10X_flex_utils.CELLRANGER_MULTI_SCRIPT_TEMPLATE",
                 'template/cellranger_template/cellranger_multi_run_script_v1.sh'):
        output_dict = \
          prepare_cellranger_flex_script.\
            function(
              work_dir=work_dir,
              design_dict=design_dict)
        script_file = output_dict.get("run_script")
        output_dir = output_dict.get("output_dir")
        sample_group = output_dict.get("sample_group")
        assert os.path.exists(script_file)
        with open(script_file, 'r') as fp:
          data = fp.read()
        assert sample_group == "grp1"
        assert "--id=grp1" in data

if __name__=='__main__':
  unittest.main()
  


  