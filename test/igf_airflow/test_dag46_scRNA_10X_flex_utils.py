import unittest
import pytest
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from yaml import load, SafeLoader
from igf_airflow.utils.dag46_scRNA_10X_flex_utils import (
    _get_cellranger_sample_group)
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

  def tearDown(self):
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
    with pytest.raises(Exception):
      sample_groups = \
      _get_cellranger_sample_group(
        sample_metadata=sample_metadata)
      

if __name__=='__main__':
  unittest.main()
  


  