import unittest,os
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _validate_analysis_description
from igf_airflow.utils.dag9_tenx_single_cell_immune_profiling_utils import _fetch_formatted_analysis_description

class Dag9_tenx_single_cell_immune_profiling_utilstestA(unittest.TestCase):
  def setUp(self):
    self.workdir = get_temp_dir()
    self.feature_types = [
      "gene_expression",
      "vdj",
      "vdj-t",
      "vdj-b",
      "antibody_capture",
      "antigen_capture",
      "crisper"]
  def tearDown(self):
    remove_dir(self.workdir)
  def test_validate_analysis_description(self):
    feature_ref = \
      os.path.join(self.workdir,'r.csv')
    with open(feature_ref,'w') as fp:
      fp.write('a,b,c,d,e')
    analysis_description = [{
        'sample_igf_id':'IGF0001',
        'feature_type':'Gene Expression'
    },{
        'sample_igf_id':'IGF002',
        'feature_type':'VDJ-T'
    },{
        'sample_igf_id':'IGF003',
        'feature_type':'Antibody Capture',
        'reference':feature_ref
    },{
        'sample_igf_id':'IGF0004',
        'feature_type':'Gene Expression'
    },{
        'sample_igf_id':'IGF0005',
        'feature_type':'vdj-t'
    },{
        'sample_igf_id':'IGF0006',
        'feature_type':'vdj_t'
    },{
        'sample_igf_id':'IGF0007',
        'feature_type':'vdj-b',
        'reference':'/path/a.csv'
    }]
    _,feature_list,msgs = \
      _validate_analysis_description(analysis_description,self.feature_types)
    errors = 0
    self.assertTrue('gene_expression' in feature_list)
    self.assertTrue('vdj-t' in feature_list)
    self.assertTrue('vdj_t' in feature_list)
    self.assertEqual(len(feature_list),5)
    for m in msgs:
      if 'IGF0001' in m:
        self.assertIn('IGF0001,IGF0004',m)
        errors += 1
      if 'IGF0006' in m:
        self.assertIn('vdj_t',m)
        errors += 1
      if '/path/a.csv' in m:
        self.assertIn('reference',m)
        errors += 1
    self.assertEqual(errors,3)
  def test_fetch_formatted_analysis_description(self):
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv'
    }]
    fastq_run_list = [{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R2_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R2 _001.fastq.gz'
    },{
      'sample_igf_id':'IGF003',
      'run_igf_id':'run3',
      'file_path':'/path/IGF003/run3/IGF003-FB_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF003',
      'run_igf_id':'run3',
      'file_path':'/path/IGF003/run3/IGF003-FB_S1_L001_R2_001.fastq.gz'
    }]
    os.environ['EPHEMERAL']='/tmp'
    formatted_analysis_description = \
      _fetch_formatted_analysis_description(
        analysis_description=analysis_description,
        fastq_run_list=fastq_run_list)
    self.assertTrue('gene_expression' in formatted_analysis_description)
    gex_entry = formatted_analysis_description.get('gene_expression')
    self.assertEqual(gex_entry.get('sample_igf_id'),'IGF001')
    self.assertEqual(gex_entry.get('sample_name'),'IGF001-GEX')
    self.assertEqual(gex_entry.get('run_count'),1)
    self.assertTrue('0' in gex_entry.get('runs'))
    self.assertEqual(gex_entry.get('runs').get('0').get('run_igf_id'),'run1')
    self.assertEqual(gex_entry.get('runs').get('0').get('fastq_dir'),'/path/IGF001/run1')

  def test_fetch_formatted_analysis_description2(self):
    analysis_description = [{
      'sample_igf_id':'IGF001',
      'feature_type':'gene expression'
    },{
      'sample_igf_id':'IGF002',
      'feature_type':'vdj'
    },{
      'sample_igf_id':'IGF003',
      'feature_type':'antibody capture',
      'reference':'a.csv'
    }]
    fastq_run_list = [{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF001',
      'run_igf_id':'run1',
      'file_path':'/path/IGF001/run1/IGF001-GEX_S1_L001_R2_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R1_001.fastq.gz'
    },{
      'sample_igf_id':'IGF002',
      'run_igf_id':'run2',
      'file_path':'/path/IGF002/run2/IGF002-VDJ_S1_L001_R2 _001.fastq.gz'
    }]
    os.environ['EPHEMERAL']='/tmp'
    with self.assertRaises(ValueError):
      _ = \
        _fetch_formatted_analysis_description(
          analysis_description=analysis_description,
          fastq_run_list=fastq_run_list)


if __name__=='__main__':
  unittest.main()