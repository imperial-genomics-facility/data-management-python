import unittest,os
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import _check_sample_id_and_analysis_id_for_project
from igf_airflow.utils.dag10_nextflow_atacseq_pipeline_utils import _fetch_sample_ids_from_nextflow_analysis_design

class Dag10_nextflow_atacseq_pipeline_utils_testA(unittest.TestCase):
  def setUp(self):
    self.analysis_description = {
      'nextflow_design':[{
        'group':'sample_id_1',
        'replicates':1,
        'sample_igf_id':'sample_id_1'
        },{
        'group':'sample_id_2',
        'replicates':1,
        'sample_igf_id':'sample_id_2'}]}

  def test_fetch_sample_ids_from_nextflow_analysis_design(self):
    sample_id_list = \
      _fetch_sample_ids_from_nextflow_analysis_design(
          analysis_description=self.analysis_description)
    self.assertEqual(len(sample_id_list),2)
    self.assertTrue('sample_id_1' in sample_id_list)

if __name__=='__main__':
  unittest.main()