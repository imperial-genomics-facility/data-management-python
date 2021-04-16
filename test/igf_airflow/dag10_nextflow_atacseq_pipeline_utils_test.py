import unittest,os
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.utils.dbutils import read_dbconf_json
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


class Dag10_nextflow_atacseq_pipeline_utils_testB(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'project_name':'test_22-8-2017_rna',
      'description':'Its project 1',
      'project_deadline':'Before August 2017',
      'comments':'Some samples are treated with drug X'
      },{
      'project_igf_id':'IGFP0002_test_22-8-2017_rna',
      'project_name':'test_22-8-2017_rna2'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'IGF00001',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'species_name':'HG38'},{
      'sample_igf_id':'IGF00002',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'species_name':'UNKNOWN'},{
      'sample_igf_id':'IGF00003',
      'project_igf_id':'IGFP0002_test_22-8-2017_rna'}]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    analysis_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'analysis_name':'analysis_1',
      'analysis_type':'type_1',
      'analysis_description':'[{"sample_igf_id":"IGF00001"}]'
      },{
      'project_igf_id':'IGFP0002_test_22-8-2017_rna',
      'analysis_name':'analysis_2',
      'analysis_type':'type_2',
      'analysis_description':'[{"sample_igf_id":"IGF00003"}]'}]
    aa = \
      AnalysisAdaptor(**{'session':base.session})
    aa.store_analysis_data(data=analysis_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_check_sample_id_and_analysis_id_for_project(self):
    self.assertTrue(
      _check_sample_id_and_analysis_id_for_project(
        analysis_id=1,
        sample_igf_id_list=['IGF00001','IGF00002'],
        dbconfig_file=self.dbconfig) is None)
    with self.assertRaises(Exception) as c:
      _check_sample_id_and_analysis_id_for_project(
        analysis_id=1,
        sample_igf_id_list=['IGF00001','IGF00003'],
        dbconfig_file=self.dbconfig)
    with self.assertRaises(Exception) as c:
      _check_sample_id_and_analysis_id_for_project(
        analysis_id=2,
        sample_igf_id_list=['IGF00001','IGF00002'],
        dbconfig_file=self.dbconfig)

if __name__=='__main__':
  unittest.main()