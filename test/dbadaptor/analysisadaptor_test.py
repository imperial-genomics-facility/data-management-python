import unittest,os
from igf_data.igfdb.igfTables import Base,Project,Analysis
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor

class Analysisadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    self.base = BaseAdaptor(**dbparam)
    self.engine = self.base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_store_analysis_data(self):
    project_data = [{'project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    pa.close_session()
    analysis_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'analysis_name':'analysis_1',
      'analysis_type':'type_1',
      'analysis_description':'[{"sample_igf_id":"IGFS001"}]'}]
    aa = \
      AnalysisAdaptor(**{'session_class':self.base.get_session_class()})
    aa.start_session()
    aa.store_analysis_data(data=analysis_data)
    aa.close_session()
    base = self.base
    base.start_session()
    query = \
      base.session.\
        query(Analysis.analysis_id,
              Analysis.analysis_name,
              Analysis.analysis_type,
              Analysis.analysis_description).\
        filter(Analysis.analysis_name=='analysis_1')
    result = base.fetch_records(query=query,output_mode='one_or_none')
    self.assertTrue(result is not None)
    self.assertEqual(result.analysis_type,'type_1')

  def test_fetch_project_igf_id_for_analysis_id(self):
    project_data = [{'project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    pa.close_session()
    analysis_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'analysis_name':'analysis_1',
      'analysis_type':'type_1',
      'analysis_description':'[{"sample_igf_id":"IGFS001"}]'}]
    aa = \
      AnalysisAdaptor(**{'session_class':self.base.get_session_class()})
    aa.start_session()
    aa.store_analysis_data(data=analysis_data)
    aa.close_session()
    aa.start_session()
    project_id = \
      aa.fetch_project_igf_id_for_analysis_id(analysis_id=1)
    analysis_records = \
      aa.fetch_analysis_records_analysis_id(
        analysis_id=1,
        output_mode='one_or_none')
    aa.close_session()
    self.assertEqual(project_id,'IGFP0001_test_22-8-2017_rna')
    self.assertTrue(analysis_records is not None)
    print(analysis_records)
    self.assertEqual(analysis_records.analysis_name,'analysis_1')
    self.assertEqual(analysis_records.analysis_type,'type_1')

if __name__ == '__main__':
  unittest.main()