import unittest,os
from igf_data.igfdb.igfTables import Base,Project
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json

class Baseadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    self.base=BaseAdaptor(**dbparam)
    self.engine=self.base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_store_record_serial(self):
    base=self.base
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                   'project_name':'test_22-8-2017_rna',
                  },
                  {'project_igf_id':'IGFP0002_test_22-8-2017_rna',
                   'project_name':'test_23-8-2017_rna',
                  }]
    base.start_session()
    base.store_records(table=Project,
                       data=project_data,
                       mode='serial')
    base.commit_session()
    base.close_session()
    base.start_session()
    query=base.session.query(Project)
    data=base.fetch_records(query=query,
                            output_mode='dataframe')
    data=data.to_dict(orient='records')
    self.assertEqual(len(data),2)
    self.assertEqual(data[0]['project_igf_id'],
                     'IGFP0001_test_22-8-2017_rna')
    base.close_session()

  def test_store_record_bulk(self):
    base=self.base
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                   'project_name':'test_22-8-2017_rna',
                  },
                  {'project_igf_id':'IGFP0002_test_22-8-2017_rna',
                   'project_name':'test_23-8-2017_rna',
                  }]
    base.start_session()
    base.store_records(table=Project,
                       data=project_data,
                       mode='bulk')
    base.commit_session()
    base.close_session()
    base.start_session()
    query=base.session.query(Project)
    data=base.fetch_records(query=query,
                            output_mode='dataframe')
    data=data.to_dict(orient='records')
    self.assertEqual(len(data),2)
    self.assertEqual(data[0]['project_igf_id'],
                     'IGFP0001_test_22-8-2017_rna')
    base.close_session()

if __name__ == '__main__':
  unittest.main()