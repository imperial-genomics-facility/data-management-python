import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor

class Useradaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                   'project_name':'test_22-8-2017_rna',
                   'description':'Its project 1',
                   'project_deadline':'Before August 2017',
                   'comments':'Some samples are treated with drug X',
                 }]
    base.start_session()
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    
  def test_store_sample_and_attribute_data(self):
    sa=SampleAdaptor(**{'session_class': self.session_class})
    sample_data=[{'sample_igf_id':'IGFS001','library_id':'IGFS001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS002','library_id':'IGFS002','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS003','library_id':'IGFS003','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS004','library_id':'IGFS004','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                ]
    sa.start_session()
    sa.store_sample_and_attribute_data(data=sample_data)
    sa1=sa.check_sample_records_igf_id(sample_igf_id='IGFS001')
    sa.close_session()
    self.assertEqual(sa1,True)

  def test_check_project_and_sample(self):
    sa=SampleAdaptor(**{'session_class': self.session_class})
    sample_data=[{'sample_igf_id':'IGFS001','library_id':'IGFS001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS002','library_id':'IGFS002','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS003','library_id':'IGFS003','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS004','library_id':'IGFS004','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                ]
    sa.start_session()
    sa.store_sample_and_attribute_data(data=sample_data)
    sa1=sa.check_project_and_sample(project_igf_id='IGFP0001_test_22-8-2017_rna',\
                                    sample_igf_id='IGFS001')
    self.assertEqual(sa1,True)
    sa2=sa.check_project_and_sample(project_igf_id='IGFP0001_test_22-8-2017_rna',\
                                    sample_igf_id='IGFS0011')
    self.assertEqual(sa2,False)
    sa.close_session()
    

if __name__ == '__main__':
  unittest.main()