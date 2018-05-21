import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Project, Project_attribute, Sample
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.utils.dbutils import read_dbconf_json

class Projectadaptor_test1(unittest.TestCase):
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
                 },
                 {'project_igf_id':'IGFP0002_test_22-8-2017_rna',
                   'project_name':'test_23-8-2017_rna',
                   'description':'Its project 2',
                   'project_deadline':'Before August 2017',
                   'comments':'Some samples are treated with drug X',
                 }]
    base.start_session()
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sa=SampleAdaptor(**{'session': base.session})
    sample_data=[{'sample_igf_id':'IGFS001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS002','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS003','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGFS004','project_igf_id':'IGFP0001_test_22-8-2017_rna','status':'FAILED',},
                ]
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_fetch_project_samples(self):
    pa=ProjectAdaptor(**{'session_class':self.session_class})
    pa.start_session()
    sample1=pa.fetch_project_samples(project_igf_id='IGFP0001_test_22-8-2017_rna',output_mode='dataframe')
    self.assertEqual(len(sample1.index),3)
    sample2=pa.fetch_project_samples(project_igf_id='IGFP0002_test_22-8-2017_rna',output_mode='dataframe')
    self.assertEqual(len(sample2.index),0)
    sample3=pa.fetch_project_samples(project_igf_id='IGFP0001_test_22-8-2017_rna',
                                     only_active=False,
                                     output_mode='dataframe')
    self.assertEqual(len(sample3.index),4)
    pa.close_session()

  def test_count_project_samples(self):
    pa=ProjectAdaptor(**{'session_class':self.session_class})
    pa.start_session()
    sample1=pa.count_project_samples(project_igf_id='IGFP0001_test_22-8-2017_rna')
    self.assertEqual(sample1,3)
    sample2=pa.count_project_samples(project_igf_id='IGFP0002_test_22-8-2017_rna')
    self.assertEqual(sample2,0)
    sample3=pa.count_project_samples(project_igf_id='IGFP0001_test_22-8-2017_rna',
                                     only_active=False)
    self.assertEqual(sample3,4)
    pa.close_session()

if __name__ == '__main__':
  unittest.main()