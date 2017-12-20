import pandas as pd
import unittest,json,os,shutil
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.process.seqrun_processing import find_and_register_new_project_data


class Find_and_register_project_data1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.session_class
    base.start_session()
    ua=UserAdaptor(**{'session':base.session})
    user_data=[{'name':'user1','email_id':'user1@ic.ac.uk',}]
    ua.store_user_data(data=user_data)
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                   'project_name':'test_22-8-2017_rna',
                   'description':'Its project 1',
                   'project_deadline':'Before August 2017',
                   'comments':'Some samples are treated with drug X',
                 }]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_user_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                        'email_id':'user1@ic.ac.uk',
                        'data_authority':'T'}]
    pa.assign_user_to_project(data=project_user_data)
    sample_data=[{'sample_igf_id':'IGF00001',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00002',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00003',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00004',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00005', 
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                ]
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()
  
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    
if __name__=='__main__':
  unittest.main()
  