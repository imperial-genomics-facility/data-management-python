import pandas as pd
import unittest,json,os,shutil
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.process.seqrun_processing.find_and_register_new_project_data import Find_and_register_new_project_data


class Find_and_register_project_data1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    self.new_project_data='data/check_project_data/new_project_data.csv'
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
    user_data=[{'name':'user1','email_id':'user1@ic.ac.uk','username':'user1'},]
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
    new_project_data=[{'project_igf_id':'IGFP0002_test_23-5-2017_rna',
                       'name':'user2',
                       'email_id':'user2@ic.ac.uk',
                       'sample_igf_id':'IGF00006',
                      },
                      {'project_igf_id':'IGFP0003_test_24-8-2017_rna',
                       'name':'user2',
                       'email_id':'user2@ic.ac.uk',
                       'sample_igf_id':'IGF00007',
                      }]
    pd.DataFrame(new_project_data).to_csv(os.path.join('.',self.new_project_data))
  
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    os.remove(os.path.join('.',self.new_project_data))
    
  def test_find_new_projects_data(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          )
    new_project_info_list=fa._find_new_project_info()
    self.assertEqual(len(new_project_info_list),1)
    
  def test_read_project_info_and_get_new_entries(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          )
    all_data=fa._read_project_info_and_get_new_entries(project_info_file=self.new_project_data)
    project_data=all_data['project_data'].to_dict(orient='region')
    self.assertEqual(project_data[0]['project_igf_id'],\
                     'IGFP0002_test_23-5-2017_rna')
    sample_data=all_data['sample_data'].to_dict(orient='region')
    self.assertEqual(sample_data[0]['sample_igf_id'],\
                     'IGF00006')
    user_data=all_data['user_data'].to_dict(orient='region')
    self.assertEqual(user_data[0]['email_id'],\
                     'user2@ic.ac.uk')
    project_user_data=all_data['project_user_data'].to_dict(orient='region')
    self.assertEqual(project_user_data[0]['email_id'],\
                     'user2@ic.ac.uk')
    self.assertEqual(project_user_data[0]['project_igf_id'],\
                     'IGFP0002_test_23-5-2017_rna')
    
  def test_assign_username_and_password(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          check_hpc_user=False,\
                                          )
    all_data=fa._read_project_info_and_get_new_entries(project_info_file=self.new_project_data)
    user_data=all_data['user_data']
    user_data=user_data.apply(lambda x: \
                                self._assign_username_and_password(x), \
                                axis=1)
    
    
if __name__=='__main__':
  unittest.main()
  