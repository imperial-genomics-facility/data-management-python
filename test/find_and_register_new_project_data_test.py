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
    user_data=[{'name':'user1','email_id':'user1@ic.ac.uk','username':'user1'},
               {'name':'igf','email_id':'igf@imperial.ac.uk','username':'igf'}]
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
                        'data_authority':True}]
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
    
  def test_assign_username_and_password1(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          check_hpc_user=False,\
                                          )
    all_data=fa._read_project_info_and_get_new_entries(project_info_file=self.new_project_data)
    user_data=all_data['user_data']
    user_data=user_data.apply(lambda x: \
                                fa._assign_username_and_password(x), \
                                axis=1)
    user_data=user_data.to_dict(orient='region')
    self.assertEqual(user_data[0]['username'],'user2')
    self.assertTrue(user_data[0]['password'])

  def test_assign_username_and_password2(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          check_hpc_user=False,\
                                          )
    user_data1=pd.Series({'name':'user1','email_id':'user1@ic.ac.uk','hpc_username':'user11'})
    user_data1=fa._assign_username_and_password(data=user_data1)
    self.assertEqual(user_data1['username'],'user11')
    self.assertEqual(user_data1['category'],'HPC_USER')
    self.assertFalse('password' in user_data1)
    user_data2=pd.Series({'name':'user1','email_id':'user1@ic.ac.uk',\
                          'username':'user1',\
                          'hpc_username':'user11'})
    with self.assertRaises(ValueError):
      user_data2=fa._assign_username_and_password(data=user_data2)
    user_data3=pd.Series({'name':'user1','email_id':'user1@ic.ac.uk',})
    user_data3=fa._assign_username_and_password(data=user_data3)
    self.assertFalse('category' in user_data3)
    self.assertFalse('hpc_username' in user_data3)

  def test_check_existing_data(self):
    fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          check_hpc_user=False,\
                                          )
    project_data1=pd.DataFrame([{'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                                {'project_igf_id':'IGFP0002_test_23-5-2017_rna',},
                               ]
                              )
    base=BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    project_data1=project_data1.apply(lambda x: fa._check_existing_data(data=x,\
                                                                        dbsession=base.session,\
                                                                        table_name='project'),\
                                      axis=1)
    project_data1=project_data1[project_data1['EXISTS']==False].to_dict(orient='region')
    self.assertEqual(project_data1[0]['project_igf_id'],'IGFP0002_test_23-5-2017_rna')
    user_data1=pd.DataFrame([{'name':'user1','email_id':'user1@ic.ac.uk'},\
                             {'name':'user3','email_id':'user3@ic.ac.uk'},\
                            ])
    user_data1=user_data1.apply(lambda x: fa._check_existing_data(data=x,\
                                                                  dbsession=base.session,\
                                                                  table_name='user'),\
                                axis=1)
    user_data1=user_data1[user_data1['EXISTS']==False].to_dict(orient='region')
    self.assertEqual(user_data1[0]['email_id'],'user3@ic.ac.uk')
    sample_data1=pd.DataFrame([{'sample_igf_id':'IGF00001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                               {'sample_igf_id':'IGF00007','project_igf_id':'IGFP0001_test_22-8-2017_rna',},])
    
    sample_data1=sample_data1.apply(lambda x: fa._check_existing_data(data=x,\
                                                                      dbsession=base.session,\
                                                                      table_name='sample'),\
                                    axis=1)
    sample_data1=sample_data1[sample_data1['EXISTS']==False].to_dict(orient='region')
    self.assertEqual(sample_data1[0]['sample_igf_id'],'IGF00007')
    project_user_data1=pd.DataFrame([{'project_igf_id':'IGFP0001_test_22-8-2017_rna'\
                                      ,'email_id':'user1@ic.ac.uk'},\
                                     {'project_igf_id':'IGFP0002_test_23-5-2017_rna',\
                                      'email_id':'user3@ic.ac.uk'},\
                                    ]
                                   )
    project_user_data1=project_user_data1.apply(lambda x: fa._check_existing_data(\
                                                                data=x,\
                                                                dbsession=base.session,\
                                                                table_name='project_user'),\
                                                axis=1)
    project_user_data1=project_user_data1[project_user_data1['EXISTS']==False].to_dict(orient='region')
    self.assertEqual(project_user_data1[0]['project_igf_id'],'IGFP0002_test_23-5-2017_rna')
    base.close_session()
    
  def test_process_project_data_and_account(self):
      fa=Find_and_register_new_project_data(projet_info_path=os.path.join('.','data/check_project_data'),\
                                          dbconfig=self.dbconfig,\
                                          user_account_template='template/email_notification/send_new_account_info.txt',\
                                          log_slack=False,\
                                          setup_irods=False,\
                                          notify_user=False,\
                                          check_hpc_user=False,\
                                          )
      fa.process_project_data_and_account()
      dbparam = None
      with open(self.dbconfig, 'r') as json_data:
        dbparam = json.load(json_data)
      base = BaseAdaptor(**dbparam)
      base.start_session()
      pa=ProjectAdaptor(**{'session':base.session})
      project_exists=pa.check_project_records_igf_id(project_igf_id='IGFP0002_test_23-5-2017_rna')
      self.assertTrue(project_exists)
      ua=UserAdaptor(**{'session':base.session})
      user_exists=ua.check_user_records_email_id(email_id='user2@ic.ac.uk')
      self.assertTrue(user_exists)
      sa=SampleAdaptor(**{'session':base.session})
      sample_exists=sa.check_sample_records_igf_id(sample_igf_id='IGF00006')
      self.assertTrue(sample_exists)
      project_user_exists=pa.check_existing_project_user(project_igf_id='IGFP0002_test_23-5-2017_rna',\
                                                         email_id='user2@ic.ac.uk')
      self.assertTrue(project_user_exists)
      base.close_session()
    
if __name__=='__main__':
  unittest.main()
  