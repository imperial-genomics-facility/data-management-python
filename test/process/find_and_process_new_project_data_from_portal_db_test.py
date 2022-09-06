import pandas as pd
import unittest,json,os,shutil
from igf_data.igfdb.igfTables import Base, Project, User, Sample, ProjectUser, Project_attribute
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.process.seqrun_processing.find_and_process_new_project_data_from_portal_db import Find_and_register_new_project_data_from_portal_db

class Find_and_register_new_project_data_from_portal_db_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    self.work_dir = get_temp_dir()
    self.new_project_data_csv = os.path.join(self.work_dir, 'new_project_data.csv')
    self.portal_conf_file = os.path.join(self.work_dir, 'portal_conf.json')
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    base.start_session()
    ua = UserAdaptor(**{'session':base.session})
    user_data = [
      {'name':'user1','email_id':'user1@ic.ac.uk','username':'user1'},
      {'name':'igf','email_id':'admin@email.com','username':'igf'}]
    ua.store_user_data(data=user_data)
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'project_name':'test_22-8-2017_rna',
      'description':'Its project 1',
      'project_deadline':'Before August 2017',
      'comments':'Some samples are treated with drug X',}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_user_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'email_id':'user1@ic.ac.uk',
      'data_authority':True}]
    pa.assign_user_to_project(data=project_user_data)
    sample_data = [
      {'sample_igf_id':'IGF00001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00002','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00003','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00004','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id':'IGF00005','project_igf_id':'IGFP0001_test_22-8-2017_rna',}]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()
    self.project_data = [
      {'project_igf_id':'IGFP0002_test_23-5-2017_rna',
       'name':'user2',
       'email_id':'user2@ic.ac.uk',
       'sample_igf_id':'IGF00006'},
      {'project_igf_id':'IGFP0003_test_24-8-2017_rna',
       'name':'user2',
       'email_id':'user2@ic.ac.uk',
       'sample_igf_id':'IGF00007',
       'barcode_check':'OFF'}]
    pd.DataFrame(self.project_data).\
      to_csv(self.new_project_data_csv)

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.work_dir)

  def test_read_project_info_and_get_new_entries(self):
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=self.portal_conf_file,
        dbconfig=self.dbconfig,
        user_account_template='template/email_notification/send_new_account_info.txt',
        default_user_email='admin@email.com',
        log_slack=False)
    formatted_data = \
      fa._read_project_info_and_get_new_entries(self.project_data)
    project_data = formatted_data.get('project_data')
    user_data = formatted_data.get('user_data')
    project_user_data = formatted_data.get('project_user_data')
    sample_data = formatted_data.get('sample_data')
    self.assertTrue(project_data is not None)
    self.assertTrue(user_data is not None)
    self.assertTrue(project_user_data is not None)
    self.assertTrue(sample_data is not None)
    self.assertTrue(isinstance(project_data, pd.DataFrame))
    self.assertTrue(isinstance(user_data, pd.DataFrame))
    self.assertTrue(isinstance(project_user_data, pd.DataFrame))
    self.assertTrue(isinstance(sample_data, pd.DataFrame))
    self.assertEqual(len(project_data.index), 2)
    self.assertTrue('project_igf_id' in project_data.columns)
    self.assertTrue('barcode_check' in project_data.columns)
    self.assertTrue('IGFP0002_test_23-5-2017_rna' in project_data['project_igf_id'].values.tolist())
    self.assertTrue('IGFP0003_test_24-8-2017_rna' in project_data['project_igf_id'].values.tolist())
    self.assertEqual(project_data[project_data['project_igf_id']=='IGFP0002_test_23-5-2017_rna']['barcode_check'].values[0], 'ON')
    self.assertEqual(project_data[project_data['project_igf_id']=='IGFP0003_test_24-8-2017_rna']['barcode_check'].values[0], 'OFF')
    self.assertEqual(len(sample_data.index), 2)
    self.assertTrue('project_igf_id' in sample_data.columns)
    self.assertTrue('sample_igf_id' in sample_data.columns)
    self.assertEqual(sample_data[sample_data['sample_igf_id']=='IGF00006']['project_igf_id'].values.tolist()[0], 'IGFP0002_test_23-5-2017_rna')
    self.assertEqual(len(project_user_data.index), 2)
    self.assertTrue('project_igf_id' in project_user_data.columns)
    self.assertTrue('email_id' in project_user_data.columns)
    self.assertEqual(project_user_data[project_user_data['project_igf_id']=='IGFP0002_test_23-5-2017_rna']['email_id'].values[0], 'user2@ic.ac.uk')
    self.assertEqual(len(user_data.index), 1)
    self.assertTrue('name' in user_data.columns)
    self.assertTrue('email_id' in user_data.columns)
    self.assertEqual(user_data['email_id'].values[0], 'user2@ic.ac.uk')

  def test_check_and_register_data(self):
    project_data = [
      {'project_igf_id':'IGFP0002_test_23-5-2017_rna',
       'name':'user2',
       'email_id':'user2@ic.ac.uk',
       'sample_igf_id':'IGF00006'},
      {'project_igf_id':'IGFP0003_test_24-8-2017_rna',
       'name':'user2',
       'email_id':'user2@ic.ac.uk',
       'sample_igf_id':'IGF00007',
       'barcode_check':'OFF'},
      {'sample_igf_id':'IGF00001',
       'project_igf_id':'IGFP0001_test_22-8-2017_rna',
       'name':'user1',
       'email_id':'user1@ic.ac.uk'}]
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=self.portal_conf_file,
        dbconfig=self.dbconfig,
        user_account_template='template/email_notification/send_new_account_info.txt',
        log_slack=False,
        default_user_email='admin@email.com',
        setup_irods=False,
        notify_user=False)
    formatted_data = \
      fa._read_project_info_and_get_new_entries(self.project_data)
    fa._check_and_register_data(data=formatted_data)
    base = \
      BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    results = \
      base.session.\
        query(Project.project_igf_id, Sample.sample_igf_id).\
        join(Sample, Project.project_id==Sample.project_id).\
        filter(Project.project_igf_id=='IGFP0002_test_23-5-2017_rna').all()
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0][0], 'IGFP0002_test_23-5-2017_rna')
    self.assertEqual(results[0][1], 'IGF00006')
    result = \
      base.session.\
        query(Project.project_igf_id, User.email_id).\
        join(ProjectUser, Project.project_id==ProjectUser.project_id).\
        join(User, User.user_id==ProjectUser.user_id).\
        filter(Project.project_igf_id=='IGFP0002_test_23-5-2017_rna').\
        filter(ProjectUser.data_authority=='T').\
        one_or_none()
    self.assertTrue(result is not None)
    self.assertEqual(result.project_igf_id, 'IGFP0002_test_23-5-2017_rna')
    self.assertEqual(result.email_id, 'user2@ic.ac.uk')
    results = \
      base.session.\
        query(
          Project.project_igf_id,
          User.email_id,
          ProjectUser.data_authority).\
        join(ProjectUser, Project.project_id==ProjectUser.project_id).\
        join(User, User.user_id==ProjectUser.user_id).\
        filter(Project.project_igf_id=='IGFP0002_test_23-5-2017_rna').\
        all()
    self.assertEqual(len(results), 2)
    results = \
      [e for e in results if e[1]=='admin@email.com']
    self.assertEqual(len(results), 1)
    self.assertIsNone(results[0][2])


  def test_check_existing_data(self):
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=self.portal_conf_file,
        dbconfig=self.dbconfig,
        user_account_template='template/email_notification/send_new_account_info.txt',
        log_slack=False,
        default_user_email='admin@email.com',
        setup_irods=False,
        notify_user=False)
    project_data1 = \
      pd.DataFrame([
        {'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
        {'project_igf_id':'IGFP0002_test_23-5-2017_rna',}])
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    project_data1['EXISTS'] = ''
    project_data1 = \
      project_data1.apply(
        lambda x: \
          fa._check_existing_data(
            data=x,
            dbsession=base.session,
            table_name='project'),
        axis=1,
        result_type=None)
    project_data1 = \
      project_data1[project_data1['EXISTS']==False].\
        to_dict(orient='records')
    self.assertEqual(
      project_data1[0]['project_igf_id'],
      'IGFP0002_test_23-5-2017_rna')
    user_data1 = \
      pd.DataFrame([
        {'name':'user1','email_id':'user1@ic.ac.uk'},
        {'name':'user3','email_id':'user3@ic.ac.uk'}])
    user_data1['EXISTS'] = ''
    user_data1 = \
      user_data1.apply(
        lambda x: \
          fa._check_existing_data(
            data=x,
            dbsession=base.session,
            table_name='user'),
        axis=1,
        result_type=None)
    user_data1 = \
      user_data1[user_data1['EXISTS']==False].\
        to_dict(orient='records')
    self.assertEqual(user_data1[0]['email_id'],'user3@ic.ac.uk')
    sample_data1 = \
      pd.DataFrame([
        {'sample_igf_id':'IGF00001','project_igf_id':'IGFP0001_test_22-8-2017_rna',},
        {'sample_igf_id':'IGF00007','project_igf_id':'IGFP0001_test_22-8-2017_rna',}])
    sample_data1['EXISTS'] = ''
    sample_data1 = \
      sample_data1.apply(
        lambda x: \
          fa._check_existing_data(
            data=x,
            dbsession=base.session,
            table_name='sample'),
        axis=1,
        result_type=None)
    sample_data1 = \
      sample_data1[sample_data1['EXISTS']==False].\
        to_dict(orient='records')
    self.assertEqual(
      sample_data1[0]['sample_igf_id'],
      'IGF00007')
    project_user_data1 = \
      pd.DataFrame([
        {'project_igf_id':'IGFP0001_test_22-8-2017_rna','email_id':'user1@ic.ac.uk'},
        {'project_igf_id':'IGFP0002_test_23-5-2017_rna','email_id':'user3@ic.ac.uk'}])
    project_user_data1['EXISTS'] = ''
    project_user_data1['data_authority'] = ''
    #print('a',project_user_data1.to_dict(orient='records'))
    project_user_data1 = \
      project_user_data1.apply(
        lambda x: \
          fa._check_existing_data(
          data=x,
          dbsession=base.session,
          table_name='project_user'),
        axis=1,
        result_type=None)
    #print('b',project_user_data1.to_dict(orient='records'))
    project_user_data1 = \
      project_user_data1[project_user_data1['EXISTS']==False].\
        to_dict(orient='records')
    self.assertEqual(
      project_user_data1[0]['project_igf_id'],
      'IGFP0002_test_23-5-2017_rna')
    base.close_session()

  def test_assign_username_and_password1(self):
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=self.portal_conf_file,
        dbconfig=self.dbconfig,
        user_account_template='template/email_notification/send_new_account_info.txt',
        log_slack=False,
        check_hpc_user=False)
    all_data = \
      fa._read_project_info_and_get_new_entries(
        project_info_data=self.project_data)
    user_data = all_data['user_data']
    user_data = \
      user_data.apply(
        lambda x: \
          fa._assign_username_and_password(x),
        axis=1)
    user_data = user_data.to_dict(orient='records')
    self.assertEqual(user_data[0]['username'],'user2')
    self.assertTrue(user_data[0]['password'])

  def test_assign_username_and_password2(self):
    fa = \
      Find_and_register_new_project_data_from_portal_db(
        portal_db_conf_file=self.portal_conf_file,
        dbconfig=self.dbconfig,
        user_account_template='template/email_notification/send_new_account_info.txt',
        log_slack=False,
        check_hpc_user=False)
    user_data1 = \
      pd.Series({
        'name':'user1',
        'email_id':'user1@ic.ac.uk',
        'hpc_username':'user11'})
    user_data1 = \
      fa._assign_username_and_password(data=user_data1)
    self.assertEqual(user_data1['username'],'user11')
    self.assertEqual(user_data1['category'],'HPC_USER')
    self.assertFalse('password' in user_data1)
    user_data2 = \
      pd.Series({
        'name':'user1',
        'email_id':'user1@ic.ac.uk',
        'username':'user1',
        'hpc_username':'user11'})
    with self.assertRaises(ValueError):
      user_data2 = \
        fa._assign_username_and_password(data=user_data2)
    user_data3 = \
      pd.Series({
        'name':'user1',
        'email_id':'user1@ic.ac.uk',})
    user_data3 = \
      fa._assign_username_and_password(data=user_data3)
    self.assertFalse('category' in user_data3)
    self.assertFalse('hpc_username' in user_data3)

  

  def test_get_user_password(self):
      pass_str = \
        Find_and_register_new_project_data_from_portal_db(
          portal_db_conf_file=self.portal_conf_file,
          dbconfig=self.dbconfig,
          user_account_template='template/email_notification/send_new_account_info.txt',
          log_slack=False,
          check_hpc_user=False).\
          _get_user_password()
      self.assertEqual(len(pass_str), 12)

if __name__=='__main__':
  unittest.main()