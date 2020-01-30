import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base,Project,User,ProjectUser,Sample,Experiment,Run,Collection,Collection_group,File
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.projectutils import get_files_and_irods_path_for_project,mark_project_as_withdrawn
from igf_data.utils.projectutils import get_project_read_count,mark_project_barcode_check_off,get_seqrun_info_for_project

class Projectutils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    data = [
      {'project_igf_id': 'IGFP001_test1_24-1-18',},
      {'project_igf_id': 'IGFP002_test1_24-1-18',
       'barcode_check':'ON'},
      {'project_igf_id': 'IGFP003_test1_24-1-18',
       'barcode_check':'OFF'}]
    self.data = pd.DataFrame(data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_mark_project_barcode_check_off(self):
    pr = ProjectAdaptor(**{'session_class':self.session_class})
    pr.start_session()
    pr.store_project_and_attribute_data(self.data)
    pr.close_session()

    mark_project_barcode_check_off(
      project_igf_id='IGFP001_test1_24-1-18',
      session_class=self.session_class)                                         # no attribute record
    pr.start_session()
    attribute_check = \
      pr.check_project_attributes(
        project_igf_id='IGFP001_test1_24-1-18',
        attribute_name='barcode_check')
    self.assertTrue(attribute_check)
    pr_attributes = \
      pr.get_project_attributes(
        project_igf_id='IGFP001_test1_24-1-18',
        attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'OFF')
    
    pr_attributes = \
      pr.get_project_attributes(
        project_igf_id='IGFP002_test1_24-1-18',
        attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'ON')
    pr.close_session()

    mark_project_barcode_check_off(
      project_igf_id='IGFP002_test1_24-1-18',
      session_class=self.session_class)                                         # barcode check ON
    pr.start_session()
    pr_attributes = \
      pr.get_project_attributes(
        project_igf_id='IGFP002_test1_24-1-18',
        attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'OFF')

    pr_attributes = \
      pr.get_project_attributes(
        project_igf_id='IGFP003_test1_24-1-18',
        attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'OFF')
    pr.close_session()

    mark_project_barcode_check_off(
      project_igf_id='IGFP003_test1_24-1-18',
      session_class=self.session_class)                                         # barcode check OFF
    pr.start_session()
    pr_attributes = \
      pr.get_project_attributes(
        project_igf_id='IGFP003_test1_24-1-18',
        attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'OFF')
    pr.close_session()

class Projectutils_test2(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/travis_dbconf.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    platform_data = [{
      "platform_igf_id" : "M001",
      "model_name" : "MISEQ" ,
      "vendor_name" : "ILLUMINA" ,
      "software_name" : "RTA",
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id":"M001",
      "flowcell_type":"MISEQ",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE"}]
    seqrun_data = [
      {'seqrun_igf_id':'SeqrunA', 
       'flowcell_id':'000000000-D0YLK', 
       'platform_igf_id':'M001',
       'flowcell':'MISEQ'},
      {'seqrun_igf_id':'SeqrunB', 
       'flowcell_id':'000000000-D0YLL', 
       'platform_igf_id':'M001',
       'flowcell':'MISEQ'}]
    project_data = [
      {'project_igf_id':'ProjectA'},
      {'project_igf_id':'ProjectB'}]
    user_data = [
      {'name':'UserA', 
       'email_id':'usera@ic.ac.uk', 
       'username':'usera'},
      {'name':'UserB', 
       'email_id':'userb@ic.ac.uk', 
       'username':'userb'}]
    project_user_data = [
      {'project_igf_id': 'ProjectA',
       'email_id': 'usera@ic.ac.uk',
       'data_authority':True},
      {'project_igf_id': 'ProjectA',
       'email_id': 'userb@ic.ac.uk'},
      {'project_igf_id': 'ProjectB',
       'email_id': 'userb@ic.ac.uk',
       'data_authority':True},
      {'project_igf_id': 'ProjectB',
       'email_id': 'usera@ic.ac.uk'}]
    sample_data = [
      {'sample_igf_id':'SampleA',
       'project_igf_id':'ProjectA'},
      {'sample_igf_id':'SampleB',
       'project_igf_id':'ProjectB'}]
    experiment_data = [
      {'experiment_igf_id':'ExperimentA',
       'sample_igf_id':'SampleA',
       'library_name':'SampleA',
       'platform_name':'MISEQ',
       'project_igf_id':'ProjectA'},
      {'experiment_igf_id':'ExperimentB',
       'sample_igf_id':'SampleB',
       'library_name':'SampleB',
       'platform_name':'MISEQ',
       'project_igf_id':'ProjectB'}]
    run_data = [
      {'run_igf_id':'RunA_A',
       'experiment_igf_id':'ExperimentA',
       'seqrun_igf_id':'SeqrunA',
       'lane_number':'1'},
      {'run_igf_id':'RunA_B',
       'experiment_igf_id':'ExperimentA',
       'seqrun_igf_id':'SeqrunB',
       'lane_number':'1'}]
    file_data = [
      {'file_path':'/path/RunA_A_R1.fastq.gz',
       'location':'HPC_PROJECT',
       'md5':'fd5a95c18ebb7145645e95ce08d729e4',
       'size':'1528121404'},
      {'file_path':'/path/ExperimentA.cram',
       'location':'HPC_PROJECT',
       'md5':'fd5a95c18ebb7145645e95ce08d729e4',
       'size':'1528121404'},
      {'file_path':'/path/ExperimentB.cram',
       'location':'HPC_PROJECT',
       'md5':'fd5a95c18ebb7145645e95ce08d729e3',
       'size':'1528121404'}]
    collection_data = [
      {'name':'RunA_A',
       'type':'demultiplexed_fastq',
       'table':'run'},
      {'name':'ExperimentA',
       'type':'analysis_cram',
       'table':'run'},
      {'name':'ExperimentB',
       'type':'analysis_cram',
       'table':'run'}]
    collection_files_data = [
      {'name':'RunA_A',
       'type':'demultiplexed_fastq',
       'file_path':'/path/RunA_A_R1.fastq.gz'},
      {'name':'ExperimentA',
       'type':'analysis_cram',
       'file_path':'/path/ExperimentA.cram'},
      {'name':'ExperimentB',
       'type':'analysis_cram',
       'file_path':'/path/ExperimentB.cram'}]
    base.start_session()
    ## store platform data
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    ## store flowcell rules data
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    ## store seqrun data
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    ## store user data
    ua=UserAdaptor(**{'session':base.session})
    ua.store_user_data(data=user_data)
    ## store project data
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    ## assign users to projects
    pa.assign_user_to_project(data=project_user_data)
    ## store sample data
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    ## store experiment data
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    ## store run data
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    ## store files to db
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    ## store collection info to db
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ## assign files to collections
    ca.create_collection_group(data=collection_files_data)
    base.close_session()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)


  def test_mark_project_as_withdrawn(self):
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    query = \
      base.session.\
        query(
          Sample.status.label('sample_status'),
          Experiment.status.label('exp_status'),
          Run.status.label('run_status'),
          File.status.label('file_status')).\
        join(Project,Project.project_id==Sample.project_id).\
        join(Experiment,Experiment.sample_id==Sample.sample_id).\
        join(Run,Run.experiment_id==Experiment.experiment_id).\
        join(Collection,Collection.name==Run.run_igf_id).\
        join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
        join(File,File.file_id==Collection_group.file_id).\
        filter(Collection.type=='demultiplexed_fastq').\
        filter(Collection.table=='run').\
        filter(Project.project_igf_id=='ProjectA')
    records = base.fetch_records(query=query)
    self.assertEqual(records['sample_status'].values[0],'ACTIVE')
    self.assertEqual(records['exp_status'].values[0],'ACTIVE')
    self.assertEqual(records['run_status'].values[0],'ACTIVE')
    self.assertEqual(records['file_status'].values[0],'ACTIVE')
    query = \
      base.session.\
        query(
          Sample.status.label('sample_status'),
          Experiment.status.label('exp_status'),
          File.status.label('file_status')).\
        join(Project,Project.project_id==Sample.project_id).\
        join(Experiment,Experiment.sample_id==Sample.sample_id).\
        join(Collection,Collection.name==Experiment.experiment_igf_id).\
        join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
        join(File,File.file_id==Collection_group.file_id).\
        filter(Collection.type=='analysis_cram').\
        filter(Collection.table=='run').\
        filter(Project.project_igf_id=='ProjectB')
    records = base.fetch_records(query=query)
    self.assertEqual(records['sample_status'].values[0],'ACTIVE')
    self.assertEqual(records['exp_status'].values[0],'ACTIVE')
    self.assertEqual(records['file_status'].values[0],'ACTIVE')
    base.close_session()
    mark_project_as_withdrawn(
      project_igf_id='ProjectA',
      db_session_class=self.session_class,
      withdrawn_tag='WITHDRAWN')
    base.start_session()
    query = \
      base.session.\
        query(
          Sample.status.label('sample_status'),
          Experiment.status.label('exp_status'),
          Run.status.label('run_status'),
          File.status.label('file_status')).\
        join(Project,Project.project_id==Sample.project_id).\
        join(Experiment,Experiment.sample_id==Sample.sample_id).\
        join(Run,Run.experiment_id==Experiment.experiment_id).\
        join(Collection,Collection.name==Run.run_igf_id).\
        join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
        join(File,File.file_id==Collection_group.file_id).\
        filter(Collection.type=='demultiplexed_fastq').\
        filter(Collection.table=='run').\
        filter(Project.project_igf_id=='ProjectA')
    records = base.fetch_records(query=query)
    self.assertEqual(records['sample_status'].values[0],'WITHDRAWN')
    self.assertEqual(records['exp_status'].values[0],'WITHDRAWN')
    self.assertEqual(records['run_status'].values[0],'WITHDRAWN')
    self.assertEqual(records['file_status'].values[0],'WITHDRAWN')
    query = \
      base.session.\
        query(
          Sample.status.label('sample_status'),
          Experiment.status.label('exp_status'),
          File.status.label('file_status')).\
        join(Project,Project.project_id==Sample.project_id).\
        join(Experiment,Experiment.sample_id==Sample.sample_id).\
        join(Collection,Collection.name==Experiment.experiment_igf_id).\
        join(Collection_group,Collection.collection_id==Collection_group.collection_id).\
        join(File,File.file_id==Collection_group.file_id).\
        filter(Collection.type=='analysis_cram').\
        filter(Collection.table=='run').\
        filter(Project.project_igf_id=='ProjectB')
    records = base.fetch_records(query=query)
    self.assertEqual(records['sample_status'].values[0],'ACTIVE')
    self.assertEqual(records['exp_status'].values[0],'ACTIVE')
    self.assertEqual(records['file_status'].values[0],'ACTIVE')
    base.close_session()

  def test_get_files_and_irods_path_for_project(self):
    base = BaseAdaptor(**{'session_class':self.session_class})
    file_list, irods_dir = \
      get_files_and_irods_path_for_project(
        project_igf_id='ProjectA',
        db_session_class=self.session_class,
        irods_path_prefix='/igfZone/home/')
    base.start_session()
    query = \
      base.session.\
        query(File.file_path).\
        join(Collection_group,File.file_id==Collection_group.file_id).\
        join(Collection,Collection.collection_id==Collection_group.collection_id).\
        join(Experiment,Experiment.experiment_igf_id==Collection.name).\
        join(Sample,Sample.sample_id==Experiment.sample_id).\
        join(Project,Project.project_id==Sample.project_id).\
        filter(Project.project_igf_id=='ProjectA')
    exp_file_list = base.fetch_records(query=query)
    exp_file_list = exp_file_list['file_path'].values
    self.assertTrue(exp_file_list[0] in file_list)
    query = \
      base.session.\
        query(File.file_path).\
        join(Collection_group,File.file_id==Collection_group.file_id).\
        join(Collection,Collection.collection_id==Collection_group.collection_id).\
        join(Experiment,Experiment.experiment_igf_id==Collection.name).\
        join(Sample,Sample.sample_id==Experiment.sample_id).\
        join(Project,Project.project_id==Sample.project_id).\
        filter(Project.project_igf_id=='ProjectB')
    exp_file_list = base.fetch_records(query=query)
    exp_file_list = exp_file_list['file_path'].values
    self.assertTrue(exp_file_list[0] not in file_list)


if __name__ == '__main__':
  unittest.main()