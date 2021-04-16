import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor

class Sampleadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'project_name':'test_22-8-2017_rna',
      'description':'Its project 1',
      'project_deadline':'Before August 2017',
      'comments':'Some samples are treated with drug X'}]
    base.start_session()
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_store_sample_and_attribute_data(self):
    sa = SampleAdaptor(**{'session_class': self.session_class})
    sample_data = [{
      'sample_igf_id':'IGFS001','library_id':'IGFS001','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS002','library_id':'IGFS002','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS003','library_id':'IGFS003','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS004','library_id':'IGFS004','project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    sa.start_session()
    sa.store_sample_and_attribute_data(data=sample_data)
    sa1 = sa.check_sample_records_igf_id(sample_igf_id='IGFS001')
    sa.close_session()
    self.assertEqual(sa1,True)

  def test_check_project_and_sample(self):
    sa = SampleAdaptor(**{'session_class': self.session_class})
    sample_data = [{
      'sample_igf_id':'IGFS001','library_id':'IGFS001','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS002','library_id':'IGFS002','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS003','library_id':'IGFS003','project_igf_id':'IGFP0001_test_22-8-2017_rna'},{
      'sample_igf_id':'IGFS004','library_id':'IGFS004','project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    sa.start_session()
    sa.store_sample_and_attribute_data(data=sample_data)
    sa1 = \
      sa.check_project_and_sample(
        project_igf_id='IGFP0001_test_22-8-2017_rna',
        sample_igf_id='IGFS001')
    self.assertEqual(sa1,True)
    sa2 = \
      sa.check_project_and_sample(
        project_igf_id='IGFP0001_test_22-8-2017_rna',
        sample_igf_id='IGFS0011')
    self.assertEqual(sa2,False)
    sa.close_session()


class Sampleadaptor_test2(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    platform_data = [{
      "platform_igf_id":"M00001" ,
      "model_name":"MISEQ" ,
      "vendor_name":"ILLUMINA" ,
      "software_name":"RTA" ,
      "software_version":"RTA1.18.54" }]
    flowcell_rule_data = [{
      "platform_igf_id":"M00001",
      "flowcell_type":"MISEQ",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE" }]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'project_name':'test_22-8-2017_rna',
      'description':'Its project 1',
      'project_deadline':'Before August 2017',
      'comments':'Some samples are treated with drug X' }]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'IGF00001',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna' }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    seqrun_data = [{
      'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
      'flowcell_id':'000000000-D0YLK',
      'platform_igf_id':'M00001',
      'flowcell':'MISEQ' }]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    experiment_data = [{
      'experiment_igf_id':'IGF00001_MISEQ',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',
      'library_name':'IGF00001',
      'sample_igf_id':'IGF00001' }]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'run_igf_id':'IGF00001_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id':'IGF00001_MISEQ',
      'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
      'lane_number':'1' }]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_seqrun_and_platform_list_for_sample_id(self):
    sa = SampleAdaptor(**{'session_class': self.session_class})
    sa.start_session()
    records = \
      sa.fetch_seqrun_and_platform_list_for_sample_id(
        sample_igf_id='IGF00001') 
    sa.close_session()
    self.assertTrue(isinstance(records,pd.DataFrame))
    records.drop_duplicates(inplace=True)
    self.assertTrue('MISEQ' in records['model_name'].values)

class Sampleadaptor_test3(unittest.TestCase):
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
      'comments':'Some samples are treated with drug X' }]
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
      'project_igf_id':'IGFP0001_test_22-8-2017_rna'}]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_sample_species_name(self):
    sa = SampleAdaptor(**{'session_class': self.session_class})
    sa.start_session()
    species_name = \
      sa.fetch_sample_species_name(sample_igf_id='IGF00001')
    self.assertEqual(species_name,'HG38')
    species_name = \
      sa.fetch_sample_species_name(sample_igf_id='IGF00002')
    self.assertEqual(species_name,'UNKNOWN')
    species_name = \
      sa.fetch_sample_species_name(sample_igf_id='IGF00003')
    self.assertTrue(species_name is None)
    sa.close_session()

class Sampleadaptor_test4(unittest.TestCase):
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
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_get_project_ids_for_list_of_samples(self):
    sa = SampleAdaptor(**{'session_class': self.session_class})
    sa.start_session()
    project_ids = \
      sa.get_project_ids_for_list_of_samples(
       ['IGF00001'])
    self.assertTrue('IGFP0001_test_22-8-2017_rna' in project_ids)
    project_ids = \
      sa.get_project_ids_for_list_of_samples(
       ['IGF00001','IGF00002'])
    self.assertTrue('IGFP0001_test_22-8-2017_rna' in project_ids)
    self.assertEqual(len(project_ids),1)
    project_ids = \
      sa.get_project_ids_for_list_of_samples(
       ['IGF00001','IGF00003'])
    self.assertTrue('IGFP0001_test_22-8-2017_rna' in project_ids)
    self.assertTrue('IGFP0002_test_22-8-2017_rna' in project_ids)
    self.assertEqual(len(project_ids),2)



if __name__ == '__main__':
  unittest.main()