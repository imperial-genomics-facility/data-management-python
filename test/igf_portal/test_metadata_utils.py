import unittest, json, os, shutil
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_portal.metadata_utils import get_db_data_and_create_json_dump
from igf_portal.metadata_utils import _create_json_for_metadata_upload
from igf_portal.metadata_utils import _get_metadata_csv_files_from_metadata_dir

class Metadata_dump_test(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
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
    platform_data = [{
      "platform_igf_id" : "M00001" ,
      "model_name" : "MISEQ" ,
      "vendor_name" : "ILLUMINA" ,
      "software_name" : "RTA" ,
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id": "M00001",
      "flowcell_type": "MISEQ",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE"}]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    project_data = [{
      'project_igf_id': 'IGFP0001_test_22-8-2017_rna',
      'project_name': 'test_22-8-2017_rna',
      'description': 'Its project 1',
      'project_deadline': 'Before August 2017',
      'comments': 'Some samples are treated with drug X'},{
      'project_igf_id': 'IGFP0001_test_22-8-2017_rna2'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'IGF00001',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna',}]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    seqrun_data = [{
      'seqrun_igf_id': '171003_M00001_0089_000000000-TEST',
      'flowcell_id': '000000000-D0YLK',
      'platform_igf_id': 'M00001',
      'flowcell':'MISEQ'}]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    experiment_data = [{
      'experiment_igf_id': 'IGF00001_MISEQ',
      'project_igf_id': 'IGFP0001_test_22-8-2017_rna',
      'library_name': 'IGF00001',
      'sample_igf_id': 'IGF00001'}]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'run_igf_id': 'IGF00001_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id': 'IGF00001_MISEQ',
      'seqrun_igf_id': '171003_M00001_0089_000000000-TEST',
      'lane_number': '1',
      'R1_READ_COUNT': 1000}]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    base.close_session()
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(self.temp_dir)

  def test_get_db_data_and_create_json_dump(self):
    temp_dir = get_temp_dir()
    json_path = os.path.join(temp_dir, 'metadata_dump.json')
    get_db_data_and_create_json_dump(
      dbconfig_json=self.dbconfig,
      output_json_path=json_path)
    self.assertTrue(os.path.exists(json_path))
    with open(json_path, 'r') as fp:
      json_data = json.load(fp)
    self.assertTrue(isinstance(json_data, dict))
    self.assertTrue('project' in json_data.keys())
    self.assertTrue('sample' in json_data.keys())
    sample_data = json_data.get('sample')
    self.assertEqual(len(sample_data), 1)
    self.assertTrue('sample_igf_id' in sample_data[0].keys())
    self.assertEqual(sample_data[0].get('sample_igf_id'), 'IGF00001')
    project_data = json_data.get('project')
    self.assertEqual(len(project_data), 2)
    project_record = [
      entry.get('project_igf_id')
        for entry in project_data
          if entry.get('project_id')==sample_data[0].get('project_id')][0]
    self.assertEqual(project_record, 'IGFP0001_test_22-8-2017_rna')


class Metadata_load_test(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    formatted_dir = os.path.join(self.temp_dir, 'metadata', 'formatted_data')
    raw_dir = os.path.join(self.temp_dir, 'metadata', 'raw_data')
    os.makedirs(formatted_dir, exist_ok=True)
    os.makedirs(raw_dir, exist_ok=True)
    self.metadata_list = [
      'formatted_data/project1_reformatted.csv',
      'raw_data/project1.csv',
      'raw_data/project1_SampleSheet.csv',
      'formatted_data/project1_SampleSheet_reformatted.csv',
      'formatted_data/project2_reformatted.csv',
      'raw_data/project2.csv',
      'raw_data/project2_SampleSheet.csv',
      'formatted_data/project2_SampleSheet_reformatted.csv'
      ]
    metadata = pd.DataFrame([{'project_igf_id':'project', 'sample_igf_id':'sample1'}])
    for entry in self.metadata_list:
      path = os.path.join(self.temp_dir, 'metadata', entry)
      metadata.to_csv(path, index=False)

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_get_metadata_csv_files_from_metadata_dir(self):
    final_metadata_dict = \
      _get_metadata_csv_files_from_metadata_dir(
        metadata_dir=self.temp_dir)
    self.assertEqual(len(final_metadata_dict.keys()), 2)
    self.assertTrue('project1' in final_metadata_dict)
    self.assertTrue(isinstance(final_metadata_dict.get('project1'), dict))
    self.assertTrue('raw_csv' in final_metadata_dict.get('project1'))
    self.assertTrue('formatted_csv' in final_metadata_dict.get('project1'))
    self.assertEqual(
      final_metadata_dict.get('project1').get('raw_csv'),
      os.path.join(self.temp_dir, 'metadata', 'raw_data/project1.csv'))
    self.assertEqual(
      final_metadata_dict.get('project2').get('formatted_csv'),
      os.path.join(self.temp_dir, 'metadata', 'formatted_data/project2_reformatted.csv'))

  def test_create_json_for_metadata_upload(self):
    final_metadata_dict = \
      _get_metadata_csv_files_from_metadata_dir(
        metadata_dir=self.temp_dir)
    project_list = list(final_metadata_dict.keys())
    temp_json = \
      _create_json_for_metadata_upload(
        project_list=project_list,
        metadata_dict=final_metadata_dict)
    self.assertTrue(os.path.exists(temp_json))
    with open(temp_json, 'r') as fp:
      json_data = json.load(fp)
    self.assertEqual(len(json_data), 2)
    self.assertTrue('metadata_tag' in json_data[0])
    self.assertTrue('raw_csv_data' in json_data[0])
    self.assertTrue('formatted_csv_data' in json_data[0])
    self.assertTrue(isinstance(json_data[0].get('formatted_csv_data'), list))
    self.assertEqual(len(json_data[0].get('formatted_csv_data')), 1)
    self.assertTrue('project_igf_id' in json_data[0].get('formatted_csv_data')[0])
    self.assertEqual(json_data[0].get('formatted_csv_data')[0].get('project_igf_id'), 'project')
    self.assertTrue('sample_igf_id' in json_data[0].get('formatted_csv_data')[0])
    self.assertEqual(json_data[0].get('formatted_csv_data')[0].get('sample_igf_id'), 'sample1')

if __name__=='__main__':
  unittest.main()