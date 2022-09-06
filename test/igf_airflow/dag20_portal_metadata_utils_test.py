import unittest, os
import pandas as pd
from igf_data.utils.fileutils import copy_local_file, get_temp_dir, remove_dir
from igf_airflow.utils.dag20_portal_metadata_utils import _get_all_known_projects
from igf_airflow.utils.dag20_portal_metadata_utils import _reformat_metadata_files
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.dbutils import read_dbconf_json


class Dag20_portal_metadata_utils_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    pa = ProjectAdaptor(**dbparam)
    self.engine = pa.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = pa.get_session_class()
    pa.start_session()
    project_data = [{
        'project_igf_id': 'project1' },{
        'project_igf_id': 'project2' },{
        'project_igf_id': 'project3' }]
    pa.store_project_and_attribute_data(project_data)
    pa.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_get_all_known_projects(self):
    project_list_csv = \
      _get_all_known_projects(self.dbconfig)
    project_list = \
      pd.read_csv(project_list_csv)
    self.assertTrue('project_igf_id' in project_list.columns)
    self.assertEqual(len(project_list.index), 3)
    self.assertTrue('project2' in project_list['project_igf_id'].values.tolist())


class Dag20_portal_metadata_utils_testB(unittest.TestCase):
  def setUp(self):
      self.temp_dir = get_temp_dir()
      self.metadata_file = os.path.join(self.temp_dir, 'project1.csv')
      metadata = [{
          'project_igf_id': 'IGFQ1 scRNA-seq5primeFB',
          'name': 'XXX YYY',
          'email_id': 'x.y@ic.ac.uk',
          'sample_igf_id': 'IGF1 ',
          'species_text': 'human',
          'fragment_length_distribution_mean': 500,
          'fragment_length_distribution_sd': 50,
          'library_preparation': 'Not Applicable',
          'sample_type': '',
          'sample_description': 'Pre made library',
          'library_type': 'Hybrid Capture - Custom',
          'expected_lanes': 1,
          'sequencing_type': 'HiSeq 4000 75 PE'},{
          'project_igf_id': 'IGFQ1 scRNA-seq5primeFB',
          'name': 'XXX YYY',
          'email_id': 'x.y@ic.ac.uk',
          'sample_igf_id': 'IGF2 ',
          'species_text': 'HUMAN',
          'fragment_length_distribution_mean': 450,
          'fragment_length_distribution_sd': 50,
          'library_preparation': 'Not Applicable',
          'sample_type': '',
          'sample_description': 'Pre made library',
          'library_type': "SINGLE CELL-3' RNA (NUCLEI)",
          'expected_lanes': 1,
          'sequencing_type': 'HiSeq 4000 75 PE'},{
          'project_igf_id': 'IGFQ1 scRNA-seq5primeFB',
          'name': 'XXX YYY',
          'email_id': 'x.y@ic.ac.uk',
          'sample_igf_id': 'IGF3[',
          'species_text': 'Human',
          'fragment_length_distribution_mean': 450,
          'fragment_length_distribution_sd': 50,
          'library_preparation': 'Not Applicable',
          'sample_type': '',
          'sample_description': 'Pre made library',
          'library_type': 'Hybrid Capture - Custom',
          'expected_lanes': 1,
          'sequencing_type': 'HiSeq 4000 75 PE'},{
          'project_igf_id': 'IGFQ1 scRNA-seq5primeFB',
          'name': 'XXX YYY',
          'email_id': 'x.y@ic.ac.uk',
          'sample_igf_id': 'IGF4/',
          'species_text': 'human',
          'fragment_length_distribution_mean': 450,
          'fragment_length_distribution_sd': 50,
          'library_preparation': 'Not Applicable',
          'sample_type': '',
          'sample_description': 'Pre made library',
          'library_type': 'Hybrid Capture - Custom',
          'expected_lanes': 1,
          'sequencing_type': 'HiSeq 4000 75 PE'},{
          'project_igf_id': 'IGFQ1 scRNA-seq5primeFB',
          'name': 'XXX YYY',
          'email_id': 'x.y@ic.ac.uk',
          'sample_igf_id': 'IGF5*',
          'species_text': 'human ',
          'fragment_length_distribution_mean': 475,
          'fragment_length_distribution_sd': 50,
          'library_preparation': 'Not Applicable',
          'sample_type': '',
          'sample_description': 'Pre made library',
          'library_type': 'Hybrid Capture - Custom',
          'expected_lanes': 1,
          'sequencing_type': 'HiSeq 4000 75 PE'}]
      pd.DataFrame(metadata).to_csv(self.metadata_file, index=False)

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_reformat_metadata_files(self):
    _reformat_metadata_files(self.temp_dir)
    self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'formatted_data', 'project1_reformatted.csv')))
    data = \
      pd.read_csv(os.path.join(self.temp_dir, 'formatted_data', 'project1_reformatted.csv'))
    self.assertTrue('library_source' in data.columns)
    sample_igf1_library_strategy = data[data['sample_igf_id']=='IGF1']['library_strategy'].values[0]
    self.assertEqual(sample_igf1_library_strategy,'TARGETED-CAPTURE')
    sample_igf2_experiment_type = data[data['sample_igf_id']=='IGF2']['experiment_type'].values[0]
    self.assertEqual(sample_igf2_experiment_type,'TENX-TRANSCRIPTOME-3P')
    sample_igf2_biomaterial_type = data[data['sample_igf_id']=='IGF2']['biomaterial_type'].values[0]
    self.assertEqual(sample_igf2_biomaterial_type,'SINGLE_NUCLEI')

if __name__=='__main__':
  unittest.main()