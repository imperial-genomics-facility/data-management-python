import unittest, os
import pandas as pd
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import find_all_analysis_yaml_files
from igf_airflow.utils.dag18_upload_and_trigger_analysis_utils import get_new_file_list

class Dag18_upload_and_trigger_analysis_utils_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.yaml_file = os.path.join(self.temp_dir, 'a.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write('A')
    with open(os.path.join(self.temp_dir, 'a.yam'), 'w') as fp:
      fp.write('A')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_find_all_analysis_yaml_files(self):
    yaml_files = \
      find_all_analysis_yaml_files(self.temp_dir)
    self.assertEqual(len(yaml_files), 1)
    self.assertTrue(os.path.join(self.temp_dir, 'a.yaml') in yaml_files)

class Dag18_upload_and_trigger_analysis_utils_testB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.yaml_file = os.path.join(self.temp_dir, 'a.yaml')
    with open(self.yaml_file, 'w') as fp:
      fp.write('A')
    with open(os.path.join(self.temp_dir, 'b.yaml'), 'w') as fp:
      fp.write('A')
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    fa = FileAdaptor(**dbparam)
    self.engine = fa.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = fa.get_session_class()
    fa.start_session()
    file_data = \
      pd.DataFrame([{'file_path': os.path.join(self.temp_dir, 'b.yaml')}])
    fa.store_file_and_attribute_data(
      data=file_data,
      autosave=True)
    fa.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_get_new_file_list(self):
    yaml_files = \
      find_all_analysis_yaml_files(self.temp_dir)
    new_files = \
      get_new_file_list(
        all_files=yaml_files,
        db_config_file=self.dbconfig)
    self.assertEqual(len(new_files), 1)
    self.assertTrue(os.path.join(self.temp_dir, 'a.yaml') in new_files)

if __name__=='__main__':
  unittest.main()