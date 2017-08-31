import unittest, sqlalchemy
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.platformutils import load_new_platform_data

class Platformutils_test1(unittest.TestCase):
  def setUp(self):
    self.data_file='data/platform.json'

  def test_read_json_data(self):
    data=read_json_data(data_file=self.data_file)
    self.assertIsInstance(data,list)

if __name__ == '__main__':
  unittest.main()
