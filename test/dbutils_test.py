import unittest
from igf_data.utils.dbutils import read_dbconf_json, clean_and_rebuild_database

class Dbutils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/incorrect_dbconfig.json'
  
  def test_read_dbconf_json(self):
    with self.assertRaises(ValueError):
      read_dbconf_json(dbconfig=self.dbconfig)
    

if __name__ == '__main__':
  unittest.main()
