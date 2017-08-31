import os, unittest, json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Platform
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.platformutils import load_new_platform_data

class Platformutils_test1(unittest.TestCase):
  def setUp(self):
    self.data_file = 'data/platform.json'

  def test_read_json_data(self):
    data=read_json_data(data_file=self.data_file)
    self.assertIsInstance(data,list)



class Platformutils_test2(unittest.TestCase):
  def setUp(self):
    self.data_file = 'data/platform.json'
    self.dbconfig = 'data/dbconfig.json'

    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)

    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    self.pipeline_name=''
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_load_new_platform_data(self):
    load_new_platform_data(data_file=self.data_file, dbconfig=self.dbconfig)
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    data=pl.fetch_platform_records_igf_id(platform_igf_id='ILM4K_001')
    pl.close_session()
    self.assertEqual(data.platform_igf_id,'ILM4K_001')

    
if __name__ == '__main__':
  unittest.main()
