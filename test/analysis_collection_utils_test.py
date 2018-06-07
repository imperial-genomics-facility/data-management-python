import os, unittest
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.igfdb.igfTables import Base,File,Collection,Collection_group
from igf_data.utils.analysis_collection_utils import Analysis_collection_utils

class Analysis_collection_utils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)


if __name__=='__main__':
  unittest.main()