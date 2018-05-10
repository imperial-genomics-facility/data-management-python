import unittest,os
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.dbutils import read_dbconf_json

class Fileadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_update_file_table_for_file_path(self):
    fa=FileAdaptor(**{'session_class': self.session_class})
    fa.start_session()
    file_data=pd.DataFrame([{'file_path':'AAAA','size':'10','md5':'AAAAAAAAAAAAAAAA'}])
    fa.store_file_and_attribute_data(data=file_data, autosave=True)
    file_obj=fa.fetch_file_records_file_path(file_path='AAAA')
    self.assertEqual(file_obj.md5,'AAAAAAAAAAAAAAAA')
    fa.update_file_table_for_file_path(file_path='AAAA', 
                                       tag='md5',
                                       value='BBBBBBBBBBBBBB', 
                                       autosave=True)
    fa.close_session()
    fa.start_session()
    file_obj=fa.fetch_file_records_file_path(file_path='AAAA')
    self.assertEqual(file_obj.md5,'BBBBBBBBBBBBBB')
    fa.close_session()
    fa.start_session()
    fa.update_file_table_for_file_path(file_path='AAAA', 
                                       tag='md5',
                                       value='CCCCCCCCCCCCCCC', 
                                       autosave=False)
    fa.close_session()
    fa.start_session()
    file_obj=fa.fetch_file_records_file_path(file_path='AAAA')
    self.assertEqual(file_obj.md5,'BBBBBBBBBBBBBB')
    fa.close_session()

if __name__ == '__main__':
  unittest.main()