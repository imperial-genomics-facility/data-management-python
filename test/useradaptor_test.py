import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json
from igf_data.igfdb.useradaptor import UserAdaptor

class Useradaptor_test1(unittest.TestCase):
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
  
  def test_email_check(self):
    ua=UserAdaptor(**{'session_class': self.session_class})
    with self.assertRaises(ValueError):
      ua._email_check(email='a_b.com')
    self.assertFalse(ua._email_check(email='a@b.com'))

  def test__encrypt_password(self):
    ua=UserAdaptor(**{'session_class': self.session_class})
    user_data=pd.Series({'name':'AAAA','password':'BBB'})
    user_data=ua._encrypt_password(series=user_data)
    user_data=user_data.to_dict()
    self.assertTrue('encryption_salt' in user_data)
    self.assertTrue('ht_password' in user_data)

  def test_map_missing_user_status(self):
    ua=UserAdaptor(**{'session_class': self.session_class})
    user_data1=pd.Series({'name':'AAAA','hpc_username':'BBB'})
    user_data1=ua._map_missing_user_status(data_series=user_data1,\
                                          categoty_column='category',\
                                          hpc_user_column='hpc_username',\
                                          hpc_user='HPC_USER',\
                                          non_hpc_user='NON_HPC_USER')
    user_data1=user_data1.to_dict()
    self.assertEqual(user_data1['category'],'HPC_USER')
    user_data2=pd.Series({'name':'AAAA','hpc_username':''})
    user_data2=ua._map_missing_user_status(data_series=user_data2,\
                                          categoty_column='category',\
                                          hpc_user_column='hpc_username',\
                                          hpc_user='HPC_USER',\
                                          non_hpc_user='NON_HPC_USER')
    user_data2=user_data2.to_dict()
    self.assertEqual(user_data2['category'],'NON_HPC_USER')
    
if __name__ == '__main__':
  unittest.main()
  