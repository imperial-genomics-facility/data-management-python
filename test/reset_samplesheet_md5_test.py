import unittest,os
import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.collectionadaptor import CollectionAdaptor

class Reset_samplesheet_md5_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    self.json_file_path='data/reset_samplesheet_md5/seqrun1_file_md5.json'
    self.json_collection_name='seqrun1'
    self.json_collection_type='ILLUMINA_BCL_MD5'
    self.seqrun_path='data/reset_samplesheet_md5'
    self.seqrun_input_list='data/reset_samplesheet_md5/seqrun_input_list.txt'
    ca=CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    data=pd.DataFRame([{'name':self.json_collection_name,
                        'type':self.json_collection_type,
                        'table':'seqrun',
                        'file_path':self.json_file_path,
                        }])
    ca.load_file_and_create_collection(data,
                                       autosave=True,
                                       hasher='md5')
    ca.close_session()
    with open(self.seqrun_input_list,'w') as fp:
      fp.write(self.json_collection_name)

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    os.remove(self.seqrun_input_list)

