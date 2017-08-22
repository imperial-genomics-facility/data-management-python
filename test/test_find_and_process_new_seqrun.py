import unittest, json, os, shutil
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import find_new_seqrun_dir,calculate_file_md5,load_seqrun_files_to_db

class Find_seqrun_test1(unittest.TestCase):
  def setUp(self):
    self.path = 'data/seqrun_dir'
    self.dbconfig = 'data/dbconfig.json'
    self.md5_out_path = 'data/md5_dir'
    seqrun_json = 'data/seqrun_db_data.json'
    platform_json = 'data/platform_db_data.json'

    os.mkdir(self.md5_out_path)
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    base.start_session()

    with open(platform_json, 'r') as json_data:
      platform_data=json.load(json_data)
      pl=PlatformAdaptor(**{'session':base.session})
      pl.store_platform_data(data=platform_data)
        
    with open(seqrun_json, 'r') as json_data:
      seqrun_data=json.load(json_data)
      sra=SeqrunAdaptor(**{'session':base.session})
      sra.store_seqrun_and_attribute_data(data=seqrun_data)
      base.close_session()

  def tearDown(self):
     #shutil.copyfile(self.dbname, 'test.db')
     Base.metadata.drop_all(self.engine)
     os.remove(self.dbname)
     shutil.rmtree(self.md5_out_path, ignore_errors=False, onerror=None)

  def test_find_new_seqrun_dir(self):
    valid_seqrun_dir=find_new_seqrun_dir(path=self.path,dbconfig=self.dbconfig) 
    self.assertEqual(list(valid_seqrun_dir.keys())[0],'seqrun1')

  def test_calculate_file_md5(self):
    valid_seqrun_dir=find_new_seqrun_dir(path=self.path,dbconfig=self.dbconfig) 
    new_seqrun_and_md5=calculate_file_md5(seqrun_info=valid_seqrun_dir, md5_out=self.md5_out_path, seqrun_path=self.path)
    md5_file_name=new_seqrun_and_md5['seqrun1']
    with open(md5_file_name, 'r') as json_data:
      md5_data=json.load(json_data)
    shutil.rmtree(self.md5_out_path, ignore_errors=False, onerror=None)
    os.mkdir(self.md5_out_path)
    self.assertEqual(md5_data["RTAComplete.txt"], "c514939fdd61df26b103925a5122b356")

  def test_load_seqrun_files_to_db(self):
    valid_seqrun_dir=find_new_seqrun_dir(path=self.path,dbconfig=self.dbconfig)
    new_seqrun_and_md5=calculate_file_md5(seqrun_info=valid_seqrun_dir, md5_out=self.md5_out_path, seqrun_path=self.path) 
    load_seqrun_files_to_db(seqrun_info=valid_seqrun_dir, seqrun_md5_info=new_seqrun_and_md5, dbconfig=self.dbconfig)
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    sra=SeqrunAdaptor(**dbparam)
    sra.start_session()
    sra_data=sra.fetch_seqrun_records_igf_id(seqrun_igf_id='seqrun1')
    sra.close_session()
    self.assertEqual(sra_data.flowcell_id, 'HXXXXXXXX')
  
if __name__=='__main__':
  unittest.main()

