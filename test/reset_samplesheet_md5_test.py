import unittest,os,json
import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.process.seqrun_processing.reset_samplesheet_md5 import Reset_samplesheet_md5

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
    json_data=pd.DataFrame([{'file_md5': '1e7531158974b5a5b7cbb7dde09ac779',
                             'seqrun_file_name': 'SampleSheet.csv'},
                            {'file_md5': '2b22f945bc9e7e390af5432425783a03',
                             'seqrun_file_name': 'RTAConfiguration.xml'}
                           ])
    with open(self.json_file_path,'w') as jp:
      json.dump(json_data.to_dict(orient='record'),jp,indent=4)
    self.initial_json_md5=calculate_file_checksum(filepath=self.json_file_path)
    self.correct_samplesheet_md5='259ed03f2e8c45980de121f7c3a70565'
    self.json_collection_name='seqrun1'
    self.json_collection_type='ILLUMINA_BCL_MD5'
    self.seqrun_path='data/reset_samplesheet_md5'
    self.seqrun_input_list='data/reset_samplesheet_md5/seqrun_input_list.txt'
    ca=CollectionAdaptor(**{'session_class': self.session_class})
    ca.start_session()
    data=pd.DataFrame([{'name':self.json_collection_name,
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
    os.remove(self.json_file_path)

  def test_run_1(self):
    rs=Reset_samplesheet_md5(seqrun_path=self.seqrun_path,
                             seqrun_igf_list=self.seqrun_input_list,
                             dbconfig_file=self.dbconfig,
                             log_slack=False, 
                             log_asana=False)
    rs.run()
    with open(self.json_file_path,'r') as jp:
      json_data=json.load(jp)

    new_md5_value=''
    for json_row in json_data:
      if json_row['seqrun_file_name']=='SampleSheet.csv':
        new_md5_value=json_row['file_md5']
    self.assertEqual(self.correct_samplesheet_md5,new_md5_value)                # check json file for updated samplesheet md5
    fa=FileAdaptor(**{'session_class': self.session_class})
    fa.start_session()
    file=fa.fetch_file_records_file_path(file_path=self.json_file_path)
    self.assertNotEqual(file.md5, self.initial_json_md5)                        # check db for updated json file md5 value
    fa.close_session()

if __name__ == '__main__':
  unittest.main()