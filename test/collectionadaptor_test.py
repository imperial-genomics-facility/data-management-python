import unittest, json, os, shutil
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor

class CollectionAdaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.session_class
  
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    
  def test_load_file_and_create_collection(self):
    file_path='data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00002/IGF00002-2_S1_L001_R1_001.fastq.gz'
    data=[{'name':'IGF00001_MISEQ_000000000-D0YLK_1',\
           'type':'demultiplexed_fastq',\
           'table':'run',\
           'file_path':file_path,\
           'location':'HPC_PROJECT'}]
    ca=CollectionAdaptor(**{'session_class':self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(data)
    ca.close_session()
    fa=FileAdaptor(**{'session_class':self.session_class})
    fa.start_session()
    file_data=fa.fetch_file_records_file_path(file_path)
    fa.close_session()
    file_md5=file_data.md5
    self.assertEqual(file_md5, 'd5798564a14a09b9e80640ac2f42f47e')

  def test_fetch_collection_name_and_table_from_file_path(self):
    file_path='data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00002/IGF00002-2_S1_L001_R1_001.fastq.gz'
    data=[{'name':'IGF00001_MISEQ_000000000-D0YLK_1',\
           'type':'demultiplexed_fastq',\
           'table':'run',\
           'file_path':file_path,\
           'location':'HPC_PROJECT'}]
    ca=CollectionAdaptor(**{'session_class':self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(data)
    (name,table)=ca.fetch_collection_name_and_table_from_file_path(file_path)
    ca.close_session()
    self.assertEqual(name, 'IGF00001_MISEQ_000000000-D0YLK_1')

  def test_remove_collection_group_info(self):
    collection_data=[{ 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'a.cram',
                   },
                   { 'name':'IGF001_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'a1.cram',
                   },
                   { 'name':'IGF003_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'b.cram',
                   },
                   { 'name':'IGF003_MISEQ',
                     'type':'ALIGNMENT_CRAM',
                     'table':'experiment',
                     'file_path':'b1.cram',
                   }]
    ca=CollectionAdaptor(**{'session_class':self.session_class})
    ca.start_session()
    ca.load_file_and_create_collection(data=collection_data,
                                       calculate_file_size_and_md5=False)
    remove_data_list1=[{'name':'IGF001_MISEQ',
                        'type':'ALIGNMENT_CRAM',
                        'table':'experiment',
                        'file_path':'a.cram',
                       }]
    ca.remove_collection_group_info(data=remove_data_list1)
    cg_data=ca.get_collection_files(collection_name='IGF001_MISEQ',
                                    collection_type='ALIGNMENT_CRAM',
                                    output_mode='dataframe')
    self.assertEqual(len(cg_data.index), 1)
    self.assertEqual(cg_data['file_path'].values[0],'a1.cram')
    remove_data_list2=[{ 'name':'IGF003_MISEQ',
                         'type':'ALIGNMENT_CRAM',
                         'table':'experiment',
                      }]
    ca.remove_collection_group_info(data=remove_data_list2)
    cg_data2=ca.get_collection_files(collection_name='IGF003_MISEQ',
                                     collection_type='ALIGNMENT_CRAM',
                                     output_mode='dataframe')
    self.assertEqual(len(cg_data2.index), 0)
    ca.close_session()


if __name__=='__main__':
  unittest.main()