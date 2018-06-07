import os, unittest, sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
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
    self.temp_work_dir=get_temp_dir()
    self.temp_base_dir=get_temp_dir()
    self.input_list=['a.cram',
                     'a.vcf.gz',
                     'b.tar.gz']
    for file_name in self.input_list:
      file_path=os.path.join(self.temp_work_dir,
                             file_name)
      with open(file_path,'w') as fq:
        fq.write('AAAA')                                                        # create input files

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    remove_dir(dir_path=self.temp_work_dir)
    remove_dir(dir_path=self.temp_base_dir)

  def test_create_or_update_analysis_collection_rename(self):
    au=Analysis_collection_utils(project_igf_id='ProjectA',
                                 dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project'
                                )
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    au.create_or_update_analysis_collection(file_path=os.path.join(self.temp_work_dir,
                                                                   'a.cram'),
                                            dbsession=base.session,
                                            autosave_db=True
                                           )
    base.close_session()
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    self.assertEqual(len(ca_files.index),1)
    au.create_or_update_analysis_collection(file_path=os.path.join(self.temp_work_dir,
                                                                   'a.cram'),
                                            dbsession=base.session,
                                            autosave_db=True,
                                            force=True
                                           )                                    # overwriting file collection
    base.close_session()
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    self.assertEqual(len(ca_files.index),1)

    with self.assertRaises(sqlalchemy.exc.IntegrityError):                      # file collection without force
      au.create_or_update_analysis_collection(\
        file_path=os.path.join(self.temp_work_dir,
                               'a.cram'),
        dbsession=base.session,
        autosave_db=True,
        force=False
      )
    base.close_session()

if __name__=='__main__':
  unittest.main()