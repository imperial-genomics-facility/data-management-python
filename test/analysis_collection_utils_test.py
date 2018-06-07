import os, unittest, sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.fileutils import get_datestamp_label
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

  def test_load_file_to_disk_and_db1(self):
    au=Analysis_collection_utils(project_igf_id='ProjectA',
                                 dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project'
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                withdraw_exisitng_collection=False)             # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    self.assertEqual(len(ca_files.index),
                     len(self.input_list))                                      # compare with input list
    base.close_session()


  def test_load_file_to_disk_and_db2(self):
    au=Analysis_collection_utils(project_igf_id='ProjectA',
                                 dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project'
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                withdraw_exisitng_collection=True)              # withdrawing existing collection group before loading new
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    self.assertEqual(len(ca_files.index),1)                                     # check for unique collection group
    fa=FileAdaptor(**{'session':base.session})
    query=fa.session.query(File)
    fa_records=fa.fetch_records(query=query, output_mode='dataframe')
    self.assertEqual(len(fa_records['file_path'].to_dict()),3)                  # check if all files are present although only one collection group exists
    base.close_session()

  def test_load_file_to_disk_and_db3(self):
    au=Analysis_collection_utils(project_igf_id='ProjectA',
                                 dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project',
                                 base_path=self.temp_base_dir
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                withdraw_exisitng_collection=False)             # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    datestamp=get_datestamp_label()
    test_file=os.path.join(self.temp_base_dir,
                          'ProjectA',
                          'AnalysisA',
                          '{0}_{1}_{2}_{3}.{4}'.format('ProjectA',
                                                        'AnalysisA',
                                                        'TagA',
                                                        datestamp,
                                                        'cram'))
    self.assertTrue(test_file in file_list)
    base.close_session()


  def test_load_file_to_disk_and_db4(self):
    au=Analysis_collection_utils(project_igf_id='ProjectA',
                                 dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project',
                                 rename_file=False
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                withdraw_exisitng_collection=False)             # loading all files to same collection, without rename
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    self.assertTrue(input_file_list[0] in file_list)

if __name__=='__main__':
  unittest.main()