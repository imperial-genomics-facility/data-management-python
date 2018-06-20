import os, unittest, sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.fileutils import get_datestamp_label
from igf_data.utils.fileutils import preprocess_path_name
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
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
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
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

    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    platform_data=[{ "platform_igf_id" : "M001",
                     "model_name" : "MISEQ" ,
                     "vendor_name" : "ILLUMINA" ,
                     "software_name" : "RTA",
                     "software_version" : "RTA1.18.54"}]                        # platform data
    flowcell_rule_data=[{"platform_igf_id":"M001",
                         "flowcell_type":"MISEQ",
                         "index_1":"NO_CHANGE",
                         "index_2":"NO_CHANGE"}]                                # flowcell rule data
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)                                  # loading platform data
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)                     # loading flowcell rules data
    project_data=[{'project_igf_id':'ProjectA'}]                                # project data
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)                      # load project data
    sample_data=[{'sample_igf_id':'SampleA',
                  'project_igf_id':'ProjectA'}]                                 # sample data
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)                        # store sample data
    seqrun_data=[{'seqrun_igf_id':'SeqrunA', 
                  'flowcell_id':'000000000-D0YLK', 
                  'platform_igf_id':'M001',
                  'flowcell':'MISEQ'}]                                          # seqrun data
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)                       # load seqrun data
    experiment_data=[{'experiment_igf_id':'ExperimentA',
                      'sample_igf_id':'SampleA',
                      'library_name':'SampleA',
                      'platform_name':'MISEQ',
                      'project_igf_id':'ProjectA'}]                             # experiment data
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)                   # load experiment data
    run_data=[{'run_igf_id':'RunA',
               'experiment_igf_id':'ExperimentA',
               'seqrun_igf_id':'SeqrunA',
               'lane_number':'1'}]                                              # run data
    ra=RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)                              # load run data
    base.commit_session()
    base.close_session()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    remove_dir(dir_path=self.temp_work_dir)
    remove_dir(dir_path=self.temp_base_dir)

  def test_create_or_update_analysis_collection_rename(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
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
    au=Analysis_collection_utils(dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project'
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    self.assertEqual(len(ca_files.index),
                     len(self.input_list))                                      # compare with input list
    self.assertEqual(len(output_list),
                     len(self.input_list))                                      # compare with output list
    base.close_session()


  def test_load_file_to_disk_and_db2(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ProjectA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='project'
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=True)  # withdrawing existing collection group before loading new
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
    self.assertEqual(len(output_list),3)
    base.close_session()

  def test_load_file_to_disk_and_db3(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
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
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection
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
    test_file=preprocess_path_name(input_path=test_file)
    self.assertTrue(test_file in file_list)
    self.assertTrue(test_file in output_list)
    base.close_session()


  def test_load_file_to_disk_and_db4(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
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
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection, without rename
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ProjectA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    self.assertTrue(input_file_list[0] in file_list)
    self.assertTrue(input_file_list[0] in output_list)
    base.close_session()

  def test_load_file_to_disk_and_db5(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='SampleA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='sample',
                                 base_path=self.temp_base_dir
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='SampleA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    datestamp=get_datestamp_label()
    test_file=os.path.join(self.temp_base_dir,
                          'ProjectA',
                          'SampleA',
                          'AnalysisA',
                          '{0}_{1}_{2}_{3}.{4}'.format('SampleA',
                                                        'AnalysisA',
                                                        'TagA',
                                                        datestamp,
                                                        'cram'))
    test_file=preprocess_path_name(input_path=test_file)
    self.assertTrue(test_file in file_list)
    self.assertTrue(test_file in output_list)
    base.close_session()

  def test_load_file_to_disk_and_db6(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='ExperimentA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='experiment',
                                 base_path=self.temp_base_dir
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='ExperimentA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    datestamp=get_datestamp_label()
    test_file=os.path.join(self.temp_base_dir,
                          'ProjectA',
                          'SampleA',
                          'ExperimentA',
                          'AnalysisA',
                          '{0}_{1}_{2}_{3}.{4}'.format('ExperimentA',
                                                        'AnalysisA',
                                                        'TagA',
                                                        datestamp,
                                                        'cram'))
    test_file=preprocess_path_name(input_path=test_file)
    self.assertTrue(test_file in file_list)
    self.assertTrue(test_file in output_list)
    base.close_session()


  def test_load_file_to_disk_and_db7(self):
    au=Analysis_collection_utils(dbsession_class=self.session_class,
                                 analysis_name='AnalysisA',
                                 tag_name='TagA',
                                 collection_name='RunA',
                                 collection_type='AnalysisA_Files',
                                 collection_table='run',
                                 base_path=self.temp_base_dir
                                )
    input_file_list=[os.path.join(self.temp_work_dir,
                                  file_name)
                      for file_name in self.input_list]
    output_list=au.load_file_to_disk_and_db(input_file_list=input_file_list,
                                            withdraw_exisitng_collection=False) # loading all files to same collection
    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    ca=CollectionAdaptor(**{'session':base.session})
    ca_files=ca.get_collection_files(collection_name='RunA',
                                     collection_type='AnalysisA_Files',
                                     output_mode='dataframe')
    file_list=list(ca_files['file_path'].to_dict().values())
    datestamp=get_datestamp_label()
    test_file=os.path.join(self.temp_base_dir,
                          'ProjectA',
                          'SampleA',
                          'ExperimentA',
                          'RunA',
                          'AnalysisA',
                          '{0}_{1}_{2}_{3}.{4}'.format('RunA',
                                                        'AnalysisA',
                                                        'TagA',
                                                        datestamp,
                                                        'cram'))
    test_file=preprocess_path_name(input_path=test_file)
    self.assertTrue(test_file in file_list)
    self.assertTrue(test_file in output_list)
    base.close_session()

if __name__=='__main__':
  unittest.main()