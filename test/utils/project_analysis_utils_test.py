import os,unittest,json
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.fileutils import remove_dir,get_temp_dir
from igf_data.utils.project_analysis_utils import Project_analysis

class Project_analysis_test1(unittest.TestCase):
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
    base.start_session()
    project_data=[{'project_igf_id':'ProjectA'}]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)                      # load project data
    sample_data=[{'sample_igf_id':'SampleA',
                  'project_igf_id':'ProjectA'}]                                 # sample data
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)                        # store sample data
    experiment_data=[{'experiment_igf_id':'ExperimentA',
                      'sample_igf_id':'SampleA',
                      'library_name':'SampleA',
                      'platform_name':'MISEQ',
                      'project_igf_id':'ProjectA'}]                             # experiment data
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    self.temp_dir=get_temp_dir()
    temp_files=['a.csv']
    for temp_file in temp_files:
      with open(os.path.join(self.temp_dir,temp_file),'w') as fp:
        fp.write('A')
    collection_data=[{'name':'ExperimentA',
                      'type':'AnalysisA_html',
                      'table':'experiment',
                      'file_path':os.path.join(self.temp_dir,temp_file)}
                      for temp_file in temp_files]
    ca=CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(data=collection_data,
                                       calculate_file_size_and_md5=False)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(dir_path=self.temp_dir)

  def test_get_analysis_data_for_project(self):
    output_file=os.path.join(self.temp_dir,
                             'test.json')
    prj_data=Project_analysis(\
               igf_session_class=self.session_class,
               collection_type_list=['AnalysisA_html'])
    prj_data.\
    get_analysis_data_for_project(\
      project_igf_id='ProjectA',
      output_file=output_file,
      gviz_out=False)
    data=pd.read_csv(output_file)
    self.assertEqual(len(data.index),1)
    self.assertTrue('AnalysisA_html' in data['type'].values)
    prj_data.\
    get_analysis_data_for_project(\
      project_igf_id='ProjectA',
      output_file=output_file,
      gviz_out=True)
    with open(output_file,'r') as jp:
      json_data=json.load(jp)
    self.assertEqual(len(json_data.get('rows')[0].get('c')),2)

class Project_analysis_test2(unittest.TestCase):
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
    base.start_session()
    project_data=[{'project_igf_id':'ProjectA'}]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)                      # load project data
    sample_data=[{'sample_igf_id':'SampleA',
                  'project_igf_id':'ProjectA'}]                                 # sample data
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)                        # store sample data
    experiment_data=[{'experiment_igf_id':'ExperimentA',
                      'sample_igf_id':'SampleA',
                      'library_name':'SampleA',
                      'platform_name':'MISEQ',
                      'project_igf_id':'ProjectA'}]                             # experiment data
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    self.temp_dir=get_temp_dir()
    temp_files=['a.csv','b.csv']
    for temp_file in temp_files:
      with open(os.path.join(self.temp_dir,temp_file),'w') as fp:
        fp.write('A')
    collection_data=[{'name':'ExperimentA',
                      'type':'AnalysisA_html',
                      'table':'experiment',
                      'file_path':os.path.join(self.temp_dir,temp_file)}
                      for temp_file in temp_files]
    ca=CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(data=collection_data,
                                       calculate_file_size_and_md5=False)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(dir_path=self.temp_dir)

  def test_get_analysis_data_for_project(self):
    output_file=os.path.join(self.temp_dir,
                             'test.json')
    prj_data=Project_analysis(\
               igf_session_class=self.session_class,
               collection_type_list=['AnalysisA_html'])
    prj_data.\
    get_analysis_data_for_project(\
      project_igf_id='ProjectA',
      output_file=output_file,
      gviz_out=False)
    data=pd.read_csv(output_file)
    self.assertEqual(len(data.index),2)
    self.assertTrue('AnalysisA_html' in data['type'].values)
    #print(data.to_dict(orient='records'))
    prj_data.\
    get_analysis_data_for_project(\
      project_igf_id='ProjectA',
      output_file=output_file,
      gviz_out=True)
    with open(output_file,'r') as jp:
      json_data=json.load(jp)
    #print(json_data)