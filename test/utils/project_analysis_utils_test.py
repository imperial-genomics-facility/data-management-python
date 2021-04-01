import os,unittest,json
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.igfTables import Collection
from igf_data.igfdb.igfTables import Collection_attribute
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.fileutils import get_temp_dir
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

class Project_analysis_test3(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    self.temp_dir = get_temp_dir()
    project_data = [{'project_igf_id':'ProjectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)                      # load project data
    sample_data = [{
      'sample_igf_id':'SampleA',
      'project_igf_id':'ProjectA'},{
      'sample_igf_id':'SampleB',
      'project_igf_id':'ProjectA'}]                                             # sample data
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)                        # store sample data
    file_type_data = [
      ["ANALYSIS_CRAM","rds/IGF116623_cellranger_HG38_20210330.cram"],
      ["CELLRANGER_HTML","rds/IGF116623_cellranger_multi_HG38_20210330.html"],
      ["CELLRANGER_MULTI","rds/IGF116623_cellranger_multi_HG38_20210330.tar.gz"],
      ["FTP_CELLBROWSER","www/cellbrowser_html_20210330"],
      ["FTP_CELLRANGER_HTML","www/IGF116623_cellranger_multi_HG38_20210330.html"],
      ["FTP_MULTIQC_HTML","www/IGF116623_multiqc_HG38_20210330.html"],
      ["FTP_SCANPY_HTML","www/IGF116623_scanpy_5p_HG38_20210330.0.4.1.html"],
      ["FTP_SCIRPY_VDJ_T_HTML","www/IGF116623_scirpy_vdj_t_HG38_20210330.0.2.html"],
      ["FTP_SEURAT_HTML","www/IGF116623_seurat_5p_HG38_20210330.0.1.html"],
      ["MULTIQC_HTML","rds/IGF116623_multiqc_HG38_20210330.html"],
      ["SCANPY_HTML","rds/IGF116623_scanpy_5p_HG38_20210330.0.4.1.html"],
      ["SCIRPY_VDJ_T_HTML","rds/IGF116623_scirpy_vdj_t_HG38_20210330.0.2.html"],
      ["SEURAT_HTML","rds/IGF116623_seurat_5p_HG38_20210330.0.1.html"]]
    collection_data = []
    os.makedirs(os.path.join(self.temp_dir,'rds'))
    os.makedirs(os.path.join(self.temp_dir,'www'))
    for entry in file_type_data:
      collection_data.append({
        'name':'SampleA','type':entry[0],'table':'sample',
        'file_path':os.path.join(self.temp_dir,entry[1])})
      with open(os.path.join(self.temp_dir,entry[1]),'w') as fp:
        fp.write('A')
    ca = CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=collection_data,
      calculate_file_size_and_md5=False)
    ca.create_or_update_collection_attributes([dict(
         name='SampleA',
         type='ANALYSIS_CRAM',
         attribute_name='read_count',
         attribute_value='2000')],
         autosave=True)
    ca.create_or_update_collection_attributes([dict(
         name='SampleA',
         type='ANALYSIS_CRAM',
         attribute_name='total_count',
         attribute_value='2000')],
         autosave=True)
    analysis_data = [{
      'project_igf_id':'ProjectA',
      'analysis_name':'analysis_1',
      'analysis_type':'type_1',
      'analysis_description':'[{"sample_igf_id":"Sample1"}]'}]
    aa = \
      AnalysisAdaptor(**{'session':base.session})
    aa.store_analysis_data(data=analysis_data)
    pla = PipelineAdaptor(**{'session':base.session})
    pipeline_data = [{
      "pipeline_name" : "pipeline1",
      "pipeline_db" : "postgres", 
      "pipeline_init_conf" : None,
      "pipeline_run_conf" : None}]
    pla.store_pipeline_data(data=pipeline_data)
    pipeline_seed_data = [{
      'pipeline_name':'pipeline1',
      'seed_id':'1',
      'seed_table':'analysis'}]
    pla.create_pipeline_seed(data=pipeline_seed_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(dir_path=self.temp_dir)

  def test_get_analysis_data_for_project_2(self):
    #ca = CollectionAdaptor(**{'session_class':self.session_class})
    #ca.start_session()
    #query = \
    #  ca.session.query(Collection_attribute)
    #records = ca.fetch_records(query=query)
    #ca.close_session()
    #print(records.to_dict(orient='records'))
    output_file = \
      os.path.join(
        self.temp_dir,
        'test.json')
    chart_output_file = \
      os.path.join(
        self.temp_dir,
        'chart_test.json')
    cvs_output_file =  \
      os.path.join(
        self.temp_dir,
        'chart_test.csv')
    prj_data = \
      Project_analysis(
        igf_session_class=self.session_class,
        pipeline_seed_table='analysis',
        pipeline_name='pipeline1',
        pipeline_finished_status='SEEDED',
        attribute_collection_file_type=['ANALYSIS_CRAM'],
        collection_type_list=['FTP_CELLBROWSER',
                              'FTP_CELLRANGER_HTML',
                              'FTP_MULTIQC_HTML',
                              'FTP_SCANPY_HTML',
                              'FTP_SCIRPY_VDJ_T_HTML',
                              'FTP_SEURAT_HTML'])
    prj_data.\
      get_analysis_data_for_project(\
        project_igf_id='ProjectA',
        output_file=output_file,
        chart_json_output_file=chart_output_file,
        csv_output_file=cvs_output_file,
        gviz_out=False)
    data = pd.read_csv(output_file)
    self.assertTrue('SampleA' in data['sample_igf_id'].values)
    self.assertEqual(data[data['type']=='FTP_CELLRANGER_HTML']['file_path'].values[0],os.path.join(self.temp_dir,'www/IGF116623_cellranger_multi_HG38_20210330.html'))
    with open(chart_output_file,'r') as jp:
      data = json.load(jp)
    self.assertTrue('cols' in data.keys())
    self.assertEqual(len(data['rows']),1)
    with open(cvs_output_file,'r') as jp:
      data = jp.read()
      data = data.split('\n')
      self.assertTrue('read_count' in data[0].split(','))
      self.assertTrue('SampleA' in data[1].split(','))
if __name__=='__main__':
  unittest.main()