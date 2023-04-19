import os
import unittest
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_airflow.utils.dag30_register_raw_analysis_to_pipeline_db_utils import (
    check_and_register_new_analysis_data,
    check_and_trigger_new_analysis)


class Test_dag30_register_raw_analysis_to_pipeline_db_utils(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    ## 1. register test project 1
    project_data = [{'project_igf_id': 'projectA'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_data = [{'project_igf_id': 'projectB'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    project_data = [{'project_igf_id': 'projectC'}]
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    ## 2. register pipeline 1
    pipeline_data = [{
      "pipeline_name": "pipeline1",
      "pipeline_db": "sqlite:////data/aln.db",
      "pipeline_init_conf": {},
      "pipeline_run_conf": {}
    }]
    pl = PipelineAdaptor(**{'session': base.session})
    pl.store_pipeline_data(data=pipeline_data)
    ## 3. register analysis 1
    analysis_data = [{
      'project_igf_id': 'projectA',
      'analysis_name': 'analysis_1',
      'analysis_type': 'pipeline1',
      'analysis_description': '[{"sample_igf_id": "XYZ"}]'}]
    aa = \
      AnalysisAdaptor(**{'session': base.session})
    aa.store_analysis_data(data=analysis_data)
    pipeline_seed_data = [{
      'pipeline_id': 1,
      'seed_id': 1,
      'seed_table': 'analysis'}]
    pl.create_pipeline_seed(pipeline_seed_data)
    base.close_session()


  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_check_and_register_new_analysis_data(self):
    test1 = \
      check_and_register_new_analysis_data(
        project_id=1,
        pipeline_id=1,
        analysis_name='analysis_1',
        analysis_yaml='new_data',
        dbconf_json='data/dbconfig.json')
    self.assertFalse(test1)
    test2 = \
      check_and_register_new_analysis_data(
        project_id=2,
        pipeline_id=1,
        analysis_name='analysis_1',
        analysis_yaml='new_data',
        dbconf_json='data/dbconfig.json')
    self.assertTrue(test2)
    aa = \
      AnalysisAdaptor(**{'session_class': self.session_class})
    aa.start_session()
    analysis_id = \
      aa.check_analysis_record_by_analysis_name_and_project_id(
        analysis_name='analysis_1',
        project_id=2)
    self.assertIsNotNone(analysis_id)
    with self.assertRaises(ValueError):
      _ = \
        check_and_register_new_analysis_data(
          project_id=3,
          pipeline_id=2,
          analysis_name='analysis_1',
          analysis_yaml='new_data',
          dbconf_json='data/dbconfig.json')


  def test_check_and_trigger_new_analysis1(self):
    pipeline_trigger1 = \
      check_and_trigger_new_analysis(
        project_id=1,
        pipeline_id=1,
        analysis_name='analysis_1',
        dbconf_json='data/dbconfig.json')
    self.assertFalse(pipeline_trigger1)
    with self.assertRaises(ValueError):
      _ = \
        check_and_trigger_new_analysis(
          project_id=2,
          pipeline_id=1,
          analysis_name='analysis_1',
          dbconf_json='data/dbconfig.json')

  def test_check_and_trigger_new_analysis2(self):
    test2 = \
      check_and_register_new_analysis_data(
        project_id=2,
        pipeline_id=1,
        analysis_name='analysis_1',
        analysis_yaml='new_data',
        dbconf_json='data/dbconfig.json')
    self.assertTrue(test2)
    pipeline_trigger2 = \
      check_and_trigger_new_analysis(
        project_id=2,
        pipeline_id=1,
        analysis_name='analysis_1',
        dbconf_json='data/dbconfig.json')
    self.assertTrue(pipeline_trigger2)
    with self.assertRaises(ValueError):
      _ = \
        check_and_trigger_new_analysis(
        project_id=2,
        pipeline_id=2,
        analysis_name='analysis_1',
        dbconf_json='data/dbconfig.json')


if __name__=='__main__':
  unittest.main()