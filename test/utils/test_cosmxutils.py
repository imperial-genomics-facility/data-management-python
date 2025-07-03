import unittest, os
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import (
  Cosmx_platform,
  Cosmx_run,
  Project,
  Cosmx_slide,
  Cosmx_fov,
  Cosmx_fov_annotation,
  Cosmx_fov_rna_qc,
  Cosmx_fov_protein_qc,
  Cosmx_slide_attribute,
  Cosmx_fov_attribute)
from igf_data.utils.cosmxutils import (
  check_and_register_cosmx_run,
  check_and_register_cosmx_slide,
  create_or_update_cosmx_slide_fov,
  create_or_update_cosmx_slide_fov_annotation,
  create_or_update_cosmx_slide_fov_count_qc
)

class Analysisadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    self.base = BaseAdaptor(**dbparam)
    self.engine = self.base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)


  def test_check_and_register_cosmx_run(self):
    project_data = [{'project_igf_id':'project1'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    project = Project(project_igf_id='project1')
    pa.session.add(project)
    pa.session.flush()
    pa.session.commit()
    pa.close_session()
    status = \
      check_and_register_cosmx_run(
        project_igf_id='project1',
        cosmx_run_igf_id='cosmx_run_1',
        db_session_class=self.base.get_session_class())
    self.assertTrue(status)


  def test_check_and_register_cosmx_slide(self):
    project_data = [{'project_igf_id':'project1'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    cosmx_platform = \
      Cosmx_platform(
        cosmx_platform_igf_id='cosmx_platform_1')
    pa.session.add(cosmx_platform)
    pa.session.flush()
    pa.session.commit()
    cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id='cosmx_run_1',
          project_id=1)
    pa.session.add(cosmx_run)
    pa.session.flush()
    pa.session.commit()
    pa.close_session()
    status = \
      check_and_register_cosmx_slide(
        cosmx_run_igf_id='cosmx_run_1',
        cosmx_slide_igf_id='cosmx_slide_1',
        panel_info='cosmx_panel_1',
        assay_type='assay1',
        version='1.0',
        cosmx_platform_igf_id='cosmx_platform_1',
        db_session_class=self.base.get_session_class(),
        slide_metadata=[{"a": "b"}])
    self.assertTrue(status)


  def test_create_or_update_cosmx_slide_fov(self):
    status = \
      create_or_update_cosmx_slide_fov(
        cosmx_slide_name='cosmx_slide_1',
        fov_range='1-100',
        slide_type='RNA')
    self.assertTrue(status)


  def test_create_or_update_cosmx_slide_fov_annotation(self):
    status = \
      create_or_update_cosmx_slide_fov_annotation(
        cosmx_slide_name='cosmx_slide_id',
        fov_range='1-100',
        tissue_annotation='annotation',
        tissue_ontology='ontology',
        species='HUMAN')
    self.assertTrue(status)


  def test_create_or_update_cosmx_slide_fov_count_qc(self):
    status = \
      create_or_update_cosmx_slide_fov_count_qc(
        cosmx_slide_name='cosmx_slide_id',
        slide_count_qc_csv='csv_file.csv')
    self.assertEqual(status, 'RNA')

if __name__ == '__main__':
  unittest.main()