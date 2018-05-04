import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Project, Project_attribute
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.projectutils import mark_project_barcode_check_off
from igf_data.utils.dbutils import read_dbconf_json

class Projectutils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    data=[{'project_igf_id': 'IGFP001_test1_24-1-18',},
          {'project_igf_id': 'IGFP002_test1_24-1-18',
           'barcode_check':'ON'},
          {'project_igf_id': 'IGFP003_test1_24-1-18',
           'barcode_check':'OFF'}
         ]
    self.data=pd.DataFrame(data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_mark_project_barcode_check_off(self):
    pr=ProjectAdaptor(**{'session_class':self.session_class})
    pr.start_session()
    pr.store_project_and_attribute_data(self.data)
    pr.close_session()
    mark_project_barcode_check_off(project_igf_id='IGFP001_test1_24-1-18',
                                   session_class=self.session_class)
    pr.start_session()
    attribute_check=pr.check_project_attributes(project_igf_id='IGFP001_test1_24-1-18',
                                                attribute_name='barcode_check')
    self.assertTrue(attribute_check)
    pr_attributes=pr.get_project_attributes(project_igf_id='IGFP001_test1_24-1-18',
                                                  attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'OFF')
    
    pr_attributes=pr.get_project_attributes(project_igf_id='IGFP002_test1_24-1-18',
                                                  attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'ON')
    pr.close_session()
    mark_project_barcode_check_off(project_igf_id='IGFP002_test1_24-1-18',
                                   session_class=self.session_class)
    pr.start_session()
    pr_attributes=pr.get_project_attributes(project_igf_id='IGFP002_test1_24-1-18',
                                                  attribute_name='barcode_check')
    for pr_attribute in pr_attributes.to_dict(orient='records'):
      self.assertEqual(pr_attribute['attribute_value'],'ON')
    pr.close_session()