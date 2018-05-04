import os, unittest
import pandas as pd
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Project, Project_attribute
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.projectutils import mark_project_barcode_check_off

class Projectutils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    project_data=[{'project_igf_id': 'IGFP001_test1_24-1-18',},
                  {'project_igf_id': 'IGFP002_test1_24-1-18',
                   'barcode_check':'ON'},
                  {'project_igf_id': 'IGFP002_test1_24-1-18',
                   'barcode_check':'OFF'}
                  ]