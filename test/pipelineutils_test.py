import os, unittest, json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base, Pipeline
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json
from igf_data.utils.pipelineutils import load_new_pipeline_data

class Pipelineutils_test1(unittest.TestCase):
  def setUp(self):
    self.data_file = 'data/pipeline_data.json'
    self.dbconfig = 'data/dbconfig.json'

    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)


  def test_load_new_pipeline_data(self): 
    load_new_pipeline_data(data_file=self.data_file, dbconfig=self.dbconfig)
    pp=PipelineAdaptor(**{'session_class': self.session_class})
    pp.start_session()
    data=pp.fetch_pipeline_records_pipeline_name(pipeline_name='demultiplexing_fastq')
    pp.close_session()
    self.assertEqual(data.pipeline_name,'demultiplexing_fastq')

if __name__ == '__main__':
  unittest.main()
