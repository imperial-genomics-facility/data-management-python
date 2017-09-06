import os, unittest
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json

class Pipelineadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    self.platform_json='data/platform_db_data.json'
    self.seqrun_json='data/seqrun_db_data.json'
    self.pipeline_json='data/pipeline_data.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    # load platform data
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=read_json_data(self.platform_json))
    # load seqrun data
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=read_json_data(self.seqrun_json))
    # load platform data
    pla=PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=read_json_data(self.pipeline_json))
    base.close_session()

  
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)


  def test_fetch_pipeline_records_pipeline_name(self):
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    pl_data=pl.fetch_pipeline_records_pipeline_name(pipeline_name='demultiplexing_fastq')
    self.assertEqual(pl_data.pipeline_id, 1)

if __name__ == '__main__':
  unittest.main()
