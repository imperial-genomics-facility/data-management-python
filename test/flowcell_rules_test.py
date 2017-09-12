import os, unittest
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json

class Flowcell_barcode_rule_test1(unittest.TestCase):
  def setUp(self):
      self.dbconfig='data/dbconfig.json'
      self.platform_json='data/platform_db_data.json'
      self.seqrun_json='data/seqrun_db_data.json'
      self.pipeline_json='data/pipeline_data.json'
      self.flowcell_rules_json='data/flowcell_rules.json'
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
      pl.store_flowcell_barcode_rule(data=read_json_data(self.flowcell_rules_json))
      # load seqrun data
      sra=SeqrunAdaptor(**{'session':base.session})
      sra.store_seqrun_and_attribute_data(data=read_json_data(self.seqrun_json))
      base.close_session()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    
    
  def test_fetch_flowcell_barcode_rules_for_seqrun(self):
    sra=SeqrunAdaptor(**{'session_class': self.session_class})
    sra.start_session()
    sra_data=sra.fetch_flowcell_barcode_rules_for_seqrun(seqrun_igf_id='170101_K00001_0001_AHABCDEFGH')
    sra.close_session()
    sra_data=sra_data.to_dict(orient='records')[0]
    self.assertEqual(sra_data['index_2'],'REVCOMP')

if __name__ == '__main__':
  unittest.main()
