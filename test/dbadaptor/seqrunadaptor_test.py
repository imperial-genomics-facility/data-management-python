import unittest,json,os,shutil
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor

class SeqrunAdaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    base.start_session()
    platform_data = [{
      "platform_igf_id":"M00001" ,
      "model_name":"MISEQ" ,
      "vendor_name":"ILLUMINA" ,
      "software_name":"RTA" ,
      "software_version":"RTA1.18.54"
    },{
      "platform_igf_id":"H00001" ,
      "model_name":"HISEQ4000" ,
      "vendor_name":"ILLUMINA" ,
      "software_name":"RTA" ,
      "software_version":"RTA1.18.54"
    }]
    flowcell_rule_data = [{
      "platform_igf_id":"M00001",
      "flowcell_type":"MISEQ",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE"
    },{
      "platform_igf_id":"H00001",
      "flowcell_type":"Hiseq 3000/4000 PE",
      "index_1":"NO_CHANGE",
      "index_2":"REVCOMP"
    }]
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
      'flowcell_id':'000000000-D0YLK',
      'platform_igf_id':'M00001',
      'flowcell':'MISEQ',
    },{
      'seqrun_igf_id':'171003_H00001_0089_TEST',
      'flowcell_id':'TEST',
      'platform_igf_id':'H00001',
      'flowcell':'HISEQ 3000/4000 PE',
    }]
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_platform_info_for_seqrun(self):
    sr = SeqrunAdaptor(**{'session_class':self.session_class})
    sr.start_session()
    pl1 = sr.fetch_platform_info_for_seqrun('171003_H00001_0089_TEST')
    self.assertTrue(pl1,'H00001')
    pl2 = sr.fetch_platform_info_for_seqrun('171003_M00001_0089_000000000-TEST')
    self.assertTrue(pl2,'M00001')
    pl3 = sr.fetch_platform_info_for_seqrun('171003_M00001_0089_000000000')
    self.assertEqual(pl3,None)

if __name__=='__main__':
  unittest.main()