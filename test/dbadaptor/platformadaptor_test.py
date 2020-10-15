import os,unittest,sqlalchemy
from igf_data.igfdb.igfTables import Base,Platform,Flowcell_barcode_rule
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_dbconf_json

class Platformadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    self.platform_data=\
      [{"platform_igf_id" : "M03291" , 
        "model_name" : "MISEQ" ,
        "vendor_name" : "ILLUMINA" ,
        "software_name" : "RTA" ,
        "software_version" : "RTA1.18.54"
       },
       {"platform_igf_id" : "NB501820", 
        "model_name" : "NEXTSEQ",
        "vendor_name" : "ILLUMINA",
        "software_name" : "RTA",
        "software_version" : "RTA2"
       },
       {"platform_igf_id" : "K00345", 
        "model_name" : "HISEQ4000",
        "vendor_name" : "ILLUMINA",
        "software_name" : "RTA",
        "software_version" : "RTA2"
       }]
    self.flowcell_rule_data=\
      [{"platform_igf_id":"K00345",
        "flowcell_type":"HiSeq 3000/4000 SR",
        "index_1":"NO_CHANGE",
        "index_2":"NO_CHANGE"},
       {"platform_igf_id":"K00345",
        "flowcell_type":"HiSeq 3000/4000 PE",
        "index_1":"NO_CHANGE",
        "index_2":"REVCOMP"},
       {"platform_igf_id":"NB501820",
        "flowcell_type":"NEXTSEQ",
        "index_1":"NO_CHANGE",
        "index_2":"REVCOMP"},
       {"platform_igf_id":"M03291",
        "flowcell_type":"MISEQ",
        "index_1":"NO_CHANGE",
        "index_2":"NO_CHANGE"}]

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_store_platform_data1(self):
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    pl.store_platform_data(data=self.platform_data,
                           autosave=True)
    pl.close_session()
    pl.start_session()
    record=pl.fetch_platform_records_igf_id(\
                platform_igf_id='M03291',
                target_column_name='platform_igf_id')
    pl.close_session()
    self.assertEqual(record.platform_igf_id, 'M03291')

  def test_store_platform_data2(self):
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    pl.store_platform_data(data=self.platform_data,
                           autosave=False)
    pl.close_session()
    pl.start_session()
    record=pl.fetch_platform_records_igf_id(\
                platform_igf_id='M03291',
                target_column_name='platform_igf_id',
                output_mode='one_or_none')
    pl.close_session()
    self.assertEqual(record, None)

  def test_store_platform_data3(self):
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    with self.assertRaises(ValueError):#sqlalchemy.exc.IntegrityError):
      pl.store_platform_data(data=[{}])
    pl.close_session()

  def test_store_flowcell_barcode_rule1(self):
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    pl.store_platform_data(data=self.platform_data,
                           autosave=True)
    pl.store_flowcell_barcode_rule(data=self.flowcell_rule_data,
                                   autosave=True)
    pl.close_session()
    pl.start_session()
    query=pl.session.query(Flowcell_barcode_rule).\
          filter(Flowcell_barcode_rule.flowcell_type=='HiSeq 3000/4000 SR')
    record=pl.fetch_records(query=query,
                            output_mode='one_or_none')
    pl.close_session()
    self.assertEqual(record.flowcell_type, 'HiSeq 3000/4000 SR')

  def test_store_flowcell_barcode_rule2(self):
    pl=PlatformAdaptor(**{'session_class':self.session_class})
    pl.start_session()
    pl.store_platform_data(data=self.platform_data,
                           autosave=True)
    with self.assertRaises(ValueError):#sqlalchemy.exc.IntegrityError):
       pl.store_flowcell_barcode_rule(data=[{}])
    pl.close_session()


if __name__ == '__main__':
  unittest.main()