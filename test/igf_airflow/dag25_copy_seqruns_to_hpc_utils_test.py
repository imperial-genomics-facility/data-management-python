import os
import re
import json
import unittest
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import remove_dir
from igf_airflow.utils.dag25_copy_seqruns_to_hpc_utils import register_new_seqrun_to_db
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.igfTables import Base, Seqrun

class TestDag25_copy_seqruns_to_hpc_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.seqrun_id = 'TEST1'
    self.seqrun_path = \
      os.path.join(
        self.temp_dir,
        self.seqrun_id)
    os.makedirs(
      self.seqrun_path,
      exist_ok=False)
    check_file_path(self.seqrun_path)
    ## create RunInfo.xml file
    xml_data = """
    <?xml version="1.0"?>
    <RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="3">
      <Run Id="TEST1" Number="7">
      <Flowcell>HXXXXXXXX</Flowcell>
      <Instrument>M00001</Instrument>
      <Date>170228</Date>
      <Reads>
        <Read Number="1" NumCycles="151" IsIndexedRead="N" />
        <Read Number="2" NumCycles="8" IsIndexedRead="Y" />
        <Read Number="3" NumCycles="8" IsIndexedRead="Y" />
        <Read Number="4" NumCycles="151" IsIndexedRead="N" />
      </Reads>
      <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="2" TileCount="28">
      </Run>
    </RunInfo>
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    xml_data = re.sub(pattern1, '\n', xml_data)
    xml_data = re.sub(pattern2, '', xml_data)
    run_info_xml = \
      os.path.join(
        self.seqrun_path,
        'RunInfo.xml')
    with open(run_info_xml, 'w') as fh:
        fh.write(xml_data)
    check_file_path(run_info_xml)
    ## add platform to db
    self.dbconfig = 'data/dbconfig.json'
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
      "platform_igf_id": "M00001" ,
      "model_name": "MISEQ" ,
      "vendor_name": "ILLUMINA" ,
      "software_name": "RTA" ,
      "software_version": "RTA1.18.54"
    },{
      "platform_igf_id": "H00001" ,
      "model_name": "HISEQ4000" ,
      "vendor_name": "ILLUMINA" ,
      "software_name": "RTA" ,
      "software_version": "RTA1.18.54"
    }]
    flowcell_rule_data = [{
      "platform_igf_id": "M00001",
      "flowcell_type": "MISEQ",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE"
    },{
      "platform_igf_id": "H00001",
      "flowcell_type": "Hiseq 3000/4000 PE",
      "index_1": "NO_CHANGE",
      "index_2": "REVCOMP"
    }]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
      'flowcell_id':'000000000-D0YLK',
      'platform_igf_id':'M00001',
      'flowcell':'MISEQ',
    }]
    sra = SeqrunAdaptor(**{'session': base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)

  def test_register_new_seqrun_to_db(self):
    status = \
      register_new_seqrun_to_db(
        dbconfig_file=self.dbconfig,
        seqrun_id='TEST1',
        seqrun_base_path=self.temp_dir)
    self.assertTrue(status)

class TestDag25_copy_seqruns_to_hpc_utilsB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.seqrun_id = 'TEST1'
    self.seqrun_path = \
      os.path.join(
        self.temp_dir,
        self.seqrun_id)
    os.makedirs(
      self.seqrun_path,
      exist_ok=False)
    check_file_path(self.seqrun_path)
    ## create RunInfo.xml file
    xml_data = """
    <?xml version="1.0"?>
    <RunInfo xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" Version="3">
      <Run Id="TEST1" Number="7">
      <Flowcell>HXXXXXXXX</Flowcell>
      <Instrument>M00001</Instrument>
      <Date>170228</Date>
      <Reads>
        <Read Number="1" NumCycles="151" IsIndexedRead="N" />
        <Read Number="2" NumCycles="8" IsIndexedRead="Y" />
        <Read Number="3" NumCycles="8" IsIndexedRead="Y" />
        <Read Number="4" NumCycles="151" IsIndexedRead="N" />
      </Reads>
      <FlowcellLayout LaneCount="8" SurfaceCount="2" SwathCount="2" TileCount="28">
      </Run>
    </RunInfo>
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    xml_data = re.sub(pattern1, '\n', xml_data)
    xml_data = re.sub(pattern2, '', xml_data)
    run_info_xml = \
      os.path.join(
        self.seqrun_path,
        'RunInfo.xml')
    with open(run_info_xml, 'w') as fh:
        fh.write(xml_data)
    check_file_path(run_info_xml)
    ## add platform to db
    self.dbconfig = 'data/dbconfig.json'
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
      "platform_igf_id": "M00001" ,
      "model_name": "MISEQ" ,
      "vendor_name": "ILLUMINA" ,
      "software_name": "RTA" ,
      "software_version": "RTA1.18.54"
    },{
      "platform_igf_id": "H00001" ,
      "model_name": "HISEQ4000" ,
      "vendor_name": "ILLUMINA" ,
      "software_name": "RTA" ,
      "software_version": "RTA1.18.54"
    }]
    flowcell_rule_data = [{
      "platform_igf_id": "M00001",
      "flowcell_type": "MISEQ",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE"
    },{
      "platform_igf_id": "H00001",
      "flowcell_type": "Hiseq 3000/4000 PE",
      "index_1": "NO_CHANGE",
      "index_2": "REVCOMP"
    }]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id':'TEST1',
      'flowcell_id':'000000000-D0YLK',
      'platform_igf_id':'M00001',
      'flowcell':'MISEQ',
    }]
    sra = SeqrunAdaptor(**{'session': base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)

  def test_register_new_seqrun_to_db(self):
    status = \
      register_new_seqrun_to_db(
        dbconfig_file=self.dbconfig,
        seqrun_id='TEST1',
        seqrun_base_path=self.temp_dir)
    self.assertFalse(status)


if __name__=='__main__':
  unittest.main()