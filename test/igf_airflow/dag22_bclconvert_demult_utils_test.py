import unittest, os
from igf_data.igfdb.igfTables import Base, Seqrun
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_load_seqrun_to_db
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_seed_seqrun_pipeline

class Dag22_bclconvert_demult_utils_testA(unittest.TestCase):
  def setUp(self):
    self.seqrun_path = 'doc/data/Illumina/NextSeq2k'
    self.seqrun_id = 'NextSeq2k'
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    platform_data = [{
      "platform_igf_id":"VH004" ,
      "model_name":"NEXTSEQ" ,
      "vendor_name":"ILLUMINA" ,
      "software_name":"RTA"}]
    flowcell_rule_data = [{
      "platform_igf_id":"VH004",
      "flowcell_type":"NEXTSEQ",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE"}]
    pl=PlatformAdaptor(**{'session_class':base.session_class})
    pl.start_session()
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    pl.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_check_and_load_seqrun_to_db(self):
    base = BaseAdaptor(**read_dbconf_json(self.dbconfig))
    base.start_session()
    seqrun_entry = \
      base.fetch_records(
        query=base.session.query(Seqrun).filter_by(seqrun_igf_id=self.seqrun_id),
        output_mode='one_or_none')
    self.assertTrue(seqrun_entry is None)
    base.close_session()
    _check_and_load_seqrun_to_db(
      seqrun_path=self.seqrun_path,
      seqrun_id=self.seqrun_id,
      dbconf_json_path=self.dbconfig)
    base.start_session()
    seqrun_entry = \
      base.fetch_records(
        query=base.session.query(Seqrun).filter_by(seqrun_igf_id=self.seqrun_id),
        output_mode='one_or_none')
    self.assertTrue(seqrun_entry is not None)
    self.assertEqual(seqrun_entry.seqrun_igf_id, self.seqrun_id)
    base.close_session()

  def test_check_and_seed_seqrun_pipeline(self):
    _check_and_load_seqrun_to_db(
      seqrun_path=self.seqrun_path,
      seqrun_id=self.seqrun_id,
      dbconf_json_path=self.dbconfig)
    base = BaseAdaptor(**read_dbconf_json(self.dbconfig))
    pla = PipelineAdaptor(**{'session_class':base.session_class})
    pla.start_session()
    pipeline_data = [{ 
      "pipeline_name" : "demultiplexing_fastq",
      "pipeline_db" : "sqlite:////data/bcl2fastq.db", 
      "pipeline_init_conf" : { "input_dir":"data/seqrun_dir/" , "output_dir" : "data"}, 
      "pipeline_run_conf" : { "output_dir" : "data" }}]
    pla.store_pipeline_data(data=pipeline_data)
    pla.close_session()
    _check_and_seed_seqrun_pipeline(
      seqrun_id=self.seqrun_id,
      pipeline_name='demultiplexing_fastq',
      dbconf_json_path=self.dbconfig)
    base.start_session()
    pla = PipelineAdaptor(**{'session':base.session})
    (pipeseed_data, table_data) = \
      pla.fetch_pipeline_seed_with_table_data(
          pipeline_name='demultiplexing_fastq')
    self.assertEqual(len(pipeseed_data.index), 1)
    self.assertEqual(len(table_data.index), 1)
    self.assertTrue(pipeseed_data.to_dict(orient='records')[0]['seed_table'] == 'seqrun')
    self.assertTrue(table_data.to_dict(orient='records')[0]['seqrun_igf_id'] == self.seqrun_id)
    base.close_session()


if __name__=='__main__':
  unittest.main()