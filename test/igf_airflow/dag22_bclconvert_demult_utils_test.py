import unittest, os
import pandas as pd
from igf_data.igfdb.igfTables import Base, Seqrun
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_load_seqrun_to_db
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_seed_seqrun_pipeline
from igf_airflow.utils.dag22_bclconvert_demult_utils import _get_formatted_samplesheets
from igf_airflow.utils.dag22_bclconvert_demult_utils import _calculate_bases_mask

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


class Dag22_bclconvert_demult_utils_testB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.input_dir = get_temp_dir()
    sa = \
      SampleSheet(\
        'doc/data/SampleSheet/NextSeq/SampleSheet.csv')
    sa_data = [{
      'Lane': '1', 'Sample_ID': 'sampleA', 'Sample_Name': 'sampleA', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'AAAAAA', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': ''}, {
      'Lane': '1', 'Sample_ID': 'sampleB', 'Sample_Name': 'sampleB', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'TTTTTTTT', 'I5_Index_ID': '', 'index2': 'TTTTTTTT', 'Sample_Project': 'projectA', 'Description': ''}, {
      'Lane': '1', 'Sample_ID': 'sampleC', 'Sample_Name': 'sampleC', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'SI-TT-A5', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': '10X'}, {
      'Lane': '1', 'Sample_ID': 'sampleD', 'Sample_Name': 'sampleD', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'SI-GA-A2', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': '10X'}, {
      'Lane': '1', 'Sample_ID': 'sampleE', 'Sample_Name': 'sampleE', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'CCCCCC', 'I5_Index_ID': '', 'index2': 'CCCCCC', 'Sample_Project': 'projectB', 'Description': ''}, {
      'Lane': '2', 'Sample_ID': 'sampleA', 'Sample_Name': 'sampleA', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'AAAAAA', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': ''}, {
      'Lane': '2', 'Sample_ID': 'sampleB', 'Sample_Name': 'sampleB', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'TTTTTTTT', 'I5_Index_ID': '', 'index2': 'TTTTTTTT', 'Sample_Project': 'projectA', 'Description': ''}, {
      'Lane': '2', 'Sample_ID': 'sampleC', 'Sample_Name': 'sampleC', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'SI-TT-A5', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': '10X'}, {
      'Lane': '2', 'Sample_ID': 'sampleD', 'Sample_Name': 'sampleD', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'SI-GA-A2', 'I5_Index_ID': '', 'index2': '', 'Sample_Project': 'projectA', 'Description': '10X'}, {
      'Lane': '2', 'Sample_ID': 'sampleE', 'Sample_Name': 'sampleE', 'Sample_Plate': '', 'Sample_Well': '', 'I7_Index_ID': '', 'index': 'CCCCCC', 'I5_Index_ID': '', 'index2': 'CCCCCC', 'Sample_Project': 'projectB', 'Description': ''}]
    df = pd.DataFrame(sa_data)
    sa._data = df.to_dict(orient='records')
    sa._data_header = df.columns.tolist()
    self.samplesheet_file = os.path.join(self.temp_dir, 'SampleSheet.csv')
    sa.print_sampleSheet(self.samplesheet_file)
    self.runinfo_xml_file = 'doc/data/Illumina/NextSeq2k/RunInfo.xml'


  def tearDown(self):
    remove_dir(self.temp_dir)
    remove_dir(self.input_dir)


  def test_get_formatted_samplesheets_and_bases_mask(self):
    formatted_samplesheets_list = \
      _get_formatted_samplesheets(
        samplesheet_file=self.samplesheet_file,
        samplesheet_output_dir=self.temp_dir,
        runinfo_xml_file=self.runinfo_xml_file,
        singlecell_barcode_json='data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json',
        singlecell_dual_barcode_json='data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json')
    self.assertEqual(len(formatted_samplesheets_list), 10)
    df = pd.DataFrame(formatted_samplesheets_list)
    self.assertTrue('projectA' in df['project'].values.tolist())
    self.assertTrue('projectB' in df['project'].values.tolist())
    projectA = df[df['project'] == 'projectA']
    self.assertTrue('1' in projectA['lane'].values.tolist())
    projectA_lane1 = projectA[projectA['lane'] == '1']
    self.assertEqual(len(projectA_lane1.index), 4)
    self.assertTrue('8_10X' in projectA_lane1['index_group'].values.tolist())
    self.assertTrue('20_NA' in projectA_lane1['index_group'].values.tolist())
    projectA_lane1_20_NA = projectA_lane1[projectA_lane1['index_group'] == '20_NA']
    sa = SampleSheet(projectA_lane1_20_NA['samplesheet_file'].values.tolist()[0])
    self.assertEqual(len(pd.DataFrame(sa._data).index), 1)
    self.assertEqual(pd.DataFrame(sa._data)['Sample_ID'].values.tolist()[0], 'sampleC')
    self.assertEqual(pd.DataFrame(sa._data)['index'].values.tolist()[0], 'GTAGCCCTGT')
    self.assertEqual(pd.DataFrame(sa._data)['index2'].values.tolist()[0], 'GAGCATCTAT')
    projectA_lane1_8_10X = projectA_lane1[projectA_lane1['index_group'] == '8_10X']
    sa = SampleSheet(projectA_lane1_8_10X['samplesheet_file'].values.tolist()[0])
    self.assertEqual(len(pd.DataFrame(sa._data).index), 4)
    self.assertTrue('sampleD_1' in pd.DataFrame(sa._data)['Sample_ID'].values.tolist())
    self.assertTrue('TTTCATGA' in pd.DataFrame(sa._data)['index'].values.tolist())

  def test_calculate_bases_mask(self):
    formatted_samplesheets_list = \
      _get_formatted_samplesheets(
        samplesheet_file=self.samplesheet_file,
        samplesheet_output_dir=self.temp_dir,
        runinfo_xml_file=self.runinfo_xml_file,
        singlecell_barcode_json='data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json',
        singlecell_dual_barcode_json='data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json')
    df = pd.DataFrame(formatted_samplesheets_list)
    projectA = \
      df[df['project'] == 'projectA']
    projectA_lane1 = \
      projectA[projectA['lane'] == '1']
    projectA_lane1_20_NA = \
      projectA_lane1[projectA_lane1['index_group'] == '20_NA']
    projectA_lane1_20_NA_samplesheet = \
      projectA_lane1_20_NA['samplesheet_file'].values.tolist()[0]
    bases_mask = \
      _calculate_bases_mask(
        samplesheet_file=projectA_lane1_20_NA_samplesheet,
        runinfoxml_file=self.runinfo_xml_file)
    self.assertEqual(bases_mask, 'Y29,I10,I10,Y91')
    projectA_lane1_8_10X = \
      projectA_lane1[projectA_lane1['index_group'] == '8_10X']
    projectA_lane1_8_10X_samplesheet = \
      projectA_lane1_8_10X['samplesheet_file'].values.tolist()[0]
    bases_mask = \
      _calculate_bases_mask(
        samplesheet_file=projectA_lane1_8_10X_samplesheet,
        runinfoxml_file=self.runinfo_xml_file)
    self.assertEqual(bases_mask, 'Y29,I8N2,N10,Y91')

if __name__=='__main__':
  unittest.main()