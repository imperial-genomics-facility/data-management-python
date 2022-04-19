import unittest, os
import pandas as pd
from igf_data.igfdb.igfTables import Base, Seqrun
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.utils.fileutils import calculate_file_checksum
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_load_seqrun_to_db
from igf_airflow.utils.dag22_bclconvert_demult_utils import _check_and_seed_seqrun_pipeline
from igf_airflow.utils.dag22_bclconvert_demult_utils import _get_formatted_samplesheets
from igf_airflow.utils.dag22_bclconvert_demult_utils import _calculate_bases_mask
from igf_airflow.utils.dag22_bclconvert_demult_utils import bclconvert_singularity_wrapper
from igf_airflow.utils.dag22_bclconvert_demult_utils import generate_bclconvert_report
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_jobs_per_worker
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_sample_groups_for_bcl_convert_output
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_sample_id_and_fastq_path_for_sample_groups
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_sample_info_from_sample_group
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_checksum_for_sample_group_fastq_files

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
    self.assertTrue('project_index' in df.columns.tolist())
    self.assertTrue('lane_index' in df.columns.tolist())
    self.assertTrue('index_group_index' in df.columns.tolist())
    self.assertEqual(df['project_index'].max(), 2)
    projectA = \
      df[df['project'] == 'projectA']
    self.assertEqual(projectA['lane_index'].max(), 2)
    projectA_lane1 = \
      projectA[projectA['lane'] == '1']
    self.assertEqual(projectA_lane1['index_group_index'].max(), 4)
    projectA_lane1_20_NA = \
      projectA_lane1[projectA_lane1['index_group'] == '20_NA']
    projectA_lane1_20_NA_samplesheet = \
      projectA_lane1_20_NA['samplesheet_file'].values.tolist()[0]
    bases_mask = \
      _calculate_bases_mask(
        samplesheet_file=projectA_lane1_20_NA_samplesheet,
        runinfoxml_file=self.runinfo_xml_file)
    self.assertEqual(bases_mask, 'Y29;I10;I10;Y91')
    projectA_lane1_8_10X = \
      projectA_lane1[projectA_lane1['index_group'] == '8_10X']
    projectA_lane1_8_10X_samplesheet = \
      projectA_lane1_8_10X['samplesheet_file'].values.tolist()[0]
    bases_mask = \
      _calculate_bases_mask(
        samplesheet_file=projectA_lane1_8_10X_samplesheet,
        runinfoxml_file=self.runinfo_xml_file)
    self.assertEqual(bases_mask, 'Y29;I8N2;N10;Y91')

class Dag22_bclconvert_demult_utils_testC(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.image_path = os.path.join(self.temp_dir, 'image.sif')
    with open(self.image_path, 'w') as fp:
      fp.write('A')
    self.input_dir = os.path.join(self.temp_dir, 'input')
    os.makedirs(self.input_dir)
    self.output_dir = os.path.join(self.temp_dir, 'output')
    os.makedirs(self.output_dir)
    self.samplesheet_file = os.path.join(self.temp_dir, 'samplesheet.csv')
    with open(self.samplesheet_file, 'w') as fp:
      fp.write('A')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_bclconvert_singularity_wrapper(self):
    cmd = \
      bclconvert_singularity_wrapper(
        image_path=self.image_path,
        input_dir=self.input_dir,
        output_dir=self.output_dir,
        samplesheet_file=self.samplesheet_file,
        lane_id=1,
        dry_run=True)
    self.assertTrue('bcl-convert' in cmd)
    self.assertTrue('--sample-sheet {0}'.format(self.samplesheet_file) in cmd)
    self.assertTrue('--output-directory {0}'.format(self.output_dir) in cmd)
    self.assertTrue('--bcl-only-lane {0}'.format(1) in cmd)
    self.assertTrue('--bcl-input-directory {0}'.format(self.input_dir) in cmd)
    cmd = \
      bclconvert_singularity_wrapper(
        image_path=self.image_path,
        input_dir=self.input_dir,
        output_dir=self.output_dir,
        samplesheet_file=self.samplesheet_file,
        lane_id=1,
        tile_id_list=('s_1_1102', 's_1_1103'),
        dry_run=True)
    self.assertTrue('--tiles s_1_1102,s_1_1103' in cmd)

class Dag22_bclconvert_demult_utils_testD(unittest.TestCase):
  def setUp(self):
    self.seqrun_dir = get_temp_dir()
    self.interop_dir = \
      os.path.join(self.seqrun_dir, 'InterOp')
    os.makedirs(self.interop_dir)
    self.runinfo_xml = \
      os.path.join(
        self.seqrun_dir,
        'RunInfo.xml')
    with open(self.runinfo_xml, 'w') as fp:
      fp.write('A')
    self.reports_dir = get_temp_dir()
    self.index_metrics_file = \
      os.path.join(
        self.reports_dir,
        'IndexMetricsOut.bin')
    with open(self.index_metrics_file, 'w') as fp:
      fp.write('A')
    self.image_path = \
      os.path.join(
        self.seqrun_dir,
        'image.sif')
    with open(self.image_path, 'w') as fp:
      fp.write('A')
    self.report_template = \
      os.path.join(
        self.seqrun_dir,
        'template.ipynb')
    with open(self.report_template, 'w') as fp:
      fp.write('A')
    self.bclconvert_report_library_path = \
      get_temp_dir()


  def tearDown(self):
    remove_dir(self.seqrun_dir)
    remove_dir(self.reports_dir)
    remove_dir(self.bclconvert_report_library_path)


  def test_generate_bclconvert_report(self):
    report_file = \
      generate_bclconvert_report(
        seqrun_path=self.seqrun_dir,
        image_path=self.image_path,
        report_template=self.report_template,
        bclconvert_report_library_path=self.bclconvert_report_library_path,
        bclconvert_reports_path=self.reports_dir,
        dry_run=True)
    self.assertTrue(os.path.exists(report_file))
    self.assertEqual(
      os.path.basename(report_file),
      os.path.basename(self.report_template))

  def test_get_jobs_per_worker(self):
    jobs_list = \
      get_jobs_per_worker(
        max_workers=2,
        total_jobs=2)
    self.assertEqual(len(jobs_list), 2)
    self.assertTrue('worker_index' in jobs_list[0])
    self.assertEqual(jobs_list[0]['worker_index'], 1)
    self.assertTrue('jobs' in jobs_list[0])
    self.assertEqual(len(jobs_list[0]['jobs']), 1)
    self.assertTrue('1' in jobs_list[0]['jobs'])
    jobs_list = \
      get_jobs_per_worker(
        max_workers=2,
        total_jobs=10)
    self.assertEqual(len(jobs_list), 2)
    self.assertTrue('worker_index' in jobs_list[0])
    self.assertEqual(jobs_list[0]['worker_index'], 1)
    self.assertTrue('jobs' in jobs_list[0])
    self.assertEqual(len(jobs_list[0]['jobs']), 5)
    self.assertTrue('9' in jobs_list[0]['jobs'])
    jobs_list = \
      get_jobs_per_worker(
        max_workers=3,
        total_jobs=10)
    self.assertEqual(len(jobs_list), 3)
    self.assertTrue('worker_index' in jobs_list[0])
    self.assertEqual(jobs_list[0]['worker_index'], 1)
    self.assertTrue('jobs' in jobs_list[0])
    self.assertEqual(len(jobs_list[0]['jobs']), 4)
    self.assertTrue('10' in jobs_list[0]['jobs'])
    self.assertEqual(len(jobs_list[1]['jobs']), 3)
    self.assertTrue('8' in jobs_list[1]['jobs'])
    self.assertEqual(len(jobs_list[2]['jobs']), 3)
    self.assertTrue('9' in jobs_list[2]['jobs'])
    with self.assertRaises(ValueError):
      get_jobs_per_worker(
        max_workers=0,
        total_jobs=0)
    with self.assertRaises(ValueError):
      get_jobs_per_worker(
        max_workers=0,
        total_jobs=1)
    with self.assertRaises(ValueError):
      get_jobs_per_worker(
        max_workers=1,
        total_jobs=0)

class Dag22_bclconvert_demult_utils_testE(unittest.TestCase):
  def setUp(self):
    self.bclconvert_output_path = get_temp_dir()
    os.makedirs(
      os.path.join(
        self.bclconvert_output_path,
        'Reports'))
    os.makedirs(
      os.path.join(
        self.bclconvert_output_path,
        'IGFQ0013_23-11-2021_10x'))
    self.samplesheet_file = \
      os.path.join(
        self.bclconvert_output_path,
        'Reports',
        'SampleSheet.csv')
    with open(self.samplesheet_file, 'w') as fp:
      fp.write('[Header]\n')
      fp.write('IEMFileVersion,4\n')
      fp.write('[Data]\n')
      fp.write('Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description\n')
      fp.write('IGF001,sample1,,,SI-TT-A1,GTAACATGCG,,AGTGTTACCT,IGFQ0013_23-11-2021_10x,,,,\n')
      fp.write('IGF002,sample2,,,SI-TT-A2,GTAACATGCG,,AGTGTTACCT,IGFQ0013_23-11-2021_10x,,,,\n')
      fp.write('IGF003,sample3,,,SI-TT-A3,GTAACATGCG,,AGTGTTACCT,IGFQ0013_23-11-2021_10x,,,,\n')
      fp.write('IGF004,sample4,,,SI-TT-A4,GTAACATGCG,,AGTGTTACCT,IGFQ0013_23-11-2021_10x,,,,\n')
    # sample1
    fastq_file_list = [
      'IGF001_S1_L001_R1_001.fastq.gz',
      'IGF001_S1_L001_R2_001.fastq.gz',
      'IGF001_S1_L001_I1_001.fastq.gz',
      'IGF001_S1_L001_I2_001.fastq.gz',
      'IGF002_S2_L001_R1_001.fastq.gz',
      'IGF002_S2_L001_R2_001.fastq.gz',
      'IGF002_S2_L001_I1_001.fastq.gz',
      'IGF002_S2_L001_I2_001.fastq.gz',
      'IGF003_S3_L001_R1_001.fastq.gz',
      'IGF003_S3_L001_R2_001.fastq.gz',
      'IGF003_S3_L001_I1_001.fastq.gz',
      'IGF003_S3_L001_I2_001.fastq.gz',
      'IGF004_S4_L001_R1_001.fastq.gz',
      'IGF004_S4_L001_R2_001.fastq.gz',
      'IGF004_S4_L001_I1_001.fastq.gz',
      'IGF004_S4_L001_I2_001.fastq.gz',
      'IGF001_S1_L002_R1_001.fastq.gz',
      'IGF001_S1_L002_R2_001.fastq.gz',
      'IGF001_S1_L002_I1_001.fastq.gz',
      'IGF001_S1_L002_I2_001.fastq.gz',
      'IGF002_S2_L002_R1_001.fastq.gz',
      'IGF002_S2_L002_R2_001.fastq.gz',
      'IGF002_S2_L002_I1_001.fastq.gz',
      'IGF002_S2_L002_I2_001.fastq.gz',
      'IGF003_S3_L002_R1_001.fastq.gz',
      'IGF003_S3_L002_R2_001.fastq.gz',
      'IGF003_S3_L002_I1_001.fastq.gz',
      'IGF003_S3_L002_I2_001.fastq.gz',
      'IGF004_S4_L002_R1_001.fastq.gz',
      'IGF004_S4_L002_R2_001.fastq.gz',
      'IGF004_S4_L002_I1_001.fastq.gz',
      'IGF004_S4_L002_I2_001.fastq.gz']
    for fastq_file in fastq_file_list:
      with open(
        os.path.join(
          self.bclconvert_output_path,
          'IGFQ0013_23-11-2021_10x',
          fastq_file),
        'w') as fp:
        fp.write('A')

  def tearDown(self):
    remove_dir(self.bclconvert_output_path)

  def test_get_sample_groups_for_bcl_convert_output(self):
    sample_group_list = \
      get_sample_groups_for_bcl_convert_output(
        samplesheet_file=self.samplesheet_file,
        max_samples=2)
    self.assertEqual(len(sample_group_list), 2)
    self.assertTrue('worker_index' in sample_group_list[0])
    self.assertEqual(sample_group_list[0]['worker_index'], 1)
    self.assertTrue('sample_ids' in sample_group_list[0])
    self.assertEqual(len(sample_group_list[0]['sample_ids']), 2)
    self.assertTrue('IGF001' in sample_group_list[0]['sample_ids'])
    self.assertEqual(len(sample_group_list[1]['sample_ids']), 2)
    self.assertTrue('IGF004' in sample_group_list[1]['sample_ids'])

  def test_get_sample_id_and_fastq_path_for_sample_groups(self):
    formatted_sample_groups = \
      get_sample_id_and_fastq_path_for_sample_groups(
        samplesheet_file=self.samplesheet_file,
        lane_id=2,
        bclconv_output_path=self.bclconvert_output_path,
        sample_group=[{
          'worker_index': '1',
          'sample_ids': ['IGF001', 'IGF002']}, {
          'worker_index': '2',
          'sample_ids': ['IGF003', 'IGF004']}]
    )
    self.assertEqual(len(formatted_sample_groups), 2)
    self.assertTrue('worker_index' in formatted_sample_groups[0])
    self.assertEqual(int(formatted_sample_groups[0]['worker_index']), 1)
    self.assertTrue('sample_ids' in formatted_sample_groups[0])
    self.assertEqual(len(formatted_sample_groups[0]['sample_ids']), 2)
    df = pd.DataFrame(formatted_sample_groups[0]['sample_ids'])
    df = df[df['sample_id']=='IGF001']
    self.assertEqual(len(df), 1)
    sample1_lane2_fastqs = df['fastq_list'].values.tolist()
    self.assertEqual(len(sample1_lane2_fastqs[0]), 4)
    self.assertTrue(
      os.path.join(
        self.bclconvert_output_path,
        'IGFQ0013_23-11-2021_10x',
        'IGF001_S1_L002_R1_001.fastq.gz') in sample1_lane2_fastqs[0])

  def test_get_sample_info_from_sample_group(self):
    sample_group_list = \
      get_sample_groups_for_bcl_convert_output(
        samplesheet_file=self.samplesheet_file,
        max_samples=2)
    formatted_sample_groups = \
      get_sample_id_and_fastq_path_for_sample_groups(
        samplesheet_file=self.samplesheet_file,
        lane_id=2,
        bclconv_output_path=self.bclconvert_output_path,
        sample_group=sample_group_list)
    fastq_files_list = \
      get_sample_info_from_sample_group(
        worker_index=1,
        sample_group=formatted_sample_groups)
    self.assertEqual(len(fastq_files_list), 2)
    self.assertTrue('sample_id' in fastq_files_list[0])
    self.assertTrue('fastq_list' in fastq_files_list[0])
    self.assertEqual(len(fastq_files_list[0]['fastq_list']), 4)
    self.assertTrue(
      os.path.join(
        self.bclconvert_output_path,
        'IGFQ0013_23-11-2021_10x',
        'IGF001_S1_L002_R1_001.fastq.gz') in fastq_files_list[0]['fastq_list']
    )

  def test_get_checksum_for_sample_group_fastq_files(self):
    sample_group_list = \
      get_sample_groups_for_bcl_convert_output(
        samplesheet_file=self.samplesheet_file,
        max_samples=2)
    formatted_sample_groups = \
      get_sample_id_and_fastq_path_for_sample_groups(
        samplesheet_file=self.samplesheet_file,
        lane_id=2,
        bclconv_output_path=self.bclconvert_output_path,
        sample_group=sample_group_list)
    fastq_files_list = \
      get_sample_info_from_sample_group(
        worker_index=1,
        sample_group=formatted_sample_groups)
    check_sum_sample_group = \
      get_checksum_for_sample_group_fastq_files(
        sample_group=fastq_files_list)
    self.assertEqual(len(check_sum_sample_group), 2)
    self.assertEqual('IGF001', check_sum_sample_group[0]['sample_id'])
    self.assertTrue(
      os.path.join(
        self.bclconvert_output_path,
        'IGFQ0013_23-11-2021_10x',
        'IGF001_S1_L002_R1_001.fastq.gz') in check_sum_sample_group[0]['fastq_list']
    )
    sample1_r1_checksum = \
      calculate_file_checksum(
        os.path.join(
          self.bclconvert_output_path,
          'IGFQ0013_23-11-2021_10x',
          'IGF001_S1_L002_R1_001.fastq.gz'))
    sample1_r1_calculated_checksum = \
      check_sum_sample_group[0]['fastq_list'][
        os.path.join(
          self.bclconvert_output_path,
          'IGFQ0013_23-11-2021_10x',
          'IGF001_S1_L002_R1_001.fastq.gz')]
    self.assertEqual(sample1_r1_checksum, sample1_r1_calculated_checksum)

if __name__=='__main__':
  unittest.main()