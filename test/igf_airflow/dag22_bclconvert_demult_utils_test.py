import unittest
import os
import json
import re
import subprocess
import pandas as pd
from pyrsistent import s
from igf_data.igfdb.igfTables import Base, Seqrun, Seqrun_attribute
from igf_data.igfdb.igfTables import Collection, Collection_group, File
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.useradaptor import UserAdaptor
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import check_file_path, get_temp_dir, remove_dir
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.illumina.samplesheet import SampleSheet
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
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_platform_name_and_flowcell_id_for_seqrun
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_project_id_samples_list_from_db
from igf_airflow.utils.dag22_bclconvert_demult_utils import register_experiment_and_runs_to_db
from igf_airflow.utils.dag22_bclconvert_demult_utils import load_data_raw_data_collection
from igf_airflow.utils.dag22_bclconvert_demult_utils import copy_or_replace_file_to_disk_and_change_permission
from igf_airflow.utils.dag22_bclconvert_demult_utils import _get_project_user_list
from igf_airflow.utils.dag22_bclconvert_demult_utils import _configure_qc_pages_for_ftp
from igf_airflow.utils.dag22_bclconvert_demult_utils import _get_project_sample_count
from igf_airflow.utils.dag22_bclconvert_demult_utils import _calculate_image_height_for_project_page
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from igf_airflow.utils.dag22_bclconvert_demult_utils import reset_single_cell_samplesheet
from igf_airflow.utils.dag22_bclconvert_demult_utils import check_demult_stats_file_for_failed_samples
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_data_for_sample_qc_page
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_run_id_for_samples_flowcell_and_lane
from igf_airflow.utils.dag22_bclconvert_demult_utils import get_files_for_collection_ids
from igf_airflow.utils.dag22_bclconvert_demult_utils import _build_run_qc_page
from igf_airflow.utils.dag22_bclconvert_demult_utils import copy_collection_file_to_globus_for_ig
from igf_airflow.utils.dag22_bclconvert_demult_utils import copy_fastqs_for_sample_to_globus_dir
from igf_airflow.utils.dag22_bclconvert_demult_utils import generate_email_body

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
    query = \
      base.session.\
        query(Seqrun_attribute.attribute_value).\
        join(Seqrun, Seqrun.seqrun_id==Seqrun_attribute.seqrun_id).\
        filter(Seqrun.seqrun_igf_id==self.seqrun_id).\
        filter(Seqrun_attribute.attribute_name=='flowcell')
    seqrun_attribute_entry = \
      base.fetch_records(
        query=query,
        output_mode='one_or_none')
    base.close_session()
    self.assertTrue(seqrun_attribute_entry is not None)
    self.assertEqual(seqrun_attribute_entry[0], 'NEXTSEQ')

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
      "pipeline_run_conf" : { "output_dir" : "data" }},{
      "pipeline_name" : "demultiplexing_fastq2",
      "pipeline_db" : "sqlite:////data/bcl2fastq2.db", 
      "pipeline_init_conf" : { "input_dir":"data/seqrun_dir/" , "output_dir" : "data"}, 
      "pipeline_run_conf" : { "output_dir" : "data" }}]
    pla.store_pipeline_data(data=pipeline_data)
    pla.close_session()
    seed_status = \
      _check_and_seed_seqrun_pipeline(
        seqrun_id=self.seqrun_id,
        pipeline_name='demultiplexing_fastq',
        dbconf_json_path=self.dbconfig)
    self.assertTrue(seed_status)
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
    seed_status = \
      _check_and_seed_seqrun_pipeline(
        seqrun_id=self.seqrun_id,
        pipeline_name='demultiplexing_fastq2',
        dbconf_json_path=self.dbconfig,
        check_all_pipelines_for_seed_id=True)
    self.assertFalse(seed_status)


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


  def test_get_formatted_samplesheets_and_bases_mask1(self):
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
    self.assertEqual(projectA_lane1_20_NA['sample_counts'].values.tolist()[0], 1)
    projectA_lane1_8_10X = projectA_lane1[projectA_lane1['index_group'] == '8_10X']
    sa = SampleSheet(projectA_lane1_8_10X['samplesheet_file'].values.tolist()[0])
    self.assertEqual(len(pd.DataFrame(sa._data).index), 4)
    self.assertTrue('sampleD_1' in pd.DataFrame(sa._data)['Sample_ID'].values.tolist())
    self.assertTrue('TTTCATGA' in pd.DataFrame(sa._data)['index'].values.tolist())
    self.assertEqual(projectA_lane1_8_10X['sample_counts'].values.tolist()[0], 1)


  def test_get_formatted_samplesheets_and_bases_mask2(self):
    formatted_samplesheets_list = \
      _get_formatted_samplesheets(
        samplesheet_file=self.samplesheet_file,
        samplesheet_output_dir=self.temp_dir,
        runinfo_xml_file=self.runinfo_xml_file,
        singlecell_barcode_json='data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json',
        singlecell_dual_barcode_json='data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json',
        override_cycles='Y29;I10;U10;Y91')
    df = pd.DataFrame(formatted_samplesheets_list)
    projectA = df[df['project'] == 'projectA']
    projectA_lane1 = projectA[projectA['lane'] == '1']
    projectA_lane1_20_NA = projectA_lane1[projectA_lane1['index_group'] == '20_NA']
    self.assertEqual(projectA_lane1_20_NA['bases_mask'].values.tolist()[0], 'Y29;I10;U10;Y91')


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
    projectA_lane1_16_NA = \
      projectA_lane1[projectA_lane1['index_group'] == '16_NA']
    projectA_lane1_16_NA_samplesheet = \
      projectA_lane1_16_NA['samplesheet_file'].values.tolist()[0]
    bases_mask = \
      _calculate_bases_mask(
        samplesheet_file=projectA_lane1_16_NA_samplesheet,
        runinfoxml_file=self.runinfo_xml_file)
    self.assertEqual(bases_mask, 'Y29;I8N2;N2I8;Y91')

class Dag22_bclconvert_demult_utils_testC(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.image_path = os.path.join(self.temp_dir, 'image.sif')
    with open(self.image_path, 'w') as fp:
      fp.write('A')
    self.input_dir = os.path.join(self.temp_dir, 'input')
    os.makedirs(self.input_dir)
    self.output_dir = os.path.join(self.temp_dir, 'output')
    #os.makedirs(self.output_dir)
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
        'IGF001_S1_L002_R1_001.fastq.gz') in check_sum_sample_group[0]['fastq_list'])
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


class Dag22_bclconvert_demult_utils_testF(unittest.TestCase):
  def setUp(self):
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
    },{
      'seqrun_igf_id': '171003_H00001_0089_TEST',
      'flowcell_id': 'TEST',
      'platform_igf_id': 'H00001',
      'flowcell': 'HISEQ 3000/4000 PE',
    }]
    sra = SeqrunAdaptor(**{'session': base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_get_flatform_name_and_flowcell_id_for_seqrun(self):
    (platform_name, flowcell_id) = \
      get_platform_name_and_flowcell_id_for_seqrun(
        seqrun_igf_id='171003_H00001_0089_TEST',
        db_config_file=self.dbconfig)
    self.assertEqual(platform_name, 'HISEQ4000')
    self.assertEqual(flowcell_id, 'TEST')


class Dag22_bclconvert_demult_utils_testG(unittest.TestCase):
  def setUp(self):
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
      "platform_igf_id": "H00001" ,
      "model_name": "HISEQ4000" ,
      "vendor_name": "ILLUMINA" ,
      "software_name": "RTA" ,
      "software_version": "RTA1.18.54"
    }]
    flowcell_rule_data = [{
      "platform_igf_id": "H00001",
      "flowcell_type": "Hiseq 3000/4000 PE",
      "index_1": "NO_CHANGE",
      "index_2": "REVCOMP"
    }]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id': '171003_H00001_0089_TEST',
      'flowcell_id': 'TEST',
      'platform_igf_id': 'H00001',
      'flowcell': 'HISEQ 3000/4000 PE',
    }]
    sra = SeqrunAdaptor(**{'session': base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{
      'project_igf_id':'IGFQ0013_23-11-2021_10x' }]
    sample_data = [{
      'sample_igf_id': 'IGF001', 'project_igf_id':'IGFQ0013_23-11-2021_10x' }, {
      'sample_igf_id': 'IGF002', 'project_igf_id':'IGFQ0013_23-11-2021_10x' }, {
      'sample_igf_id': 'IGF003', 'project_igf_id':'IGFQ0013_23-11-2021_10x' }, {
      'sample_igf_id': 'IGF004', 'project_igf_id':'IGFQ0013_23-11-2021_10x' }]
    pa = ProjectAdaptor(**{'session': base.session})
    sa = SampleAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()
    self.temp_dir = get_temp_dir()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)

  def test_get_project_id_samples_list_from_db(self):
    project_sample_dict = \
      get_project_id_samples_list_from_db(
        sample_igf_id_list=['IGF001', 'IGF002', 'IGF003', 'IGF004'],
        db_config_file=self.dbconfig)
    self.assertTrue('IGF001' in project_sample_dict)
    self.assertEqual(project_sample_dict['IGF001'], 'IGFQ0013_23-11-2021_10x')

  def test_register_experiment_and_runs_to_db(self):
    for f in ('IGF001_S1_R1_001.fastq', 'IGF001_S1_R2_001.fastq'):
      file_path = \
        os.path.join(self.temp_dir, f)
      with open(file_path, 'w') as fh:
        fh.write('@AAAA\n')
        fh.write('AAAA\n')
        fh.write('+\n')
        fh.write('AAAA\n')
      subprocess.check_call(f"gzip {file_path}", shell=True)
    sample_group_with_run_id = \
      register_experiment_and_runs_to_db(
        db_config_file=self.dbconfig,
        seqrun_id='171003_H00001_0089_TEST',
        lane_id=2,
        index_group='20_10x',
        sample_group=[{
          'sample_id': 'IGF001' ,
          'fastq_list': {
            os.path.join(self.temp_dir, 'IGF001_S1_R1_001.fastq.gz'): '00000',
            os.path.join(self.temp_dir, 'IGF001_S1_R2_001.fastq.gz'): '00001'
          }
        }]
      )
    self.assertEqual(len(sample_group_with_run_id), 1)
    self.assertTrue('collection_name' in sample_group_with_run_id[0])
    self.assertTrue('dir_list' in sample_group_with_run_id[0])
    self.assertTrue('file_list' in sample_group_with_run_id[0])
    self.assertTrue('IGF001' in sample_group_with_run_id[0].get('dir_list'))
    self.assertEqual(len(sample_group_with_run_id[0].get('file_list')), 2)
    #self.assertTrue(
    #  '/path/IGF001_S1_R2_001.fastq.gz' in \
    #  pd.DataFrame(sample_group_with_run_id[0].get('file_list'))['file_name'].values.tolist())


class Dag22_bclconvert_demult_utils_testH(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    self.temp_dir = get_temp_dir()
    base.start_session()
    ca = CollectionAdaptor(**{'session': base.session})
    file_pathA = os.path.join(self.temp_dir, 'a.csv')
    with open(file_pathA, 'w') as f:
      f.write('a,b,c\n')
    file_pathB = os.path.join(self.temp_dir, 'b.csv')
    with open(file_pathB, 'w') as f:
      f.write('a,b,c\n')
    collection_list = [{
      'name': 'a',
      'type': 'csv',
      'table': 'file',
      'file_path': file_pathA
    },
    {
      'name': 'b',
      'type': 'csv',
      'table': 'file',
      'file_path': file_pathB
    }]
    ca.load_file_and_create_collection(
      data=collection_list,
      calculate_file_size_and_md5=True,
      autosave=True)
    base.close_session()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)


  def test_load_data_raw_data_collection(self):
    file_pathA = os.path.join(self.temp_dir, 'a.csv')
    with open(file_pathA, 'w') as f:
      f.write('a,b,c,d\n')
    file_pathB = os.path.join(self.temp_dir, 'b1.csv')
    with open(file_pathB, 'w') as f:
      f.write('a,b,c\n')
    collection_list = [{
      'collection_name': 'a',
      'collection_type': 'csv',
      'collection_table': 'file',
      'file_path': file_pathA
    },
    {
      'collection_name': 'b',
      'collection_type': 'csv',
      'collection_table': 'file',
      'file_path': file_pathB
    }]
    with self.assertRaises(ValueError):
      load_data_raw_data_collection(
        collection_list=collection_list,
        db_config_file=self.dbconfig)
    load_data_raw_data_collection(
      db_config_file=self.dbconfig,
      collection_list=collection_list,
      cleanup_existing_collection=True)
    ca = \
      CollectionAdaptor(**{
        'session_class': self.session_class})
    ca.start_session()
    file_list = \
      ca.get_collection_files(
        collection_name='a',
        collection_type='csv')
    self.assertEqual(len(file_list.index), 1)
    self.assertTrue(
      os.path.join(self.temp_dir,'a.csv') in \
      file_list['file_path'].values.tolist())
    file_list = \
      ca.get_collection_files(
        collection_name='b',
        collection_type='csv')
    self.assertEqual(len(file_list.index), 1)
    self.assertTrue(
      os.path.join(self.temp_dir,'b1.csv') in \
      file_list['file_path'].values.tolist())
    file_pathA = os.path.join(self.temp_dir, 'a1.csv')
    with open(file_pathA, 'w') as f:
      f.write('a,b,c,d,e\n')
    collection_list = [{
      'collection_name': 'a',
      'collection_type': 'csv',
      'collection_table': 'file',
      'file_path': file_pathA
    }]
    load_data_raw_data_collection(
      db_config_file=self.dbconfig,
      collection_list=collection_list,
      cleanup_existing_collection=False)
    file_list = \
      ca.get_collection_files(
        collection_name='a',
        collection_type='csv')
    self.assertEqual(len(file_list.index), 2)
    self.assertTrue(
      os.path.join(self.temp_dir,'a.csv') in \
      file_list['file_path'].values.tolist())
    self.assertTrue(
      os.path.join(self.temp_dir,'a1.csv') in \
      file_list['file_path'].values.tolist())

  def test_copy_or_replace_file_to_disk_and_change_permission(self):
    source_file = os.path.join(self.temp_dir, 'source', 'source.txt')
    os.makedirs(os.path.dirname(source_file))
    with open(source_file, 'w') as f:
      f.write('source')
    dest_file = os.path.join(self.temp_dir, 'dest', 'dest.txt')
    copy_or_replace_file_to_disk_and_change_permission(
      source_path=source_file,
      destination_path=dest_file,
      replace_existing_file=True,
      make_file_and_dir_read_only=True)
    self.assertTrue(os.path.exists(dest_file))
    copy_or_replace_file_to_disk_and_change_permission(
      source_path=source_file,
      destination_path=dest_file,
      replace_existing_file=True,
      make_file_and_dir_read_only=True)
    self.assertTrue(os.path.exists(dest_file))
    os.chmod(os.path.dirname(dest_file), 0o777)
    os.chmod(dest_file, 0o777)
    remove_dir(os.path.dirname(dest_file))


class Dag22_bclconvert_demult_utils_testI(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    project_data = [{
      'project_igf_id': 'IGFP0001_test_22-8-2017_rna'}]
    user_data = [{
      'name': 'UserA', 
      'email_id': 'usera@ic.ac.uk', 
      'username': 'usera',
      'password': 'BBB'}]
    project_user_data = [{
      'project_igf_id': 'IGFP0001_test_22-8-2017_rna',
      'email_id': 'usera@ic.ac.uk',
      'data_authority': True}]
    sample_data = [
      {'sample_igf_id': 'IGFS001', 'project_igf_id': 'IGFP0001_test_22-8-2017_rna'},
      {'sample_igf_id': 'IGFS002', 'project_igf_id': 'IGFP0001_test_22-8-2017_rna',},
      {'sample_igf_id': 'IGFS003', 'project_igf_id': 'IGFP0001_test_22-8-2017_rna',}]
    base.start_session()
    ua = UserAdaptor(**{'session': base.session})
    ua.store_user_data(data=user_data)
    pa = ProjectAdaptor(**{'session': base.session})
    pa.store_project_and_attribute_data(data=project_data)
    pa.assign_user_to_project(data=project_user_data)
    sa = SampleAdaptor(**{'session': base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    base.close_session()
    self.temp_dir = get_temp_dir()
    self.test_template = \
      os.path.join(self.temp_dir, 'test_template.txt')
    with open(self.test_template, 'w') as f:
      f.write('sample_name = {{ SAMPLE_NAME }}')

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    remove_dir(self.temp_dir)

  def test_get_project_user_list(self):
    user_list, user_passwd_dict, hpc_user = \
      _get_project_user_list(
      db_config_file=self.dbconfig,
      project_name='IGFP0001_test_22-8-2017_rna')
    self.assertEqual(len(user_list), 1)
    self.assertEqual(user_list[0], 'usera')
    self.assertTrue(user_passwd_dict['usera'].startswith('{SHA}'))
    self.assertFalse(hpc_user)

  def test_get_project_sample_count(self):
    sample_count = \
      _get_project_sample_count(
      db_config_file=self.dbconfig,
      project_name='IGFP0001_test_22-8-2017_rna')
    self.assertEqual(sample_count, 3)

  def test_calculate_image_height_for_project_page(self):
    height = \
      _calculate_image_height_for_project_page(
        sample_count=4,
        height=1,
        threshold=2)
    self.assertEqual(height, 2)

  def test_create_output_from_jinja_template(self):
    output_file = \
      os.path.join(self.temp_dir, 'output.txt')
    _create_output_from_jinja_template(
      template_file=self.test_template,
      output_file=output_file,
      autoescape_list=['html', 'xml'],
      data=dict(SAMPLE_NAME='SampleA',  PROJECT_NAME='ProjectA'))
    self.assertTrue(os.path.exists(output_file))
    with open(output_file, 'r') as f:
      self.assertEqual(f.read(), 'sample_name = SampleA')

  def test_configure_qc_pages_for_ftp(self):
    output_dict = \
      _configure_qc_pages_for_ftp(
        template_dir='template',
        project_name='IGFP0001_test_22-8-2017_rna',
        db_config_file=self.dbconfig,
        remote_project_base_path='ftp_path',
        output_path=self.temp_dir)
    htacccess_path = \
      os.path.join(self.temp_dir, '.htaccess')
    htaccess_ftp_path = \
      os.path.join('ftp_path', 'IGFP0001_test_22-8-2017_rna', '.htaccess')
    self.assertEqual(len(output_dict), 6)
    self.assertTrue(os.path.exists(htacccess_path))
    self.assertEqual(output_dict[htacccess_path], htaccess_ftp_path)


class Dag22_bclconvert_demult_utils_testJ(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.samplesheet_file = \
      os.path.join(
        self.temp_dir,
        'SampleSheet.csv')
    samplesheet_data = """
    [Header]
    IEMFileVersion,4,,,,,,,,
    Application,NextSeq2000 FASTQ Only,,,,,,,,
    [Reads]
    151,,,,,,,,,
    151,,,,,,,,,
    [Settings]
    CreateFastqForIndexReads,1
    MinimumTrimmedReadLength,8
    FastqCompressionFormat,gzip
    MaskShortReads,8
    OverrideCycles,Y150N1;I8N2;N10;Y150N1
    [Data]
    Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Original_index,Original_Sample_ID,Original_Sample_Name
    IGF1_1,A01_1,,,SI-GA-C2,CCTAGACC,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_2,A01_2,,,SI-GA-C2,ATCTCTGT,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_3,A01_3,,,SI-GA-C2,TAGCTCTA,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_4,A01_4,,,SI-GA-C2,GGAGAGAG,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF2_1,A02_1,,,SI-GA-D2,TAACAAGG,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_2,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_3,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_4,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF3,A03,,,iaaa,GGTTCCTC,,,IGFQ1,,,,
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    samplesheet_data = re.sub(pattern1, '\n', samplesheet_data)
    samplesheet_data = re.sub(pattern2, '', samplesheet_data)
    with open(self.samplesheet_file, 'w') as fh:
        fh.write(samplesheet_data)

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_reset_single_cell_samplesheet(self):
    self.assertTrue(os.path.exists(self.samplesheet_file))
    samplesheet = \
      SampleSheet(self.samplesheet_file)
    samplesheet_df = \
      pd.DataFrame(samplesheet._data)
    self.assertTrue('Original_Sample_ID' in samplesheet_df.columns)
    self.assertTrue('IGF1_1' in samplesheet_df['Sample_ID'].values.tolist())
    reset_single_cell_samplesheet(
      samplesheet_file=self.samplesheet_file)
    self.assertTrue(os.path.exists(self.samplesheet_file))
    samplesheet = \
      SampleSheet(self.samplesheet_file)
    samplesheet_df = \
      pd.DataFrame(samplesheet._data)
    self.assertTrue('Original_Sample_ID' not in samplesheet_df.columns)
    self.assertTrue('IGF1' in samplesheet_df['Sample_ID'].values.tolist())
    self.assertFalse('IGF1_1' in samplesheet_df['Sample_ID'].values.tolist())
    self.assertTrue('IGF3' in samplesheet_df['Sample_ID'].values.tolist())

class Dag22_bclconvert_demult_utils_testK(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.demult_stats_file = \
      os.path.join(
        self.temp_dir,
        'Demultiplex_Stats.csv')
    demult_stats_data = """
    Lane,SampleID,Index,# Reads,# Perfect Index Reads,# One Mismatch Index Reads,# Two Mismatch Index Reads,% Reads,% Perfect Index Reads,% One Mismatch Index Reads,% Two Mismatch Index Reads
    1,IGF122019,CGGCTAAT-,413403,413403,0,0,0.0921,1.0000,0.0000,0.0000
    1,IGF121857_4,GTCGATGC-,26974,26974,0,0,0.0060,1.0000,0.0000,0.0000
    1,IGF122023,ATAGCGTC-,0,0,0,0,0.0000,1.0000,1.0000,0.0000
    1,IGF122026,GTCGGAGC-,350624,350624,0,0,0.0781,1.0000,0.0000,0.0000
    1,Undetermined,Undetermined,2345516,2345516,0,0,0.5227,1.0000,0.0000,0.0000
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    demult_stats_data = re.sub(pattern1, '\n', demult_stats_data)
    demult_stats_data = re.sub(pattern2, '', demult_stats_data)
    with open(self.demult_stats_file, 'w') as fh:
        fh.write(demult_stats_data)

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_check_demult_stats_file_for_failed_samples(self):
    check_status = \
      check_demult_stats_file_for_failed_samples(
        demult_stats_file=self.demult_stats_file)
    self.assertFalse(check_status)

class Dag22_bclconvert_demult_utils_testL(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.samplesheet_file = \
      os.path.join(
        self.temp_dir,
        'SampleSheet.csv')
    samplesheet_data = """
    [Header]
    IEMFileVersion,4,,,,,,,,
    Investigator Name,b,,,,,,,,
    Experiment Name,f_25-4-2022_Hi-C,,,,,,,,
    Date,31/05/2022,,,,,,,,
    Workflow,GenerateFASTQ,,,,,,,,
    Application,NextSeq FASTQ Only,,,,,,,,
    Assay,TruSeq HT,,,,,,,,
    Description,,,, ,,,,,
    Chemistry,Amplicon,,,,,,,,
    ,,,,,,,,,
    [Reads]
    151,,,,,,,,,
    151,,,,,,,,,
    ,,,,,,,,,
    [Settings]
    CreateFastqForIndexReads,1
    MinimumTrimmedReadLength,8
    FastqCompressionFormat,gzip
    MaskShortReads,8
    OverrideCycles,Y150N1;I8N2;N2I8;Y150N1
    [Data]
    Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Original_index,Original_Sample_ID,Original_Sample_Name
    IGF1,P01,,,Index9,CGGCTAAT,Index9,AGAACGAG,IGFQ001,,,,
    IGF2,P02,,,Index10,ATCGATCG,Index10,TGCTTCCA,IGFQ001,,,,
    IGF3,P03,,,Index2,ACTCTCGA,Index2,TGGTACAG,IGFQ001,,,,
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    samplesheet_data = re.sub(pattern1, '\n', samplesheet_data)
    samplesheet_data = re.sub(pattern2, '', samplesheet_data)
    with open(self.samplesheet_file, 'w') as fh:
        fh.write(samplesheet_data)
    ## register seqrun
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
      "platform_igf_id" : "M00001",
      "model_name" : "MISEQ",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id" : "M00001",
      "flowcell_type" : "MISEQ",
      "index_1" : "NO_CHANGE",
      "index_2" : "NO_CHANGE"}]
    pl = \
      PlatformAdaptor(**{'session' : base.session})
    pl.store_platform_data(
      data=platform_data)
    pl.store_flowcell_barcode_rule(
      data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id' : '171003_M00001_0089_000000000-D0YLK',
      'flowcell_id' : '000000000-D0YLK',
      'platform_igf_id' : 'M00001',
      'flowcell' : 'MISEQ'}]
    sra = \
      SeqrunAdaptor(**{'session' : base.session})
    sra.store_seqrun_and_attribute_data(
      data=seqrun_data)
    ## register project and run
    project_data = [{
      'project_igf_id':'IGFQ001'}]
    pa = ProjectAdaptor(**{'session' : base.session})
    pa.store_project_and_attribute_data(
      data=project_data)
    sample_data = [{
      'sample_igf_id' : 'IGF1', 'project_igf_id' : 'IGFQ001'},{
      'sample_igf_id' : 'IGF2', 'project_igf_id' : 'IGFQ001'},{
      'sample_igf_id' : 'IGF3', 'project_igf_id' : 'IGFQ001'}]
    sa = \
      SampleAdaptor(**{'session' : base.session})
    sa.store_sample_and_attribute_data(
      data=sample_data)
    experiment_data = [{
      'experiment_igf_id' : 'IGF1_MISEQ',
      'project_igf_id' : 'IGFQ001',
      'library_name' : 'IGF1',
      'platform_name': 'MISEQ',
      'sample_igf_id' : 'IGF1'}, {
      'experiment_igf_id' : 'IGF2_MISEQ',
      'project_igf_id' : 'IGFQ001',
      'library_name' : 'IGF2',
      'platform_name': 'MISEQ',
      'sample_igf_id' : 'IGF2'}, {
      'experiment_igf_id' : 'IGF3_MISEQ',
      'project_igf_id' : 'IGFQ001',
      'library_name' : 'IGF3',
      'platform_name': 'MISEQ',
      'sample_igf_id' : 'IGF3'}]
    ea = \
      ExperimentAdaptor(**{'session' : base.session})
    ea.store_project_and_attribute_data(
      data=experiment_data)
    run_data = [{
      'run_igf_id' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id' : 'IGF1_MISEQ',
      'seqrun_igf_id' : '171003_M00001_0089_000000000-D0YLK',
      'lane_number' : '1',
      'R1_READ_COUNT' : 1000}, {
      'run_igf_id' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id' : 'IGF2_MISEQ',
      'seqrun_igf_id' : '171003_M00001_0089_000000000-D0YLK',
      'lane_number' : '1',
      'R1_READ_COUNT' : 1000}, {
      'run_igf_id' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id' : 'IGF3_MISEQ',
      'seqrun_igf_id' : '171003_M00001_0089_000000000-D0YLK',
      'lane_number' : '1',
      'R1_READ_COUNT' : 1000}]
    ra = \
      RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(
      data=run_data)
    ## add fastq collection
    data = [{
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF1_S1_L001_R1_001.fastq.gz',
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF1_S1_L001_R2_001.fastq.gz',
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF2_S2_L001_R1_001.fastq.gz',
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF2_S2_L001_R2_001.fastq.gz',
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF3_S3_L001_R1_001.fastq.gz',
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : '/path/IGF3_S3_L001_R2_001.fastq.gz',
      'location' : 'HPC_PROJECT'}]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=data,
      calculate_file_size_and_md5=False)
    ## add ftp fastqc collection
    data = [{
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF1_S1_L001_R1_001.fastqc.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF1_S1_L001_R2_001.fastqc.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF2_S2_L001_R1_001.fastqc.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF2_S2_L001_R2_001.fastqc.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF3_S3_L001_R1_001.fastqc.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQC_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF3_S3_L001_R2_001.fastqc.html',
      'location' : 'ELIOT'}]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=data,
      calculate_file_size_and_md5=False)
    ## add ftp fastqscreen collection
    data = [{
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF1_S1_L001_R1_001.fastq_screen.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF1_S1_L001_R2_001.fastq_screen.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF2_S2_L001_R1_001.fastq_screen.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF2_S2_L001_R2_001.fastq_screen.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF3_S3_L001_R1_001.fastq_screen.html',
      'location' : 'ELIOT'}, {
      'name' : 'IGF3_MISEQ_000000000-D0YLK_1',
      'type' : 'FTP_FASTQSCREEN_HTML_REPORT',
      'table' : 'run',
      'file_path' : '/ftp/IGF3_S3_L001_R2_001.fastq_screen.html',
      'location' : 'ELIOT'}]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=data,
      calculate_file_size_and_md5=False)
    base.close_session()
    self.globus_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.globus_dir)

  def test_get_run_id_for_samples_flowcell_and_lane(self):
    run_id_dict = \
      get_run_id_for_samples_flowcell_and_lane(
        database_config_file=self.dbconfig,
        sample_igf_ids=['IGF1', 'IGF2', 'IGF3'],
        seqrun_igf_id='171003_M00001_0089_000000000-D0YLK',
        lane=1)
    self.assertEqual(len(run_id_dict), 3)
    self.assertTrue('IGF1' in run_id_dict)

  def test_get_files_for_collection_ids(self):
    collection_records_list = \
      get_files_for_collection_ids(
        database_config_file=self.dbconfig,
        collection_name_list=['IGF1_MISEQ_000000000-D0YLK_1', 'IGF2_MISEQ_000000000-D0YLK_1', 'IGF3_MISEQ_000000000-D0YLK_1'],
        collection_type='demultiplexed_fastq',
        collection_table='run')
    self.assertEqual(len(collection_records_list), 6)
    df = pd.DataFrame(collection_records_list)
    sample1_fastqs = \
      df[df['name']=='IGF1_MISEQ_000000000-D0YLK_1']['file_path'].values.tolist()
    self.assertTrue('/path/IGF1_S1_L001_R1_001.fastq.gz' in sample1_fastqs)
    self.assertTrue('/path/IGF2_S2_L001_R1_001.fastq.gz' not in sample1_fastqs)
    collection_records_list = \
      get_files_for_collection_ids(
        database_config_file=self.dbconfig,
        collection_name_list=['IGF1_MISEQ_000000000-D0YLK_1', 'IGF2_MISEQ_000000000-D0YLK_1', 'IGF3_MISEQ_000000000-D0YLK_1'],
        collection_type='FTP_FASTQC_HTML_REPORT',
        collection_table='run')
    self.assertEqual(len(collection_records_list), 6)
    df = pd.DataFrame(collection_records_list)
    sample1_fastqs = \
      df[df['name']=='IGF1_MISEQ_000000000-D0YLK_1']['file_path'].values.tolist()
    self.assertTrue('/ftp/IGF1_S1_L001_R1_001.fastqc.html' in sample1_fastqs)
    self.assertTrue('/ftp/IGF2_S2_L001_R2_001.fastqc.html' not in sample1_fastqs)

  def test_get_data_for_sample_qc_page(self):
    json_data = \
      get_data_for_sample_qc_page(
        project_igf_id='IGFQ001',
        seqrun_igf_id='171003_M00001_0089_000000000-D0YLK',
        lane_id=1,
        samplesheet_file=self.samplesheet_file,
        database_config_file=self.dbconfig,
        fastq_collection_type='demultiplexed_fastq',
        fastqc_collection_type='FTP_FASTQC_HTML_REPORT',
        fastq_screen_collection_type='FTP_FASTQSCREEN_HTML_REPORT',
        ftp_path_prefix='/ftp/',
        ftp_url_prefix='http://ftp.example.com/')
    self.assertEqual(len(json_data), 3)
    for entry in json_data:
      sample_id = entry['Sample_ID']
      if sample_id == 'IGF1':
        read_count = entry['Read_Counts']
        self.assertEqual(int(read_count), 1000)
        fastq_files = entry['Fastq_Files']
        self.assertEqual(len(fastq_files), 2)
        self.assertTrue('IGF1_S1_L001_R1_001.fastq.gz' in fastq_files)
        fastqc_files = entry['FastQC']
        self.assertEqual(len(fastqc_files), 2)
        self.assertTrue('<a href="http://ftp.example.com/IGF1_S1_L001_R1_001.fastqc.html">IGF1_S1_L001_R1_001.fastqc.html</a>' in fastqc_files)
        fastq_screen_files = entry['Fastq_Screen']
        self.assertEqual(len(fastq_screen_files), 2)
        self.assertTrue('<a href="http://ftp.example.com/IGF1_S1_L001_R2_001.fastq_screen.html">IGF1_S1_L001_R2_001.fastq_screen.html</a>' in fastq_screen_files)

class Dag22_bclconvert_demult_utils_testL(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
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
    self.multiqc_html_report_known = \
      "FTP_MULTIQC_HTML_REPORT_KNOWN"
    self.multiqc_html_report_undetermined = \
      "FTP_MULTIQC_HTML_REPORT_UNDETERMINED"
    self.sample_qc_page_collection_type = \
      "FTP_SAMPLE_QC_PAGE"
    self.demultiplexing_report_html_type = \
      "DEMULTIPLEXING_REPORT_HTML"
    self.run_qc_page_template = \
      "template/project_info/run_level_qc.html"
    data = [{
      'name' : 'Project1_000000000-D0YLK_1_8_10X',
      'type' : self.sample_qc_page_collection_type,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_1_8_10X/SampleQC.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_2_8_10X',
      'type' : self.sample_qc_page_collection_type,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_2_8_10X/SampleQC.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_1_8_10X',
      'type' : self.demultiplexing_report_html_type,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_1_8_10X/Demult_report.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_2_8_10X',
      'type' : self.demultiplexing_report_html_type,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_2_8_10X/Demult_report.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_1_8_10X_known',
      'type' : self.multiqc_html_report_known,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_1_8_10X_known/MultiQC.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_2_8_10X_known',
      'type' : self.multiqc_html_report_known,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_2_8_10X_known/MultiQC.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_1_8_10X_undetermined',
      'type' : self.multiqc_html_report_undetermined,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_1_8_10X_undetermined/MultiQC.html',
      'location' : 'ELIOT'}, {
      'name' : 'Project1_000000000-D0YLK_2_8_10X_undetermined',
      'type' : self.multiqc_html_report_undetermined,
      'table' : 'file',
      'file_path' : '/ftp/Project1_000000000-D0YLK_2_8_10X_undetermined/MultiQC.html',
      'location' : 'ELIOT'}]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=data,
      calculate_file_size_and_md5=False)
    base.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_build_run_qc_page(self):
    collection_name_dict = {
      'Project1_000000000-D0YLK_1_8_10X': {
        "project": 'Project1',
        "flowcell_id": '000000000-D0YLK',
        "lane": 1,
        "index_group": '8_10X',
        "tags": [
          'Project1',
          '000000000-D0YLK',
          1,
          '8_10X'
        ]},
      'Project1_000000000-D0YLK_2_8_10X': {
        "project": 'Project1',
        "flowcell_id": '000000000-D0YLK',
        "lane": 2,
        "index_group": '8_10X',
        "tags": [
          'Project1',
          '000000000-D0YLK',
          2,
          '8_10X'
        ]}
      }
    run_qc_dict = \
      _build_run_qc_page(
        collection_name_dict=collection_name_dict,
        sample_qc_page_collection_type=self.sample_qc_page_collection_type,
        known_multiqc_page_collection_type=self.multiqc_html_report_known,
        undetermined_multiqc_page_collection_type=self.multiqc_html_report_undetermined,
        demultiplexing_report_collection_type=self.demultiplexing_report_html_type,
        run_qc_page_template=self.run_qc_page_template,
        seqrun_igf_id='171003_M00001_0089_000000000-D0YLK',
        ftp_path_prefix='/ftp/',
        ftp_url_prefix='http://example.com/ftp/',
        output_dir=self.temp_dir,
        database_config_file=self.dbconfig,
        run_qc_page_name="index.html",
        known_multiqc_name_suffix="known",
        undetermined_multiqc_name_suffix="undetermined")
    self.assertTrue('Project1_000000000-D0YLK' in run_qc_dict)
    run_qc_page = \
      run_qc_dict['Project1_000000000-D0YLK']['run_qc_page']
    run_qc_json = \
      run_qc_dict['Project1_000000000-D0YLK']['run_qc_json']
    self.assertTrue(os.path.exists(run_qc_page))
    self.assertTrue(os.path.exists(run_qc_json))
    with open(run_qc_json, 'r') as fp:
      run_qc_json_data = json.load(fp)
    self.assertEqual(len(run_qc_json_data), 2)
    table_dfs = \
      pd.read_html(
        run_qc_page,
        attrs = {'class': 'table table-hover'},
        flavor='bs4')
    self.assertEqual(len(table_dfs), 1)
    table_df = table_dfs[0]
    self.assertEqual(len(table_df.index), 2)
    self.assertTrue('Lane' in table_df.columns)
    self.assertTrue('Index_group' in table_df.columns)
    lane_df = table_df[table_df['Lane'] == 1]
    self.assertEqual(len(lane_df.index), 1)
    self.assertEqual(lane_df['Index_group'].values[0], '8_10X')


class Dag22_bclconvert_demult_utils_testL(unittest.TestCase):
  def setUp(self):
    self.data_dir = get_temp_dir()
    self.globus_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.session_class
    base.start_session()
    # platform
    platform_data = [{
      "platform_igf_id" : "M00001",
      "model_name" : "MISEQ",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id" : "M00001",
      "flowcell_type" : "MISEQ",
      "index_1" : "NO_CHANGE",
      "index_2" : "NO_CHANGE"}]
    pl = \
      PlatformAdaptor(**{'session' : base.session})
    pl.store_platform_data(
      data=platform_data)
    pl.store_flowcell_barcode_rule(
      data=flowcell_rule_data)
    # seqrun
    seqrun_data = [{
      'seqrun_igf_id' : 'SEQ0001',
      'flowcell_id' : '000000000-D0YLK',
      'platform_igf_id' : 'M00001',
      'flowcell' : 'MISEQ'}]
    sra = \
      SeqrunAdaptor(**{'session' : base.session})
    sra.store_seqrun_and_attribute_data(
      data=seqrun_data)
    # project sample experiment and run
    project_data = [{
      'project_igf_id':'IGFQ001'}]
    pa = ProjectAdaptor(**{'session' : base.session})
    pa.store_project_and_attribute_data(
      data=project_data)
    sample_data = [{
      'sample_igf_id' : 'IGF1', 'project_igf_id' : 'IGFQ001'},{
      'sample_igf_id' : 'IGF2', 'project_igf_id' : 'IGFQ001'},{
      'sample_igf_id' : 'IGF3', 'project_igf_id' : 'IGFQ001'}]
    sa = \
      SampleAdaptor(**{'session' : base.session})
    sa.store_sample_and_attribute_data(
      data=sample_data)
    experiment_data = [{
      'experiment_igf_id' : 'IGF1_MISEQ',
      'project_igf_id' : 'IGFQ001',
      'library_name' : 'IGF1',
      'platform_name': 'MISEQ',
      'sample_igf_id' : 'IGF1'}, {
      'experiment_igf_id' : 'IGF2_MISEQ',
      'project_igf_id' : 'IGFQ001',
      'library_name' : 'IGF2',
      'platform_name': 'MISEQ',
      'sample_igf_id' : 'IGF2'}]
    ea = \
      ExperimentAdaptor(**{'session' : base.session})
    ea.store_project_and_attribute_data(
      data=experiment_data)
    run_data = [{
      'run_igf_id' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id' : 'IGF1_MISEQ',
      'seqrun_igf_id' : 'SEQ0001',
      'lane_number' : '1',
      'R1_READ_COUNT' : 1000}, {
      'run_igf_id' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'experiment_igf_id' : 'IGF2_MISEQ',
      'seqrun_igf_id' : 'SEQ0001',
      'lane_number' : '1',
      'R1_READ_COUNT' : 1000}]
    ra = \
      RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(
      data=run_data)
    # file collection
    files = [
      os.path.join(self.data_dir, 'IGF1_S1_L001_R1_001.fastq.gz'),
      os.path.join(self.data_dir, 'IGF1_S1_L001_R2_001.fastq.gz'),
      os.path.join(self.data_dir, 'IGF2_S2_L001_R1_001.fastq.gz'),
      os.path.join(self.data_dir, 'IGF2_S2_L001_R2_001.fastq.gz')]
    for f in files:
      with open(f , 'w') as fp:
        fp.write('A')
    data = [{
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : os.path.join(self.data_dir, 'IGF1_S1_L001_R1_001.fastq.gz'),
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF1_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : os.path.join(self.data_dir, 'IGF1_S1_L001_R2_001.fastq.gz'),
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : os.path.join(self.data_dir, 'IGF2_S2_L001_R1_001.fastq.gz'),
      'location' : 'HPC_PROJECT'}, {
      'name' : 'IGF2_MISEQ_000000000-D0YLK_1',
      'type' : 'demultiplexed_fastq',
      'table' : 'run',
      'file_path' : os.path.join(self.data_dir, 'IGF2_S2_L001_R2_001.fastq.gz'),
      'location' : 'HPC_PROJECT'}]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.load_file_and_create_collection(
      data=data,
      calculate_file_size_and_md5=False)

  def tearDown(self):
    remove_dir(self.data_dir)
    remove_dir(self.globus_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_copy_fastqs_for_sample_to_globus_dir(self):
    copy_fastqs_for_sample_to_globus_dir(
      sample_id_list=['IGF1',],
      seqrun_igf_id='SEQ0001',
      lane_number=1,
      globus_dir_for_index_group=self.globus_dir,
      fastq_collection_type='demultiplexed_fastq',
      database_config_file=self.dbconfig,
      active_status='ACTIVE',
      cleanup_globus_dir=True)
    self.assertTrue(
      os.path.exists(
        os.path.join(
          self.globus_dir,
          'IGF1',
          'IGF1_S1_L001_R1_001.fastq.gz')))
    self.assertFalse(
      os.path.exists(
        os.path.join(
          self.globus_dir,
          'IGF2',
          'IGF2_S2_L001_R1_001.fastq.gz')))

if __name__=='__main__':
  unittest.main()