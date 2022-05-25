import os
import re
import unittest
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.illumina.samplesheet import SampleSheet
from igf_airflow.utils.dag23_test_bclconvert_demult_utils import _format_samplesheet_per_index_group

class Dag23_test_bclconvert_demult_utils_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.out_dir = get_temp_dir()
    self.singlecell_barcode_json = 'data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json'
    self.singlecell_dual_barcode_json = 'data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json'
    samplesheet = """
        [Header],,,,,,,,,,
        IEMFileVersion,4,,,,,,,,,
        Workflow,GenerateFASTQ,,,,,,,,,
        Application,NextSeq FASTQ Only,,,,,,,,,
        Assay,TruSeq HT,,,,,,,,,
        Description,,,,,,,,,,
        [Reads],,,,,,,,,,
        28,,,,,,,,,,
        90,,,,,,,,,,
        [Settings],,,,,,,,,,
        Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA,,,,,,,,,
        AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT,,,,,,,,,
        [Data],,,,,,,,,,
        Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
        IGF1,sample1,,,SI-TT-A4,SI-TT-A4,,,IGFP1,10X
        IGF2,sample2,,,SI-TT-D12,SI-TT-D12,,,IGFP2,10X
        IGF3,sample3,,,,TTGGCTCT,,ATCGTAGC,IGFP3,
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    samplesheet = re.sub(pattern1, '\n', samplesheet)
    samplesheet = re.sub(pattern2, '', samplesheet)
    self.samplesheet_file1 = os.path.join(self.temp_dir, 'samplesheet1.csv')
    with open(self.samplesheet_file1, 'w') as fh:
        fh.write(samplesheet)
    samplesheet = """
        [Header],,,,,,,,,,
        IEMFileVersion,4,,,,,,,,,
        Workflow,GenerateFASTQ,,,,,,,,,
        Application,HiSeq FASTQ Only,,,,,,,,,
        Assay,TruSeq HT,,,,,,,,,
        Description,,,, ,,,,,,
        Chemistry,Amplicon,,,,,,,,,
        [Reads],,,,,,,,,,
        76,,,,,,,,,,
        76,,,,,,,,,,
        [Settings],,,,,,,,,,
        Adapter,AGATCGGAAGAGCACACGTCTGAACTCCAGTCA,,,,,,,,,
        AdapterRead2,AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT,,,,,,,,,
        [Data],,,,,,,,,,
        Lane,Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description
        1,IGF1,sample1,,,NEB1,ATCACG,,,IGFQ1,
        2,IGF2,sample2,,,S782,GGTGTACA,S528,AGACGCTA,IGFQ2,
        3,IGF3,sample3,,,S762,TTACCGAC,S512,CGAATACG,IGFQ3,
        4,IGF4,sample4,,,S762,TTACCGAC,S512,CGAATACG,IGFQ4,
        5,IGF5,sample5,,,S797,AGACCTTG,S501,TTACGTGC,IGFQ5,
        6,IGF6,sample6,,,S731,CGGCATTA,S571,GTCAGTCA,IGFQ006,
        7,IGF7,sample7,,,UDI3,CGCTGCTC,UDI3,GGCAGATC,IGFQ007,
        7,IGF8,sample8,,,SI-TT-B5,SI-TT-B5,,,IGFQ8,10x
        8,IGF9,sample9,,,S744,CCAAGGTT,S552,AGGATAGC,IGFQ9,
    """
    samplesheet = re.sub(pattern1, '\n', samplesheet)
    samplesheet = re.sub(pattern2, '', samplesheet)
    self.samplesheet_file2 = os.path.join(self.temp_dir, 'samplesheet2.csv')
    with open(self.samplesheet_file2, 'w') as fh:
        fh.write(samplesheet)

  def tearDown(self):
    remove_dir(self.temp_dir)
    remove_dir(self.out_dir)

  def test_format_samplesheet_per_index_group1(self):
    formatted_samplesheets = \
      _format_samplesheet_per_index_group(
        samplesheet_file=self.samplesheet_file1,
        singlecell_barcode_json=self.singlecell_barcode_json,
        singlecell_dual_barcode_json=self.singlecell_dual_barcode_json,
        platform='NEXTSEQ2000',
        output_dir=self.out_dir,
        singlecell_tag='10X',
        index_column='index',
        index2_column='index2',
        lane_column='Lane',
        description_column='Description',
        index2_rule='NO_CHANGE')
    self.assertEqual(len(formatted_samplesheets), 3)
    df = pd.DataFrame(formatted_samplesheets)
    df['tag'] = df['tag'].astype(str)
    df['lane'] = df['lane'].astype(str)
    sg1 = df[(df['lane']=='all') &(df['tag']=='16')]
    self.assertEqual(len(sg1.index), 1)
    samplesheet = sg1['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 1)                 # 1 16bp index
    self.assertTrue('IGF3' in sa_df['Sample_ID'].values)  # sample3
    sg2 = df[(df['lane']=='all') &(df['tag']=='20')]
    self.assertEqual(len(sg2.index), 1)
    samplesheet = sg2['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 2)                  # 2 20bp indices
    self.assertTrue('IGF1' in sa_df['Sample_ID'].values)   # sample1
    self.assertTrue('IGF2' in sa_df['Sample_ID'].values)   # sample2
    sg3 = df[(df['lane']=='all') &(df['tag']=='merged')]
    self.assertEqual(len(sg3.index), 1)
    samplesheet = sg3['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 3)                  # 3 merged indices
    self.assertTrue('IGF1' in sa_df['Sample_ID'].values)   # sample1
    self.assertTrue('IGF2' in sa_df['Sample_ID'].values)   # sample2
    self.assertTrue('IGF3' in sa_df['Sample_ID'].values)   # sample3

def test_format_samplesheet_per_index_group2(self):
    formatted_samplesheets = \
      _format_samplesheet_per_index_group(
        samplesheet_file=self.samplesheet_file2,
        singlecell_barcode_json=self.singlecell_barcode_json,
        singlecell_dual_barcode_json=self.singlecell_dual_barcode_json,
        platform='HISEQ4000',
        output_dir=self.out_dir,
        singlecell_tag='10X',
        index_column='index',
        index2_column='index2',
        lane_column='Lane',
        description_column='Description',
        index2_rule='REVCOMP')
    self.assertEqual(len(formatted_samplesheets), 17)
    df = pd.DataFrame(formatted_samplesheets)
    df['tag'] = df['tag'].astype(str)
    df['lane'] = df['lane'].astype(str)
    sg1 = df[(df['lane']=='1') &(df['tag']=='6')]
    self.assertEqual(len(sg1.index), 1)
    samplesheet = sg1['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 1)                 # 1 6bp index
    self.assertTrue('IGF1' in sa_df['Sample_ID'].values)
    sg1m = df[(df['lane']=='1') &(df['tag']=='1_merged')]
    self.assertEqual(len(sg1m.index), 1)
    sg2 = df[(df['lane']=='2') &(df['tag']=='16')]
    self.assertEqual(len(sg2.index), 1)
    sg2m = df[(df['lane']=='2') &(df['tag']=='2_merged')]
    self.assertEqual(len(sg2m.index), 1)
    sg7_1 = df[(df['lane']=='7') &(df['tag']=='16')]
    self.assertEqual(len(sg7_1.index), 1)
    samplesheet = sg7_1['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 1)                 # 1 16bp index
    self.assertTrue('IGF7' in sa_df['Sample_ID'].values)
    sg7_2 = df[(df['lane']=='7') &(df['tag']=='20')]
    self.assertEqual(len(sg7_2.index), 1)
    samplesheet = sg7_2['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 1)                 # 1 20bp index
    self.assertTrue('IGF8' in sa_df['Sample_ID'].values)
    sg7_m = df[(df['lane']=='7') &(df['tag']=='7_merged')]
    self.assertEqual(len(sg7_m.index), 2)
    samplesheet = sg7_m['samplesheet_file'].values[0]
    self.assertTrue(os.path.exists(samplesheet))
    sa = SampleSheet(samplesheet)
    sa_df = pd.DataFrame(sa._data)
    self.assertEqual(len(sa_df.index), 2)                 # 2 16bp index
    self.assertTrue('IGF7' in sa_df['Sample_ID'].values)
    self.assertTrue('IGF8' in sa_df['Sample_ID'].values)

if __name__=='__main__':
  unittest.main()