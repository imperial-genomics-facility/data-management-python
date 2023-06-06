import unittest,os
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet

class ProcessSingleCellSamplesheet_testA(unittest.TestCase):
  def setUp(self):
    self.samplesheet_file='data/singlecell_data/SampleSheet.csv'
    self.singlecell_barcode_json='data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json'
    self.output_file='data/singlecell_data/mod_SampleSheet.csv'
    if os.path.exists(self.output_file):
      os.remove(self.output_file)

  def tearDown(self):
    if os.path.exists(self.output_file):
      os.remove(self.output_file)

  def test_change_singlecell_barcodes1(self):
    sc_data=ProcessSingleCellSamplesheet(samplesheet_file=self.samplesheet_file,\
                                         singlecell_barcode_json=self.singlecell_barcode_json)
    sc_data.change_singlecell_barcodes(output_samplesheet=self.output_file)
    sc_samplesheet=SampleSheet(infile=self.output_file)
    sc_samplesheet.filter_sample_data(condition_key='Sample_Project',\
                                      condition_value='project_2', \
                                      method='include')
    self.assertEqual(len(sc_samplesheet._data),8)
    sc_samplesheet.filter_sample_data(condition_key='Sample_ID',\
                                      condition_value='IGF0008_1', \
                                      method='include')
    self.assertEqual(len(sc_samplesheet._data),1)
    sc_index=sc_samplesheet._data[0]['index']
    self.assertTrue(sc_index , ["GTGTATTA", "TGTGCGGG", "ACCATAAC", "CAACGCCT"])
    self.assertEqual(sc_samplesheet._data[0]['Original_index'],'SI-GA-B3')
    self.assertEqual(sc_samplesheet._data[0]['Original_Sample_ID'],'IGF0008')

  def test_change_singlecell_barcodes2(self):
    sc_data=ProcessSingleCellSamplesheet(samplesheet_file=self.samplesheet_file,\
                                         singlecell_barcode_json=self.singlecell_barcode_json)
    sc_data.change_singlecell_barcodes(output_samplesheet=self.output_file)
    samplesheet=SampleSheet(infile=self.output_file)
    samplesheet.filter_sample_data(condition_key='Sample_Project',\
                                   condition_value='project_2', \
                                   method='exclude')
    self.assertEqual(len(samplesheet._data),5)
    self.assertTrue('Original_Sample_ID' in samplesheet._data_header)

class ProcessSingleCellSamplesheet_testB(unittest.TestCase):
  def setUp(self):
    self.samplesheet_file='data/singlecell_data/SampleSheet_10x_NA.csv'
    self.singlecell_barcode_json='data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json'
    self.output_file='data/singlecell_data/mod_SampleSheet.csv'
    if os.path.exists(self.output_file):
      os.remove(self.output_file)

  def tearDown(self):
    if os.path.exists(self.output_file):
      os.remove(self.output_file)

  def test_change_singlecell_barcodes3(self):
    sc_data = \
      ProcessSingleCellSamplesheet(\
        samplesheet_file=self.samplesheet_file,
        singlecell_barcode_json=self.singlecell_barcode_json)
    sc_data.\
      change_singlecell_barcodes(output_samplesheet=self.output_file)
    sc_samplesheet = \
      SampleSheet(infile=self.output_file)
    sc_samplesheet.\
      filter_sample_data(\
        condition_key='Sample_Project',
        condition_value='project_3',
        method='include')
    sc_index = sc_samplesheet._data[0]['index']
    self.assertTrue(sc_index in ("AAACGGCG","CCTACCAT","GGCGTTTC","TTGTAAGA"))


class ProcessSingleCellDualIndexSamplesheetA(unittest.TestCase):
  def setUp(self):
    self.samplesheet_file = 'data/singlecell_data/SampleSheet_dual.csv'
    self.sc_barcode_json = 'data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json'
    self.work_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.work_dir)

  def test_modify_samplesheet_for_sc_dual_barcode1(self):
    sc_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=self.samplesheet_file,
        singlecell_dual_index_barcode_json=self.sc_barcode_json,
        platform='HISEQ4000')
    output = os.path.join(self.work_dir,'out1.csv')
    sc_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=output)
    sa = SampleSheet(output)
    df = pd.DataFrame(sa._data)
    df.fillna('',inplace=True)
    #print(df.to_dict(orient='records'))
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['index'].values[0],'GTGGCCTCAT')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['index2'].values[0],'TCACTTTCGA')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['Description'].values[0],'')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['index'].values[0],'CACGGTGAAT')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['index2'].values[0],'TGTGACGAAC')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['Description'].values[0],'')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0008']['index'].values[0],'SI-GA-B3')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0008']['Description'].values[0],'10X')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0003']['index'].values[0],'ATTACTCG')

  def test_modify_samplesheet_for_sc_dual_barcode2(self):
    sc_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=self.samplesheet_file,
        singlecell_dual_index_barcode_json=self.sc_barcode_json,
        platform='NOVASEQ6000')
    output = os.path.join(self.work_dir,'out2.csv')
    sc_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=output)
    sa = SampleSheet(output)
    df = pd.DataFrame(sa._data)
    df.fillna('',inplace=True)
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['index'].values[0],'GTGGCCTCAT')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['index2'].values[0],'TCACTTTCGA')
    self.assertEqual(
      df[df['Sample_ID']=='IGF0009']['Description'].values[0],'')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['index'].values[0],'CACGGTGAAT')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['index2'].values[0],'TGTGACGAAC')
    self.assertEqual(
      df[df['Sample_ID']=='IGF00010']['Description'].values[0],'')

if __name__=='__main__':
  unittest.main()
