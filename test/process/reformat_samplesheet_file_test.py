import unittest,os
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.metadata_reformat.reformat_samplesheet_file import Reformat_samplesheet_file,SampleSheet

class Reformat_samplesheet_file_testA(unittest.TestCase):
  def setUp(self):
    self.tmp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.tmp_dir)

  def test_detect_tenx_barcodes(self):
    description = \
      Reformat_samplesheet_file.\
        detect_tenx_barcodes(index='SI-GA-A1')
    self.assertEqual(description,'10X')

  def test_correct_samplesheet_data_row(self):
    data = pd.Series(\
      {'Lane':1,
       'Sample_ID':'IGF1 ',
       'Sample_Name':'samp_(1)',
       'index':'SI-GA-A1',
       'Sample_Project':'IGFQ scRNA-seq5primeFB',
       'Description':''})
    re_samplesheet = \
      Reformat_samplesheet_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_samplesheet.csv')
    data = \
      re_samplesheet.\
        correct_samplesheet_data_row(row=data)
    self.assertEqual(data['Sample_ID'],'IGF1')
    self.assertEqual(data['Sample_Name'],'samp-1')
    self.assertEqual(data['Sample_Project'],'IGFQ-scRNA-seq5primeFB')
    self.assertEqual(data['Description'],'10X')

  def test_reformat_raw_samplesheet_file(self):
    re_samplesheet = \
      Reformat_samplesheet_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_samplesheet.csv',
        remove_adapters=True)
    output_file = os.path.join(self.tmp_dir,'samplesheet.csv')
    re_samplesheet.\
      reformat_raw_samplesheet_file(\
        output_file=output_file)
    sa = SampleSheet(infile=output_file)
    self.assertFalse(sa.check_sample_header('Settings','Adapter'))
    data = pd.DataFrame(sa._data)
    sample1 = data[data['Sample_ID']=='IGF1']
    self.assertEqual(sample1['Sample_Name'].values[0],'samp-1')
    self.assertEqual(sample1['Sample_Project'].values[0],'IGFQ1-scRNA-seq5primeFB')
    self.assertEqual(sample1['Description'].values[0],'10X')

if __name__ == '__main__':
  unittest.main()