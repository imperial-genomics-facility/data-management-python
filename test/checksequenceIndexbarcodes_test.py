import unittest, json, os, shutil
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.data_qc.check_sequence_index_barcodes import CheckSequenceIndexBarcodes, IndexBarcodeValidationError

class RunAdaptor_test1(unittest.TestCase):
  def setUp(self):
    self.samplesheet_file='data/check_index_qc/SampleSheet.csv'
    self.stats_json_file='data/check_index_qc/Stats.json'
    ci1=CheckSequenceIndexBarcodes(samplesheet_file=self.samplesheet_file, \
                                   stats_json_file=self.stats_json_file,\
                                   platform_name='NEXTSEQ')
    self.checkindex_object=ci1
    self.workdir='.'
    
  def tearDown(self):
    total_barcodes=os.path.join(self.workdir,'total_barcodes.png')
    individual_barcodes=os.path.join(self.workdir,'individual_barcodes.png')
    if os.path.exists(total_barcodes):
      os.remove(total_barcodes)
      
    if os.path.exists(individual_barcodes):
      os.remove(individual_barcodes)
    
  def test_get_dataframe_from_stats_json(self):
    ci1=self.checkindex_object
    raw_json_data=ci1._get_dataframe_from_stats_json(json_file=self.stats_json_file)
    known_index_count=len(raw_json_data.groupby('tag').get_group('known').index)
    unknown_index_count=len(raw_json_data.groupby('tag').get_group('unknown').index)
    self.assertEqual(known_index_count, 64)
    self.assertEqual(unknown_index_count, 4000)
    
    
  def test_check_index_for_match(self):
    ci1=self.checkindex_object
    samplesheet_data=SampleSheet(infile=self.samplesheet_file)
    all_known_indexes=samplesheet_data.get_indexes()
    self.assertTrue('TAAGGCGA+GCGATCTA' in all_known_indexes)
    raw_json_data=ci1._get_dataframe_from_stats_json(json_file=self.stats_json_file)
    unknown_raw_df=raw_json_data.groupby('tag').get_group('unknown')
    unknown_raw_df_subset=unknown_raw_df[unknown_raw_df['index']=='GTAGAGGA']
    test_series=unknown_raw_df_subset.iloc[0]
    self.assertEqual(test_series['tag'], 'unknown')
    test_series=ci1._check_index_for_match(data_series=test_series,\
                                           index_vals=all_known_indexes)
    self.assertEqual(test_series['tag'], 'mix_index_match')
    
if __name__=='__main__':
  unittest.main()