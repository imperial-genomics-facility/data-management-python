import os,unittest
import pandas as pd
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.samplesheet_utils import get_formatted_samplesheet_per_lane
from igf_data.utils.samplesheet_utils import samplesheet_validation_and_metadata_checking

class SamplesheetUtils_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.platform_name = 'HISEQ4000'
    self.samplesheet_file = 'data/singlecell_data/SampleSheet_dual.csv'
    self.sc_index_json = 'data/singlecell_data/chromium-shared-sample-indexes-plate_20180301.json'
    self.sc_dual_index_json = 'data/singlecell_data/chromium_dual_indexes_plate_TT_NT_20210209.json'

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_get_formatted_samplesheet_per_lane1(self):
    output_list = \
      get_formatted_samplesheet_per_lane(
        samplesheet_file=self.samplesheet_file,
        singlecell_barcode_json=self.sc_index_json,
        singlecell_dual_barcode_json=self.sc_dual_index_json,
        runinfo_file='data/singlecell_data/RunInfo_dual.xml',
        output_dir=self.temp_dir,
        platform=self.platform_name,
        filter_lane=None,
        single_cell_tag='10X',
        index1_rule=None,
        index2_rule=None)
    df = pd.DataFrame(output_list)
    sa = SampleSheet(df[df['lane_id']=='5']['samplesheet_file'].values[0])
    sdf = pd.DataFrame(sa._data)
    #print(sdf.to_dict(orient='records'))
    self.assertEqual(df[df['lane_id']=='5']['bases_mask'].values[0],'y150n1,i10,i10,y150n1')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0009']['index'].values[0],'GTGGCCTCAT')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0009']['index2'].values[0],'TCACTTTCGA')
    sa = SampleSheet(df[df['lane_id']=='3']['samplesheet_file'].values[0])
    self.assertEqual(df[df['lane_id']=='3']['bases_mask'].values[0],'y150n1,i8n2,i8n2,y150n1')
    sdf = pd.DataFrame(sa._data)
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0001']['index'].values[0],'ATTACTCG')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0001']['index2'].values[0],'AGGCTATA')

  def test_get_formatted_samplesheet_per_lane2(self):
    output_list = \
      get_formatted_samplesheet_per_lane(
        samplesheet_file=self.samplesheet_file,
        singlecell_barcode_json=self.sc_index_json,
        singlecell_dual_barcode_json=self.sc_dual_index_json,
        runinfo_file='data/singlecell_data/RunInfo_dual.xml',
        output_dir=self.temp_dir,
        platform=self.platform_name,
        filter_lane=None,
        single_cell_tag='10X',
        index1_rule=None,
        index2_rule='REVCOMP')
    df = pd.DataFrame(output_list)
    #print(df.to_dict(orient='records'))
    sa = SampleSheet(df[df['lane_id']=='5']['samplesheet_file'].values[0])
    sdf = pd.DataFrame(sa._data)
    #print(sdf.to_dict(orient='records'))
    self.assertEqual(df[df['lane_id']=='5']['bases_mask'].values[0],'y150n1,i10,i10,y150n1')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0009']['index'].values[0],'GTGGCCTCAT')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0009']['index2'].values[0],'TCACTTTCGA')
    sa = SampleSheet(df[df['lane_id']=='3']['samplesheet_file'].values[0])
    self.assertEqual(df[df['lane_id']=='3']['bases_mask'].values[0],'y150n1,i8n2,i8n2,y150n1')
    sdf = pd.DataFrame(sa._data)
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0001']['index'].values[0],'ATTACTCG')
    self.assertEqual(sdf[sdf['Sample_ID']=='IGF0001']['index2'].values[0],'TATAGCCT')

if __name__=='__main__':
  unittest.main()