import os, unittest
import pandas as pd
from igf_data.utils.project_data_display_utils import convert_project_data_gviz_data, add_seqrun_path_info

class Convert_project_data_gviz_data1(unittest.TestCase):
  def setUp(self):
    data=[{'attribute_value': '28357702',
           'flowcell_id': 'HABCD1234',
           'project_igf_id': 'IGFP003_test1_24-1-18',
           'sample_igf_id': 'IGF10001'},
          {'attribute_value': '28368518',
           'flowcell_id': 'HABCD1234',
           'project_igf_id': 'IGFP003_test1_24-1-18',
           'sample_igf_id': 'IGF10001'},
          {'attribute_value': '27887870',
           'flowcell_id': 'HABCD1234',
           'project_igf_id': 'IGFP003_test1_24-1-18',
           'sample_igf_id': 'IGF10001'},
          {'attribute_value': '27963270',
           'flowcell_id': 'HABCD1234',
           'project_igf_id': 'IGFP003_test1_24-1-18',
           'sample_igf_id': 'IGF10001'}]
    data=pd.DataFrame(data)
    self.data = data

  def test_convert_project_data_gviz_data(self):
    (description,formatted_data,column_order)=convert_project_data_gviz_data(input_data=self.data)
    self.assertTrue('HABCD1234' in description)
    self.assertEqual(len(formatted_data),1)
    data_line=formatted_data[0]
    sample_id=data_line['sample_igf_id']
    read_count=data_line['HABCD1234']
    self.assertEqual(sample_id,'IGF10001')
    self.assertEqual(int(read_count),112577360)

if __name__ == '__main__':
  unittest.main()