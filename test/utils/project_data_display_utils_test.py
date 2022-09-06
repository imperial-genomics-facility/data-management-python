import os, unittest, json
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
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
    self.data=pd.DataFrame(data)

  def test_convert_project_data_gviz_data(self):
    (description,formatted_data,column_order)=convert_project_data_gviz_data(input_data=self.data)
    self.assertTrue('HABCD1234' in description)
    self.assertEqual(len(formatted_data),1)
    data_line=formatted_data[0]
    sample_id=data_line['sample_igf_id']
    read_count=data_line['HABCD1234']
    self.assertEqual(sample_id,'IGF10001')
    self.assertEqual(int(read_count),112577360)

class Add_seqrun_path_info1(unittest.TestCase):
  def setUp(self):
    data=[{'seqrun_igf_id':'180319_K00001_0039_AHABCD1234',
           'flowcell_id':'HABCD1234'}]
    self.data=pd.DataFrame(data)
    self.temp_dir=get_temp_dir()
    self.output_file=os.path.join(self.temp_dir,'out.json')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_add_seqrun_path_info(self):
    add_seqrun_path_info(input_data=self.data,
                         output_file=self.output_file)                          # write json file
    with open(self.output_file,'r') as j_data:
      json_data=json.load(j_data)
    json_data_line=json_data[0]
    self.assertEqual(json_data_line['flowcell_id'], 'HABCD1234 - 2018-03-19')
    # self.assertEqual(json_data_line['path'],'2018-03-19/HABCD1234')
    self.assertEqual(json_data_line['path'],'HABCD1234')

if __name__ == '__main__':
  unittest.main()