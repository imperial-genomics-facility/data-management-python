import unittest,os,json
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_airflow.seqrun.calculate_seqrun_file_size import calculate_seqrun_file_list

class Calculate_seqrun_file_list_testA(unittest.TestCase):
  def setUp(self):
    self.workdir = get_temp_dir()
    self.seqrun_id = 'seqrun1'
    os.mkdir(os.path.join(self.workdir,self.seqrun_id))
    file1 = os.path.join(self.workdir,self.seqrun_id,'Data')
    os.mkdir(file1)
    file1 = os.path.join(file1,'f1')
    self.file1 = os.path.relpath(file1,os.path.join(self.workdir,self.seqrun_id))
    with open(file1,'w') as fp:
      fp.write('ATGC')
    self.file1_size = os.path.getsize(file1)
    file2 = os.path.join(self.workdir,self.seqrun_id,'Thumbnail_Images')
    os.mkdir(file2)
    file2 = os.path.join(file2,'f2')
    self.file2 = os.path.relpath(file2,os.path.join(self.workdir,self.seqrun_id))
    with open(file2,'w') as fp:
      fp.write('CGTA')

  def tearDown(self):
    remove_dir(self.workdir)

  def test_calculate_seqrun_file_list(self):
    output_dir = get_temp_dir()
    output_json = \
      calculate_seqrun_file_list(
        seqrun_id=self.seqrun_id,
        seqrun_base_dir=self.workdir,
        output_path=output_dir)
    df = pd.read_json(output_json)
    self.assertTrue('file_path' in df.columns)
    file1_entry = df[df['file_path']==self.file1]
    self.assertEqual(len(file1_entry.index),1)
    self.assertEqual(file1_entry['file_size'].values[0],self.file1_size)
    file2_entry = df[df['file_path']==self.file2]
    self.assertEqual(len(file2_entry.index),0)

if __name__=='__main__':
  unittest.main()