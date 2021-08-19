import unittest,os,json
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_airflow.seqrun.ongoing_seqrun_processing import compare_existing_seqrun_files

class Compare_existing_seqrun_filesA(unittest.TestCase):
  def setUp(self):
    self.seqrun_dir = get_temp_dir()
    self.work_dir = get_temp_dir()
    self.seqrun_id = 'seqrun_id'
    os.mkdir(os.path.join(self.seqrun_dir,self.seqrun_id))
    file1 = os.path.join(self.seqrun_dir,self.seqrun_id,'f1')
    with open(file1,'w') as fp:
      fp.write('ATGC')
    self.file1 = os.path.relpath(file1,os.path.join(self.seqrun_dir,self.seqrun_id))
    self.file1_size = os.path.getsize(file1)
    file2 = os.path.join(self.seqrun_dir,self.seqrun_id,'f2')
    with open(file2,'w') as fp:
      fp.write('AAAAAAAAAA')
      fp.write('AAAAAAAAAA')
      fp.write('AAAAAAAAAA')
    self.file2 = os.path.relpath(file2,os.path.join(self.seqrun_dir,self.seqrun_id))
    self.file2_size = os.path.getsize(file2)
    file3 = os.path.join(self.seqrun_dir,self.seqrun_id,'f3')
    with open(file3,'w') as fp:
      fp.write('ATGC')
    self.file3 = os.path.relpath(file3,os.path.join(self.seqrun_dir,self.seqrun_id))
    self.file3_size = os.path.getsize(file3)
    os.remove(file3)
    data = [
      {'file_path':self.file1,'file_size':self.file1_size},
      {'file_path':self.file2,'file_size':self.file2_size},
      {'file_path':self.file3,'file_size':self.file3_size}]
    self.json_file = os.path.join(self.work_dir,'seqrun.json')
    with open(self.json_file,'w') as jp:
      json.dump(data,jp)
    with open(file2,'a') as fp:
      fp.write('CGTAAAAAAAAAAAAAAAAAAAAAA')
    self.file2_size = os.path.getsize(file2)

  def tearDown(self):
    remove_dir(self.work_dir)
    remove_dir(self.seqrun_dir)

  def test_compare_existing_seqrun_files(self):
    df = pd.read_json(self.json_file)
    compare_existing_seqrun_files(
      json_path=self.json_file,
      seqrun_id=self.seqrun_id,
      seqrun_base_path=self.seqrun_dir)
    df = pd.read_json(self.json_file)
    file1_entry = df[df['file_path']==self.file1]
    self.assertEqual(len(file1_entry.index),0)
    file2_entry = df[df['file_path']==self.file2]
    self.assertEqual(len(file2_entry.index),1)
    file3_entry = df[df['file_path']==self.file3]
    self.assertEqual(len(file3_entry.index),1)

if __name__=='__main__':
  unittest.main()