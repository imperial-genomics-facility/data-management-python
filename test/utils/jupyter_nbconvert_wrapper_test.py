import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.jupyter_nbconvert_wrapper import nbconvert_execute_in_singularity

class Nbconvert_execute_test1(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.image_path = os.path.join(self.temp_dir,'image.sif')
    self.template_path = os.path.join(self.temp_dir,'template.ipynb')
    self.input_list = [os.path.join(self.temp_dir,'input_A')]
    with open(self.template_path,'w') as fp:
      fp.write('a')
    with open(self.image_path,'w') as fp:
      fp.write('a')
    for f in self.input_list:
      with open(f,'w') as fp:
        fp.write('a')


  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_nbconvert_execute_in_singularity1(self):
    output_file_map,run_cmd = \
      nbconvert_execute_in_singularity(
        image_path=self.image_path,
        ipynb_path=self.template_path,
        input_list=self.input_list,
        output_dir=self.temp_dir,
        output_format='html',
        output_file_map={'outputA':'/tmp/input_A'},
        timeout=600,
        kernel='python3',
        allow_errors=False,
        dry_run=True)
    self.assertEqual(
      output_file_map.get('outputA'),
      os.path.join(self.temp_dir,'input_A'))
    notebook_path = \
      os.path.join(
        self.temp_dir,
        self.template_path)
    notebook_path = notebook_path.replace('.ipynb','.html')
    self.assertEqual(
      output_file_map.get('notebook'),notebook_path)
    self.assertTrue('jupyter nbconvert' in run_cmd)
    self.assertTrue('--allow-errors' not in run_cmd)

  def test_nbconvert_execute_in_singularity2(self):
    output_file_map,run_cmd = \
      nbconvert_execute_in_singularity(
        image_path=self.image_path,
        ipynb_path=self.template_path,
        input_list=self.input_list,
        output_dir=self.temp_dir,
        output_format='markdown',
        output_file_map={'outputA':'/tmp/input_A'},
        timeout=600,
        kernel='python3',
        allow_errors=True,
        dry_run=True)
    self.assertEqual(
      output_file_map.get('outputA'),
      os.path.join(self.temp_dir,'input_A'))
    notebook_path = \
      os.path.join(
        self.temp_dir,
        self.template_path)
    notebook_path = notebook_path.replace('.ipynb','.md')
    self.assertEqual(
      output_file_map.get('notebook'),notebook_path)
    self.assertTrue('jupyter nbconvert' in run_cmd)
    self.assertTrue('--allow-errors' in run_cmd)

if __name__=='__main__':
  unittest.main()

