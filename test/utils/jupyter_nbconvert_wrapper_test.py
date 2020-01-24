import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.jupyter_nbconvert_wrapper import nbconvert_execute_in_singularity
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner

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

class Nbconvert_execute_test2(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.image_path = os.path.join(self.temp_dir,'image.sif')
    self.template_path = os.path.join(self.temp_dir,'template.ipynb')
    self.input_param_map = {'inputA':[os.path.join(self.temp_dir,'input_A')]}
    with open(self.template_path,'w') as fp:
      fp.write('{{ DATE_TAG }}')
    with open(self.image_path,'w') as fp:
      fp.write('a')
    with open(os.path.join(self.temp_dir,'input_A'),'w') as fp:
      fp.write('a')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_generate_ipynb_from_template(self):
    nr = \
      Notebook_runner(
        template_ipynb_path=self.template_path,
        output_dir=self.temp_dir,
        input_param_map=self.input_param_map)
    nr._generate_ipynb_from_template(param_map={'DATE_TAG':'AAAAA'})
    temp_notebook = os.path.join(nr.temp_dir,os.path.basename(self.template_path))
    with open(temp_notebook,'r') as fp:
      tag = fp.read()
    self.assertEqual(tag,'AAAAA')

  def test_substitute_input_path_and_copy_files_to_tempdir(self):
    nr = \
      Notebook_runner(
        template_ipynb_path=self.template_path,
        output_dir=self.temp_dir,
        input_param_map=self.input_param_map)
    modified_input_map = \
      nr._substitute_input_path_and_copy_files_to_tempdir()
    self.assertTrue('inputA' in modified_input_map)
    initial_path = nr.input_param_map.get('inputA')[0]
    modified_path = modified_input_map.get('inputA')[0]
    self.assertEqual(modified_path,os.path.join(nr.container_dir_prefix,os.path.basename(initial_path)))
    moved_path = os.path.join(nr.temp_dir,os.path.basename(initial_path))
    self.assertTrue(os.path.exists(moved_path))

  def test_get_date_stamp(self):
    nr = \
      Notebook_runner(
        template_ipynb_path=self.template_path,
        output_dir=self.temp_dir,
        input_param_map=self.input_param_map)
    stamp = nr._get_date_stamp()
    self.assertTrue(isinstance(stamp,str))

  def test_copy_container_output_and_update_map(self):
    nr = \
      Notebook_runner(
        template_ipynb_path=self.template_path,
        output_dir=self.temp_dir,
        input_param_map=self.input_param_map,
        output_file_map={'OUTPUT_B':'/tmp/input_B'})
    temp_notebook = os.path.join(nr.temp_dir,os.path.basename(self.template_path))
    temp_notebook_html = temp_notebook.replace('.ipynb','.html')
    temp_output = \
      os.path.join(nr.temp_dir,'input_B')
    with open(temp_output,'w') as fp:
      fp.write('a')
    with open(temp_notebook_html,'w') as fp:
      fp.write('a')
    new_output_map = \
      nr._copy_container_output_and_update_map(
        temp_notebook_path=temp_notebook)
    self.assertTrue(nr.notebook_tag in new_output_map)
    self.assertTrue(os.path.exists(new_output_map.get(nr.notebook_tag)))
    self.assertTrue(os.path.exists(new_output_map.get('OUTPUT_B')))

  def test_nbconvert_singularity(self):
    nr = \
      Notebook_runner(
        template_ipynb_path=self.template_path,
        output_dir=self.temp_dir,
        input_param_map=self.input_param_map,
        output_file_map={'OUTPUT_B':'/tmp/input_B'})
    _, run_cmd, _ = \
      nr.nbconvert_singularity(
        singularity_image_path=self.image_path,
        dry_run=True)
    self.assertTrue('--bind {0}:{1}'.format(nr.temp_dir,nr.container_dir_prefix) in run_cmd)


if __name__=='__main__':
  unittest.main()

