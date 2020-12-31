import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner

class Nbconvert_execute_test1(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.output_dir = get_temp_dir()
    self.singularity_image_path = \
      os.path.join(self.temp_dir,'image.sif')
    self.template_ipynb_path = \
      os.path.join(self.temp_dir,'template.ipynb')
    self.input_param_map = {'INPUT_A':'aaaaa'}
    with open(self.template_ipynb_path,'w') as fp:
      fp.write('{{ INPUT_A }}')
    with open(self.singularity_image_path,'w') as fp:
      fp.write('a')

  def tearDown(self):
    remove_dir(self.temp_dir)
    remove_dir(self.output_dir)

  def test_execute_notebook_in_singularity(self):
    nb = \
      Notebook_runner(
        template_ipynb_path=self.template_ipynb_path,
        output_dir=self.output_dir,
        input_param_map=self.input_param_map,
        container_paths=None,
        singularity_image_path=self.singularity_image_path,
        dry_run=True)
    output_path,cmd = \
      nb.execute_notebook_in_singularity()
    with open(output_path,'r') as fp:
      data = fp.read()
      self.assertEqual(data,'aaaaa')
    self.assertTrue('{0} --to=html --execute'.format(os.path.basename(self.template_ipynb_path)) in cmd)


if __name__=='__main__':
  unittest.main()

