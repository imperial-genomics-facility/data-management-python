import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.singularity_run_wrapper import singularity_run
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd


class Singularity_run_test1(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.image_path = os.path.join(self.temp_dir,'image.sif')
    with open(self.image_path,'w') as fp:
      fp.write('a')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_singularity_run(self):
    singularity_cmd = \
      singularity_run(
        image_path=self.image_path,
        bind_dir_list=[self.temp_dir],
        args_list=['ls','-l','/home/vmuser'],
        dry_run=True)
    self.assertTrue('{0} --bind {1} ls -l /home/vmuser'.\
                       format(os.path.basename(self.image_path),[self.temp_dir]) \
                       in singularity_cmd)

  def test_execute_singuarity_cmd(self):
    singularity_cmd = \
      execute_singuarity_cmd(
        image_path=self.image_path,
        bind_dir_list=[self.temp_dir],
        command_string='ls -l /home/vmuser',
        dry_run=True)
    self.assertEqual('singularity exec --bind {1} {0} ls -l /home/vmuser'.\
                      format(self.image_path,[self.temp_dir]),
                      singularity_cmd)

if __name__=='__main__':
  unittest.main()