import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.singularity_run_wrapper import singularity_run

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
        path_bind=self.temp_dir,
        args_list=['ls','-l','/home/vmuser'],
        dry_run=True)
    self.assertTrue('{0} --bind {1}:/tmp ls -l /home/vmuser'.\
                       format(os.path.basename(self.image_path),self.temp_dir) \
                       in singularity_cmd)

if __name__=='__main__':
  unittest.main()