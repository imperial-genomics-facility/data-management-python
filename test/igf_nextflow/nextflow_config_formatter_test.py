import unittest,os
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_nextflow.nextflow_utils.nextflow_config_formatter import format_nextflow_config_file

class Nextflow_config_formatter_testA(unittest.TestCase):
  def setUp(self):
    self.workdir = get_temp_dir()
    self.template_file = 'igf_nextflow/config/nextflow.cfg_template'

  def tearDown(self):
    remove_dir(self.workdir)

  def test_format_nextflow_config_file(self):
    bind_dir_list = [self.workdir]
    output_file = \
      os.path.join(self.workdir,'nextflow.cfg')
    hpc_queue = 'test_queue'
    format_nextflow_config_file(
      template_file=self.template_file,
      output_file=output_file,
      bind_dir_list=bind_dir_list,
      hpc_queue_name=hpc_queue)
    self.assertTrue(os.path.exists(output_file))
    hpc_queue_entry = ''
    bind_dir_args = []
    with open(output_file,'r') as fp:
      for line in fp:
        if line.strip().startswith('queue ='):
          hpc_queue_entry = line.strip()
        if line.strip().startswith('runOptions'):
          bind_dir_args = [
              i.strip('"')
                for i in line.strip().split(',')]
    self.assertEqual(hpc_queue_entry,'queue = "{0}"'.format(hpc_queue))
    self.assertTrue(self.workdir in bind_dir_args)

if __name__=='__main__':
  unittest.main()