import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.tools.deeptools_utils import run_plotCoverage,run_bamCoverage,run_plotFingerprint

class Deeptools_util_test1(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.input_bam = os.path.join(self.temp_dir,'input.bam')
    self.blacklist_file = os.path.join(self.temp_dir,'blacklist.bed')
    with open(self.input_bam,'w') as fp:
      fp.write('1')

    with open(self.blacklist_file,'w') as fp:
      fp.write('1')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_run_plotCoverage(self):
    deeptools_cmd = \
      run_plotCoverage(\
        bam_files=[self.input_bam],
        output_raw_counts='out.raw.txt',
        plotcov_stdout='out.stdout.txt',
        output_plot='out.plot.pdf',
        blacklist_file=self.blacklist_file,
        thread=1,
        params_list=None,
        dry_run=True)
    self.assertTrue(self.input_bam in deeptools_cmd)
    self.assertTrue(self.blacklist_file in deeptools_cmd)

  def test_run_bamCoverage(self):
    deeptools_cmd = \
      run_bamCoverage(\
        bam_files=[self.input_bam],
        output_file='out.bw',
        blacklist_file=self.blacklist_file,
        thread=1,
        dry_run=True)
    self.assertTrue(self.input_bam in deeptools_cmd)
    self.assertTrue(self.blacklist_file in deeptools_cmd)
    self.assertTrue('--blackListFileName' in deeptools_cmd)
    self.assertTrue('bigwig' in deeptools_cmd)

  def test_run_plotFingerprint(self):
    deeptools_cmd = \
      run_plotFingerprint(\
        bam_files=[self.input_bam],
        output_raw_counts='out.raw.txt',
        output_matrics='out.metrics.txt',
        output_plot='out.plot.pdf',
        dry_run=True,
        blacklist_file=self.blacklist_file,
        thread=1)
    self.assertTrue(self.input_bam in deeptools_cmd)
    self.assertTrue(self.blacklist_file in deeptools_cmd)
    self.assertTrue('--blackListFileName' in deeptools_cmd)
    deeptools_cmd = \
      run_plotFingerprint(\
        bam_files=[self.input_bam],
        output_raw_counts='out.raw.txt',
        output_matrics='out.metrics.txt',
        output_plot='out.plot.pdf',
        dry_run=True,
        thread=1)
    self.assertTrue(self.input_bam in deeptools_cmd)
    self.assertFalse(self.blacklist_file in deeptools_cmd)
    self.assertFalse('--blackListFileName' in deeptools_cmd)

if __name__=='__main__':
  unittest.main()