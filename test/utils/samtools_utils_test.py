import os,unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.tools.samtools_utils import _parse_samtools_stats_output,convert_bam_to_cram,filter_bam_file
from igf_data.utils.tools.samtools_utils import run_bam_flagstat,run_bam_idxstat,run_sort_bam,merge_multiple_bam,index_bam_or_cram

class Samtools_util_test1(unittest.TestCase):
  def test_parse_samtools_stats_output(self):
    metrics = _parse_samtools_stats_output(stats_file='data/samtools_metrics/test.stats.txt')
    self.assertTrue(isinstance(metrics,list))
    self.assertEqual(len(metrics),38)
    temp_list = list()
    for i in metrics:
      temp_list.extend(i.keys())
    self.assertTrue('SAMTOOLS_STATS_reads_mapped' in temp_list)
    for i in metrics:
      for key,val in i.items():
        if key=='SAMTOOLS_STATS_reads_mapped':
          self.assertEqual(int(val),160190290)

class Samtools_util_test2(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.samtools_exe = os.path.join(self.temp_dir,'samtools')
    with open (self.samtools_exe,'w') as fp:
      fp.write('1')

    self.input_bam = os.path.join(self.temp_dir,'input.bam')
    with open (self.input_bam,'w') as fp:
      fp.write('1')

    self.input_bai = os.path.join(self.temp_dir,'input.bam.bai')
    with open (self.input_bai,'w') as fp:
      fp.write('1')

    self.reference_file = os.path.join(self.temp_dir,'genome.fa')
    with open (self.reference_file,'w') as fp:
      fp.write('1')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_convert_bam_to_cram(self):
    samtools_cmd = \
      convert_bam_to_cram(\
        samtools_exe=self.samtools_exe,
        bam_file=self.input_bam,
        reference_file=self.reference_file,
        cram_path='data/test.cram',
        threads=1,
        force=False,
        dry_run=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('view' in samtools_cmd)
    self.assertTrue('-T{0}'.format(self.reference_file) in samtools_cmd)
    self.assertTrue('-@1' in samtools_cmd)
    self.assertTrue('-C' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)

  def test_filter_bam_file(self):
    samtools_cmd = \
      filter_bam_file(\
        samtools_exe=self.samtools_exe,
        input_bam=self.input_bam,
        output_bam='data/test.bam',
        samFlagInclude=4,
        samFlagExclude=1804,
        threads=1,
        mapq_threshold=20,
        cram_out=False,
        index_output=True,
        dry_run=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('view' in samtools_cmd)
    self.assertTrue('-f4' in samtools_cmd)
    self.assertTrue('-F1804' in samtools_cmd)
    self.assertTrue('-@1' in samtools_cmd)
    self.assertTrue('-q20' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)
    with self.assertRaises(ValueError):
      filter_bam_file(\
        samtools_exe=self.samtools_exe,
        input_bam=self.input_bam,
        output_bam='data/test.bam',
        samFlagInclude=4,
        samFlagExclude=1804,
        threads=1,
        mapq_threshold=20,
        cram_out=True,
        index_output=True,
        dry_run=True)

  def test_run_bam_flagstat(self):
    samtools_cmd = \
      run_bam_flagstat(\
        samtools_exe=self.samtools_exe,
        bam_file=self.input_bam,
        output_dir='data',
        threads=1,
        force=False,
        output_prefix='test',
        dry_run=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('flagstat' in samtools_cmd)
    self.assertTrue('-@1' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)

  def test_run_bam_idxstat(self):
    samtools_cmd = \
      run_bam_idxstat(\
        samtools_exe=self.samtools_exe,
        bam_file=self.input_bam,
        output_dir='data',
        output_prefix='test',
        force=False,
        dry_run=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('idxstats' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)

  def test_run_sort_bam(self):
    samtools_cmd = \
      run_sort_bam(\
        samtools_exe=self.samtools_exe,
        input_bam_path=self.input_bam,
        output_bam_path='data/test.bam',
        sort_by_name=False,
        threads=1,
        force=False,
        dry_run=True,
        cram_out=False)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('sort' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)
    self.assertTrue('--output-fmt BAM' in samtools_cmd)
    self.assertFalse('-n' in samtools_cmd)
    self.assertTrue('-o' in samtools_cmd)
    self.assertTrue('-@1' in samtools_cmd)
    samtools_cmd = \
      run_sort_bam(\
        samtools_exe=self.samtools_exe,
        input_bam_path=self.input_bam,
        output_bam_path='data/test.bam',
        sort_by_name=True,
        threads=1,
        force=False,
        dry_run=True,
        cram_out=False)
    self.assertTrue('--output-fmt BAM' in samtools_cmd)
    self.assertTrue('-n' in samtools_cmd)
    samtools_cmd = \
      run_sort_bam(\
        samtools_exe=self.samtools_exe,
        input_bam_path=self.input_bam,
        output_bam_path='data/test.cram',
        sort_by_name=False,
        threads=1,
        force=False,
        dry_run=True,
        cram_out=True)
    self.assertTrue('--output-fmt CRAM' in samtools_cmd)

  def test_merge_multiple_bam(self):
    with open(os.path.join(self.temp_dir,'bam_list'),'w') as fp:
      fp.write(self.input_bam)

    samtools_cmd = merge_multiple_bam(\
      samtools_exe=self.samtools_exe,
      input_bam_list=os.path.join(self.temp_dir,'bam_list'),
      output_bam_path='data/test.bam',
      sorted_by_name=False,
      threads=1,
      force=False,
      dry_run=True,
      index_output=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('merge' in samtools_cmd)
    self.assertTrue(os.path.join(self.temp_dir,'bam_list') in samtools_cmd)

  def test_index_bam_or_cram(self):
    samtools_cmd = \
      index_bam_or_cram(\
        samtools_exe=self.samtools_exe,
        input_path=self.input_bam,
        threads=1,
        dry_run=True)
    self.assertTrue(self.samtools_exe in samtools_cmd)
    self.assertTrue('index' in samtools_cmd)
    self.assertTrue('-@1' in samtools_cmd)
    self.assertTrue(self.input_bam in samtools_cmd)

if __name__=='__main__':
  unittest.main()