import unittest
from igf_data.utils.tools.samtools_utils import _parse_samtools_stats_output

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


if __name__=='__main__':
  unittest.main()