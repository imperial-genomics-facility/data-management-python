import os, unittest
from igf_data.utils.tools.picard_util import Picard_tools

class Picard_util_test1(unittest.TestCase):
  def test_get_param_for_picard_command1(self):
    pa=Picard_tools(java_exe='/path/java',
                    picard_jar='/path/picard.jar',
                    input_files=['/path/input.bam'],
                    output_dir='/path/output',
                    ref_fasta='/path/genome.fa'
                    )
    param_dict, output_list,metrics_file_list=\
      pa._get_param_for_picard_command(command_name='CollectGcBiasMetrics')
    self.assertTrue('/path/output/input.CollectGcBiasMetrics.txt' in output_list)
    self.assertTrue('/path/output/input.CollectGcBiasMetrics.summary.txt' in metrics_file_list)
    self.assertTrue('O' in param_dict[0].keys())
    self.assertEqual('/path/output/input.CollectGcBiasMetrics.pdf',param_dict[0]['CHART'])

class Picard_util_test2(unittest.TestCase):
  def test_parse_picard_metrics1(self):
    metrics = Picard_tools._parse_picard_metrics('CollectAlignmentSummaryMetrics',['data/picard_metrics/test.CollectAlignmentSummaryMetrics.txt'])
    self.assertTrue('CollectAlignmentSummaryMetrics_CATEGORY' in metrics[0].keys())
    self.assertEqual(metrics[0].get('CollectAlignmentSummaryMetrics_CATEGORY'),'PAIR')


  def test_parse_picard_metrics2(self):
    metrics = Picard_tools._parse_picard_metrics('CollectGcBiasMetrics',['data/picard_metrics/test.CollectGcBiasMetrics.summary.txt'])
    self.assertTrue('CollectGcBiasMetrics_GC_NC_0_19' in metrics[0].keys())

  
  def test_parse_picard_metrics3(self):
    metrics = Picard_tools._parse_picard_metrics('CollectRnaSeqMetrics',['data/picard_metrics/test.CollectRnaSeqMetrics.txt'])
    self.assertTrue('CollectRnaSeqMetrics_MEDIAN_5PRIME_BIAS' in metrics[0].keys())
    self.assertEqual(float(metrics[0].get('CollectRnaSeqMetrics_MEDIAN_5PRIME_BIAS')),0.477757)


  def test_parse_picard_metrics4(self):
    metrics = Picard_tools._parse_picard_metrics('MarkDuplicates',['data/picard_metrics/test.MarkDuplicates.summary.txt'])
    self.assertTrue('MarkDuplicates_READ_PAIR_DUPLICATES' in metrics[0].keys())
    self.assertEqual(float(metrics[0].get('MarkDuplicates_PERCENT_DUPLICATION')),0.63392)


if __name__=='__main__':
  unittest.main()