import os, unittest
from igf_data.utils.tools.picard_util import Picard_tools

class Picard_util_test1(unittest.TestCase):
  def test_get_param_for_picard_command(self):
    pa=Picard_tools(java_exe='/path/java',
                    picard_jar='/path/picard.jar',
                    input_file='/path/input.bam',
                    output_dir='/path/output',
                    ref_fasta='/path/genome.fa'
                    )
    param_dict, output_list=\
      pa._get_param_for_picard_command(command_name='CollectGcBiasMetrics')
    self.assertTrue('/path/output/input.bam.CollectGcBiasMetrics.txt' in output_list)
    self.assertTrue('O' in param_dict.keys())
    self.assertEqual('/path/output/input.bam.CollectGcBiasMetrics.pdf',param_dict['CHART'])

if __name__=='__main__':
  unittest.main()