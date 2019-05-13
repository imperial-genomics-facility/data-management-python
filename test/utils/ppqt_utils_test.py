import unittest
from igf_data.utils.tools.ppqt_utils import Ppqt_tools

class Ppqt_util_test1(unittest.TestCase):
  def test_parse_spp_output(self):
    ppqt = Ppqt_tools(rscript_path='blah',ppqt_exe='blah')
    metrics = ppqt._parse_spp_output(spp_file='data/ppqt_metrics/test.spp.out')
    print(metrics)
    self.assertTrue(isinstance(metrics,list))
    self.assertEqual(len(metrics[0]),11)
    self.assertTrue('PPQT_Normalized_SCC_NSC' in metrics[0])
    self.assertEqual(float(metrics[0].get('PPQT_Normalized_SCC_NSC')),1.17548)

if __name__=='__main__':
  unittest.main()