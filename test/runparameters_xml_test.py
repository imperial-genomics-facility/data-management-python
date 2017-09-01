import unittest
from igf_data.illumina.runparameters_xml import RunParameter_xml

class Hiseq4000RunParam(unittest.TestCase):
  def setUp(self):
    xml_file='data/seqrun_dir/seqrun1/runParameters.xml'
    self.runparameter_data=RunParameter_xml(xml_file)
  
  def test_get_hiseq_flowcell(self):
    runparameter_data=self.runparameter_data
    flowcell=runparameter_data.get_hiseq_flowcell()
    self.assertEqual(flowcell, 'HiSeq 3000/4000 PE')

if __name__ == '__main__':
  unittest.main()
