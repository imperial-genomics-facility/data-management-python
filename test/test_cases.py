import unittest
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml

class Hiseq4000SampleSheet(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.file=file
  def test_samplesheet(self):
    s1=SampleSheet(infile=self.file)

class Hiseq4000RunInfoXml(unittest.TestCase):
  def setUp(self):
    file='doc/data/Illumina/RunInfo.xml'
    self.file=file
  def test_runinfoxml(self):
    r1=RunInfo_xml(file)
