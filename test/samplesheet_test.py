import unittest
from igf_data.illumina.samplesheet import SampleSheet

class Hiseq4000SampleSheet(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.file=file
  def test_samplesheet(self):
    s1=SampleSheet(infile=self.file)

