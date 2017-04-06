import unittest, re
from igf_data.illumina.basesMask import BasesMask

class BasesMask_testA(unittest.TestCase):

  def setUp(self):
    r_file='doc/data/Illumina/RunInfo.xml'
    s_file='doc/data/SampleSheet/HiSeq4000/SingleLaneSampleSheet.csv'
    self.bases_mask_object=BasesMask(samplesheet_file=s_file, runinfo_file=r_file, read_offset=1, index_offset=0 )
 
  def test_calculate_bases_mask_8_8(self):
    bases_mask_data=self.bases_mask_object
    bases_mask=bases_mask_data.calculate_bases_mask()
    pattern=re.compile('^y150n1,i8,i8,y150n1$', re.IGNORECASE)
    self.assertRegexpMatches(bases_mask, pattern)

class BasesMask_testB(unittest.TestCase):

  def setUp(self):   
    r_file='doc/data/Illumina/RunInfo.xml'
    s_file='doc/data/SampleSheet/HiSeq4000/SingleLaneSampleSheet2.csv'
    self.bases_mask_object=BasesMask(samplesheet_file=s_file, runinfo_file=r_file, read_offset=1, index_offset=0 )
 
  def test_calculate_bases_mask_8_N(self):
    bases_mask_data=self.bases_mask_object
    bases_mask=bases_mask_data.calculate_bases_mask()
    pattern=re.compile('^y150n1,i8,n8,y150n1$', re.IGNORECASE)
    self.assertRegexpMatches(bases_mask, pattern)

class BasesMask_testC(unittest.TestCase):

  def setUp(self):
    r_file='doc/data/Illumina/RunInfo.xml'
    s_file='doc/data/SampleSheet/HiSeq4000/SingleLaneSampleSheet3.csv'
    self.bases_mask_object=BasesMask(samplesheet_file=s_file, runinfo_file=r_file, read_offset=1, index_offset=0 )

  def test_calculate_bases_mask_6_N(self):
    bases_mask_data=self.bases_mask_object
    bases_mask=bases_mask_data.calculate_bases_mask()
    pattern=re.compile('^y150n1,i6n2,n8,y150n1$', re.IGNORECASE)
    self.assertRegexpMatches(bases_mask, pattern)


if __name__=='__main__':
  unittest.main()
