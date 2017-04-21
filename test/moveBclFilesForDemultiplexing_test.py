import unittest, os
from shutil import rmtree
from tempfile import mkdtemp
from igf_data.process.moveBclFilesForDemultiplexing import moveBclFilesForDemultiplexing

class Hiseq4000RunInfo(unittest.TestCase):
  def setUp(self):
    self.input_dir  = mkdtemp()
    self.output_dir = mkdtemp()
    sample_sheet    = 'doc/data/SampleSheet/HiSeq4000/SingleLaneSampleSheet.csv'
    run_info_xml    = 'doc/data/Illumina/RunInfo.xml'

    # setup move_file class
    self.move_file=moveBclFilesForDemultiplexing(input_dir=self.input_dir, output_dir=self.output_dir, samplesheet=sample_sheet, run_info_xml=run_info_xml)

  def tearDown(self):
    rmtree(self.input_dir)
    rmtree(self.output_dir)  

  def test_generate_platform_specific_list(self):
    move_file=self.move_file
    bcl_file_list=move_file._generate_platform_specific_list()
    bcl_file_list.sort()
    test_list=['Data/Intensities/s.locs','Data/Intensities/BaseCalls/L003','RunInfo.xml','runParameters.xml']
    test_list.sort()
    self.assertListEqual(bcl_file_list, test_list)

  def test_copy_bcl_files(self):
    # create dir structure
    os.makedirs(os.path.join(self.input_dir,'Data/Intensities/'))
    open(os.path.join(self.input_dir,'Data/Intensities/s.locs'),'a').close()
    os.makedirs(os.path.join(self.input_dir,'Data/Intensities/BaseCalls/L003'))
    open(os.path.join(self.input_dir,'RunInfo.xml'),'a').close()
    open(os.path.join(self.input_dir,'runParameters.xml'),'a').close()
  
    # move files
    move_file=self.move_file
    move_file.copy_bcl_files()

    # run tests
    self.assertTrue(os.path.exists(os.path.join(self.output_dir,'RunInfo.xml')),1)
    self.assertTrue(os.path.exists(os.path.join(self.output_dir,'runParameters.xml')),1)
    self.assertTrue(os.path.exists(os.path.join(self.output_dir,'Data/Intensities/s.locs')),1)
    self.assertTrue(os.path.exists(os.path.join(self.output_dir,'Data/Intensities/BaseCalls/L003')),1)
if __name__=='__main__':
  unittest.main()
