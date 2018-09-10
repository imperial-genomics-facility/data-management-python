import unittest, os, subprocess,fnmatch
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.singlecell_seqrun.mergesinglecellfastq import MergeSingleCellFastq

class MergeSingleCellFastq_testA(unittest.TestCase):
  def setUp(self):
    try:
      self.samplesheet_file='data/singlecell_data/SampleSheet_merge_nextseq.csv'
      self.input_filelist='data/singlecell_data/singlecell_merge_file_list.txt'
      self.fastq_dir=get_temp_dir()
      with open(self.input_filelist,'r') as input_data:
        for file_path in input_data:
          file_path=file_path.strip()
          file_path=os.path.join(self.fastq_dir,file_path)
          file_dir=os.path.dirname(file_path)
          if not os.path.exists(file_dir):
            os.makedirs(file_dir,0o700)
          fastq_name=os.path.splitext(file_path)[0]                             # get .fastq file name 
          with open(fastq_name,'w') as fastq_reads:
            fastq_reads.write('@header\nATCG\n+\n####')                         # write fastq data
          subprocess.check_call(["gzip",fastq_name])                            # zip fastq files
    except:
      raise


  def tearDown(self):
    if os.path.exists(self.fastq_dir):
      remove_dir(self.fastq_dir)

  def test_merge_fastq_per_lane_per_sample(self):
    sc_data=MergeSingleCellFastq(fastq_dir=self.fastq_dir,
                                 samplesheet=self.samplesheet_file,
                                 platform_name='NEXTSEQ')
    sc_data.merge_fastq_per_lane_per_sample()                                   # merge single cell fastqs
    all_fastq_file=list()
    for root,dir,files in os.walk(self.fastq_dir):
      for file in files:
        if fnmatch.fnmatch(file,'*.fastq.gz'):
          all_fastq_file.append(file)
    self.assertEqual(len(all_fastq_file),24)
if __name__=='__main__':
  unittest.main()