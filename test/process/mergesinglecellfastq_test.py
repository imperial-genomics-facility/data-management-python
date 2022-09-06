import sys
sys.path.append("/home/vmuser/github/data-management-python")
import unittest, re, os, subprocess, fnmatch
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.singlecell_seqrun.mergesinglecellfastq import MergeSingleCellFastq

class MergeSingleCellFastq_testA(unittest.TestCase):
  def setUp(self):
    try:
      self.samplesheet_file = 'data/singlecell_data/SampleSheet_merge_nextseq.csv'
      self.input_filelist = 'data/singlecell_data/singlecell_merge_file_list.txt'
      self.fastq_dir = get_temp_dir()
      with open(self.input_filelist,'r') as input_data:
        for file_path in input_data:
          file_path = file_path.strip()
          file_path = os.path.join(self.fastq_dir,file_path)
          file_dir = os.path.dirname(file_path)
          if not os.path.exists(file_dir):
            os.makedirs(file_dir, 0o700)
          fastq_name = os.path.splitext(file_path)[0]                             # get .fastq file name 
          with open(fastq_name,'w') as fastq_reads:
            fastq_reads.write('@header\nATCG\n+\n####')                         # write fastq data
          subprocess.check_call(["gzip",fastq_name])                            # zip fastq files
    except:
      raise

  def tearDown(self):
    if os.path.exists(self.fastq_dir):
      remove_dir(self.fastq_dir)

  def test_merge_fastq_per_lane_per_sample(self):
    sc_data = \
      MergeSingleCellFastq(
        fastq_dir=self.fastq_dir,
        samplesheet=self.samplesheet_file,
        platform_name='NEXTSEQ')
    sc_data.merge_fastq_per_lane_per_sample()                                   # merge single cell fastqs
    all_fastq_file = list()
    for root, dir, files in os.walk(self.fastq_dir):
      for file in files:
        if fnmatch.fnmatch(file, '*.fastq.gz'):
          all_fastq_file.append(file)
    self.assertEqual(len(all_fastq_file), 24)


class MergeSingleCellFastq_testB(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    samplesheet_data = """
    [Header]
    IEMFileVersion,4,,,,,,,,
    Application,NextSeq2000 FASTQ Only,,,,,,,,
    [Reads]
    151,,,,,,,,,
    151,,,,,,,,,
    [Settings]
    CreateFastqForIndexReads,1
    MinimumTrimmedReadLength,8
    FastqCompressionFormat,gzip
    MaskShortReads,8
    OverrideCycles,Y150N1;I8N2;N10;Y150N1
    [Data]
    Sample_ID,Sample_Name,Sample_Plate,Sample_Well,I7_Index_ID,index,I5_Index_ID,index2,Sample_Project,Description,Original_index,Original_Sample_ID,Original_Sample_Name
    IGF1_1,A01_1,,,SI-GA-C2,CCTAGACC,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_2,A01_2,,,SI-GA-C2,ATCTCTGT,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_3,A01_3,,,SI-GA-C2,TAGCTCTA,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF1_4,A01_4,,,SI-GA-C2,GGAGAGAG,,,IGFQ1,10X,SI-GA-C2,IGF1,A01
    IGF2_1,A02_1,,,SI-GA-D2,TAACAAGG,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_2,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_3,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF2_4,A02_2,,,SI-GA-D2,GGTTCCTC,,,IGFQ1,10X,SI-GA-D2,IGF2,A02
    IGF3,A03,,,Iaaa,AATCACGA,,,IGFQ1,,,,
    """
    pattern1 = re.compile(r'\n\s+')
    pattern2 = re.compile(r'^\n+')
    samplesheet_data = re.sub(pattern1, '\n', samplesheet_data)
    samplesheet_data = re.sub(pattern2, '', samplesheet_data)
    self.samplesheet_file = \
      os.path.join(self.temp_dir, 'samplesheet.csv')
    with open(self.samplesheet_file, 'w') as fh:
        fh.write(samplesheet_data)
    fastq_list = [
      'IGFQ1/IGF1_1_S1_L001_I1_001.fastq',
      'IGFQ1/IGF1_1_S1_L001_I2_001.fastq',
      'IGFQ1/IGF1_1_S1_L001_R1_001.fastq',
      'IGFQ1/IGF1_1_S1_L001_R2_001.fastq',
      'IGFQ1/IGF2_1_S2_L001_I1_001.fastq',
      'IGFQ1/IGF2_1_S2_L001_I2_001.fastq',
      'IGFQ1/IGF2_1_S2_L001_R1_001.fastq',
      'IGFQ1/IGF2_1_S2_L001_R2_001.fastq',
      'IGFQ1/IGF1_2_S3_L001_I1_001.fastq',
      'IGFQ1/IGF1_2_S3_L001_I2_001.fastq',
      'IGFQ1/IGF1_2_S3_L001_R1_001.fastq',
      'IGFQ1/IGF1_2_S3_L001_R2_001.fastq',
      'IGFQ1/IGF2_2_S4_L001_I1_001.fastq',
      'IGFQ1/IGF2_2_S4_L001_I2_001.fastq',
      'IGFQ1/IGF2_2_S4_L001_R1_001.fastq',
      'IGFQ1/IGF2_2_S4_L001_R2_001.fastq',
      'IGFQ1/IGF1_3_S5_L001_I1_001.fastq',
      'IGFQ1/IGF1_3_S5_L001_I2_001.fastq',
      'IGFQ1/IGF1_3_S5_L001_R1_001.fastq',
      'IGFQ1/IGF1_3_S5_L001_R2_001.fastq',
      'IGFQ1/IGF2_3_S6_L001_I1_001.fastq',
      'IGFQ1/IGF2_3_S6_L001_I2_001.fastq',
      'IGFQ1/IGF2_3_S6_L001_R1_001.fastq',
      'IGFQ1/IGF2_3_S6_L001_R2_001.fastq',
      'IGFQ1/IGF1_4_S7_L001_I1_001.fastq',
      'IGFQ1/IGF1_4_S7_L001_I2_001.fastq',
      'IGFQ1/IGF1_4_S7_L001_R1_001.fastq',
      'IGFQ1/IGF1_4_S7_L001_R2_001.fastq',
      'IGFQ1/IGF2_4_S8_L001_I1_001.fastq',
      'IGFQ1/IGF2_4_S8_L001_I2_001.fastq',
      'IGFQ1/IGF2_4_S8_L001_R1_001.fastq',
      'IGFQ1/IGF2_4_S8_L001_R2_001.fastq',
      'IGFQ1/IGF3_S9_L001_R1_001.fastq',
      'IGFQ1/IGF3_S9_L001_R2_001.fastq',
      'IGFQ1/IGF3_S9_L001_I1_001.fastq',
      'IGFQ1/IGF3_S9_L001_I2_001.fastq']
    for f in fastq_list:
      fastq_path = \
        os.path.join(self.temp_dir, 'fastq', f)
      os.makedirs(os.path.dirname(fastq_path), exist_ok=True)
      with open(fastq_path,'w') as fq:
        fq.write('@header\nATCG\n+\n####\n')
      subprocess.check_call(["gzip", fastq_path])
    self.fastq_dir = \
      os.path.join(
        self.temp_dir,
        'fastq',
        'IGFQ1')

  def tearDown(self):
    remove_dir(self.temp_dir)

  def test_merge_fastq_per_lane_per_sample_for_bclconvert(self):
    sc_data = \
      MergeSingleCellFastq(
        fastq_dir=os.path.join(self.temp_dir, 'fastq'),
        samplesheet=self.samplesheet_file,
        platform_name='NEXTSEQ2000',
        use_bclconvert_settings=True,
        pseudo_lane_list=('1',))
    sc_data.\
      merge_fastq_per_lane_per_sample()
    all_fastq_file = list()
    for root, dir, files in os.walk(self.fastq_dir):
      for file in files:
        if fnmatch.fnmatch(file, '*.fastq.gz'):
          all_fastq_file.append(os.path.join(root, file))
    self.assertEqual(len(all_fastq_file), 12)
    self.assertTrue(
      os.path.join(self.temp_dir, 'fastq', 'IGFQ1', 'IGF1_S1_L001_R1_001.fastq.gz') in all_fastq_file)
    fastq_file = \
      os.path.join(self.temp_dir, 'fastq', 'IGFQ1', 'IGF1_S1_L001_R1_001.fastq.gz')
    # fastq_lines = \
    #   subprocess.check_output(f"zcat {all_fastq_file[0]}|wc -l", shell=True)
    fastq_lines = \
      subprocess.check_output(f"zcat {fastq_file}|wc -l", shell=True)
    self.assertEqual(fastq_lines.decode().strip(), '16')
    self.assertTrue(
      os.path.join(self.temp_dir, 'fastq', 'IGFQ1', 'IGF3_S9_L001_R1_001.fastq.gz') in all_fastq_file)
    fastq_lines = \
      subprocess.check_output(f"zcat {os.path.join(self.temp_dir, 'fastq', 'IGFQ1', 'IGF3_S9_L001_R1_001.fastq.gz')}|wc -l", shell=True)
    self.assertEqual(fastq_lines.decode().strip(), '4')

if __name__=='__main__':
  unittest.main()