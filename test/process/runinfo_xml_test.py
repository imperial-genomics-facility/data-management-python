import unittest
from igf_data.illumina.runinfo_xml import RunInfo_xml

class Hiseq4000RunInfo(unittest.TestCase):

  def setUp(self):
    r_file='doc/data/Illumina/RunInfo.xml'
    self.runinfo_data=RunInfo_xml(xml_file=r_file)

  def test_runInfo_wrong_root_tag(self):
    runinfo_data=self.runinfo_data
    with self.assertRaises(ValueError):
      runinfo_data.get_reads_stats(root_tag='NO_Read')

  def test_get_platform_number(self):
    runinfo_data=self.runinfo_data
    platform_number=runinfo_data.get_platform_number()
    self.assertEqual(platform_number, 'K00001')

  def test_get_reads_stats(self):
    runinfo_data=self.runinfo_data
    reads_stats=runinfo_data.get_reads_stats()
    self.assertEqual(len(reads_stats), 4)

    index_count=0
    read_count=0
    index_len=8
    read_len=151

    for read_id in reads_stats.keys():
       if reads_stats[read_id]['isindexedread'] == 'Y':
         index_count += 1
         index_cycle=reads_stats[read_id]['numcycles']
         self.assertEqual(int(index_cycle),index_len)

       elif reads_stats[read_id]['isindexedread'] == 'N':
         read_count += 1
         read_cycle=reads_stats[read_id]['numcycles']
         self.assertEqual(int(read_cycle), read_len)
     
       else:
         continue

    self.assertEqual(index_count,2)
    self.assertEqual(read_count,2)

  def test_get_flowcell_name(self):
    runinfo_data=self.runinfo_data
    flowcell_name=runinfo_data.get_flowcell_name()
    self.assertEqual(flowcell_name, 'HXXXXXXXX')


if __name__ == '__main__':
  unittest.main()
