import re, os
from collections import defaultdict
import xml.etree.ElementTree as ET

class RunInfo_xml:
  def __init__(self, xml_file):
    self.xml_file=xml_file
    self._read_xml()

  def get_reads_stats(self):
    '''
    Function for getting read and index stats from the RunInfo.xml file
    Output is a dictionary with the read number as the key
    '''
    tree = self._tree
    root = tree.getroot()
    reads_stats=defaultdict()
    pattern=re.compile('Number', re.IGNORECASE)

    for reads in root.iter('Read'):
      read_number=0
      for attrib in reads.attrib.keys():
        if re.search(pattern, attrib):
          read_number=reads.attrib[attrib]
      if read_number:
        reads_stats[read_number]=reads
    return reads_stats
           

  def _read_xml(self):
    '''
    Internal function for reading the xml file is a tree
    '''
    xml_file=self.xml_file
    tree = ET.parse(xml_file)
    self._tree = tree
