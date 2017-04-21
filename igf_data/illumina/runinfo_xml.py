import re, os, warnings
from collections import defaultdict
import xml.etree.ElementTree as ET

class RunInfo_xml:
  def __init__(self, xml_file):
    self.xml_file=xml_file
    self._read_xml()

  def get_reads_stats(self, root_tag='Read', number_tag='Number'):
    '''
    Function for getting read and index stats from the RunInfo.xml file
    Output is a dictionary with the read number as the key
    '''
    tree = self._tree
    root = tree.getroot()
    reads_stats=defaultdict(lambda: defaultdict(dict))
    pattern=re.compile(number_tag, re.IGNORECASE)

    match_count=0

    for reads in root.iter(root_tag):
      match_count += 1
      read_numbers=[reads.attrib[attrib] for attrib in reads.attrib.keys() if re.search(pattern, attrib)]
      if len(read_numbers) != 1:
        raise ValueError('couldn\'t identify readnumber for file {0} with root tag {1} and number tag {2}'.format(self.xml_file, root_tag, number_tag))

      if read_numbers[0]:
        for k,v in reads.items():
          reads_stats[read_numbers[0]][k]=v
    
    if not match_count:
      raise ValueError('no record found for {0} in file {1}'.format(root_tag, self.xml_file))
  
    return reads_stats
    
  def get_platform_number(self, tag='Instrument'):
    '''
    Function for fetching the instrument series number
    '''           
    tree = self._tree
    root = tree.getroot()
    match_count=0
    series_number=''

    for series in root.iter(tag):
      match_count += 1  
      series_number=series.text
   
    if not series_number:
      raise ValueError('couldn\'t find tag {0}'.format(tag))

    if match_count > 1:
      raise ValueError('tag {0} present more than once'.format(tag))

    return series_number
    
  def _read_xml(self):
    '''
    Internal function for reading the xml file is a tree
    '''
    xml_file=self.xml_file
    tree = ET.parse(xml_file)
    self._tree = tree
