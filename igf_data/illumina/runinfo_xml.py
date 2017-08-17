import re, os, warnings
from collections import defaultdict
from bs4 import BeautifulSoup

class RunInfo_xml:
  def __init__(self, xml_file):
    self.xml_file=xml_file
    self._read_xml()

  def get_reads_stats(self, root_tag='read', number_tag='number', tags=['isindexedread','numcycles']):
    '''
    Function for getting read and index stats from the RunInfo.xml file
    Output is a dictionary with the read number as the key
    '''
    reads_stats=defaultdict(lambda: defaultdict(dict))
    pattern=re.compile(number_tag, re.IGNORECASE)
    soup=self._soup

    match_count=0

    try:
      for r in soup.find_all(root_tag):
        for tag_name in tags:
          reads_stats[r[number_tag]][tag_name]=r[tag_name]
          match_count+=1

      if match_count==0:
        raise ValueError('no record found for {0} in file {1}'.format(root_tag, self.xml_file))

      return reads_stats
    except:
      raise 


  def get_platform_number(self):
    '''
    Function for fetching the instrument series number
    '''           
    series_number=None
    soup=self._soup
    series_number=soup.instrument.contents[0]

    if not series_number:
      raise ValueError('couldn\'t find tag {0}'.format('instrument'))

    return series_number
    
  def _read_xml(self):
    '''
    Internal function for reading the xml file using BS4
    '''
    xml_file=self.xml_file
    with open(xml_file, 'r') as fp:
      soup = BeautifulSoup(fp)
    self._soup = soup
