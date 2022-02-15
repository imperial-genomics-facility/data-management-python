import re, os, warnings
from collections import defaultdict
from bs4 import BeautifulSoup

class RunInfo_xml:
  '''
  A class for reading runinfo xml file from illumina sequencing runs

  :param xml_file: A runinfo xml file
  '''
  def __init__(self, xml_file):
    self.xml_file=xml_file
    self._read_xml()

  def get_reads_stats(self, root_tag='read', number_tag='number',
                      tags=('isindexedread','numcycles')):
    '''
    A method for getting read and index stats from the RunInfo.xml file

    :param root_tag: Root tag for xml file, default read
    :param number_tag: Number tag for xml file, default number
    :param tags: List of tags for xml lookup, default ['isindexedread','numcycles']
    :returns: A dictionary with the read number as the key
    '''
    try:
      tags = list(tags)
      reads_stats=defaultdict(lambda: defaultdict(dict))
      soup=self._soup
      match_count=0
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

  def get_flowcell_name(self):
    '''
    A mthod for accessing flowcell name from the runinfo xml file
    '''
    flowcell=None
    flowcell=self._soup.flowcell.contents[0]

    if not flowcell:
      raise ValueError('couldn\'t find tag {0}'.format('flowcell'))
    return flowcell

  def _read_xml(self):
    '''
    Internal function for reading the xml file using BS4
    '''
    xml_file=self.xml_file
    with open(xml_file, 'r') as fp:
      soup = BeautifulSoup(fp, "html5lib")
    self._soup = soup

  def get_lane_count(self, lanecount_tag='lanecount'):
    try:
      lane_counts = \
        self._soup.flowcelllayout.get("lanecount")
      if isinstance(lane_counts, str):
        lane_counts = int(lane_counts)
      if lane_counts is None:
        lane_counts = 0
      return lane_counts
    except Exception as e:
      raise ValueError("Failed to get lane count, error: {0}".format(e))

  def get_tiles_list(self, tag="tile"):
    try:
      tiles = list()
      for r in self._soup.find_all(tag):
        tiles.append(r.contents[0])
      return tiles
    except Exception as e:
      raise ValueError("Failed to get tiles list, error: {0}".format(e))