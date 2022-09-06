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
                      tags=('isindexedread','numcycles', 'isreversecomplement')):
    '''
    A method for getting read and index stats from the RunInfo.xml file

    :param root_tag: Root tag for xml file, default read
    :param number_tag: Number tag for xml file, default number
    :param tags: List of tags for xml lookup, default ['isindexedread', 'numcycles', 'isreversecomplement']
    :returns: A dictionary with the read number as the key
    '''
    try:
      tags = list(tags)
      reads_stats = \
        defaultdict(lambda: defaultdict(dict))
      soup = self._soup
      match_count = 0
      for r in soup.find_all(root_tag):
        for tag_name in tags:
          if tag_name in r.attrs:
            reads_stats[r[number_tag]][tag_name] = r[tag_name]
          else:
            reads_stats[r[number_tag]][tag_name] = None
          match_count += 1
      if match_count == 0:
        raise ValueError(f'no record found for {root_tag} in file {self.xml_file}')
      return reads_stats
    except Exception as e:
      raise ValueError(f"Failed to get reads stats, error: {e}")


  def get_platform_number(self):
    '''
    Function for fetching the instrument series number
    '''
    try:
      series_number = None
      soup = self._soup
      series_number = soup.instrument.contents[0]
      if not series_number:
        raise ValueError('could not find tag instrument')
      return series_number
    except Exception as e:
      raise ValueError(f"Failed to get platform number, error: {e}")


  def get_flowcell_name(self):
    '''
    A method for accessing flowcell name from the runinfo xml file
    '''
    try:
      flowcell = None
      flowcell = self._soup.flowcell.contents[0]
      if not flowcell:
        raise ValueError('could not find tag flowcell')
      return flowcell
    except Exception as e:
      raise ValueError(f"Failed to get flowcell name, error: {e}")


  def _read_xml(self):
    '''
    Internal function for reading the xml file using BS4
    '''
    try:
      xml_file = self.xml_file
      with open(xml_file, 'r') as fp:
        soup = BeautifulSoup(fp, "html5lib")
      self._soup = soup
    except Exception as e:
      raise ValueError(f"Failed to read xml file {xml_file}, error: {e}")


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
      raise ValueError(f"Failed to get lane count, error: {e}")


  def get_tiles_list(self, tag="tile"):
    try:
      tiles = list()
      for r in self._soup.find_all(tag):
        tiles.append(r.contents[0])
      return tiles
    except Exception as e:
      raise ValueError(f"Failed to get tiles list, error: {e}")