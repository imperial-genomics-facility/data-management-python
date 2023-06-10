from bs4 import BeautifulSoup

class RunParameter_xml:
  '''
  A class for reading runparameters xml file from Illumina sequencing runs

  :param xml_file: A runparameters xml file
  '''
  def __init__(self, xml_file):
    self.xml_file = xml_file
    self._read_xml()


  def _read_xml(self):
    '''
    Internal function for reading the xml file using BS4
    '''
    try:
      xml_file = self.xml_file
      with open(xml_file, 'r') as fp:
        soup = BeautifulSoup(fp, features="html5lib")
      self._soup = soup
    except Exception as e:
      raise ValueError(
              'Failed to parse xml file {0}, error {1}'.\
                format(self.xml_file, e))


  def get_nova_workflow_type(self):
    try:
      soup = self._soup
      workflowtype = None
      if soup.workflowtype:
        workflowtype = \
          soup.workflowtype.contents[0]
      return workflowtype
    except Exception as e:
      raise ValueError('Failed to get NovaSeq workflow type')


  def get_novaseq_flowcell(self, workflow_types=('NovaSeqXp', 'NovaSeqStandard')):
    try:
      soup = self._soup
      flowcell_id = None
      workflowtype = self.get_nova_workflow_type()
      if workflowtype is None or \
         workflowtype not in workflow_types:
        raise ValueError(
                'Missing NovaSeq workflow type: {0}'.\
                  format(workflowtype))
      if soup.rfidsinfo and \
         soup.rfidsinfo.flowcellserialbarcode:
        flowcell_id = \
          soup.rfidsinfo.flowcellmode.contents[0]
      if flowcell_id is None:
        raise ValueError(
                'Missing NovaSeq flowcell id, file: {0}'.\
                  format(self.xml_file))
      return flowcell_id
    except Exception as e:
      raise ValueError(
              'Failed to get NovaSeq flowcell id, error: {0}'.format(e))


  def get_novaseq_flowcell_mode(self, workflow_types=('NovaSeqXp', 'NovaSeqStandard')):
    try:
      soup = self._soup
      flowcell_mode = None
      workflowtype = self.get_nova_workflow_type()
      if workflowtype is None or \
         workflowtype not in workflow_types:
          raise ValueError(
                  'Missing NovaSeq workflow type: {0}'.\
                    format(workflowtype))
      if soup.rfidsinfo and \
         soup.rfidsinfo.flowcellmode:
        flowcell_mode = \
          soup.rfidsinfo.flowcellmode.contents[0]
      if flowcell_mode is None:
        raise ValueError(
                'Missing NovaSeq flowcell mode, file: {0}'.\
                  format(self.xml_file))
      return flowcell_mode
    except Exception as e:
      raise ValueError(
              'Failed to get NovaSeq flowcell mode, error: {0}'.format(e))


  def get_hiseq_flowcell(self):
    '''
    A method for fetching flowcell details for hiseq run

    :returns: Flowcell info or None (for MiSeq, NextSeq or NovaSeq runs)
    '''
    try:
      soup = self._soup
      if soup.flowcell:
        flowcell = soup.flowcell.contents[0]
      else:
        flowcell = None
      return flowcell
    except Exception as e:
      raise ValueError(
              'Failed to get flowcell for hiseq, error: {0}'.\
                format(e))



