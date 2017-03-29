import os,argparse
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml

class BasesMask:
  def __init__(self, samplesheet_file, runinfo_file, read_offset, index_offset):
    self.samplesheet_file = samplesheet_file
    self.runinfo_file     = runinfo_file
    self.read_offset      = read_offset
    self.index_offset     = index_offset

  def calculate_bases_mask(self):
    '''
    Function for bases mask value calculation
    '''

    samplesheet_file = self.samplesheet_file
    runinfo_file     = self.runinfo_file
    read_offset      = self.read_offset
    index_offset     = self.index_offset

    samplesheet_data=SampleSheet(infile=samplesheet_file)
    index_length_stats=samplesheet_data.get_index_count()

    index_length_list=list()

    for index_name in index_length_stats.keys():
      index_type=len(index_length_stats[index_name].keys())

      # check if all the index seq are similar or not
      if index_type > 1: raise ValueError('column {0} has variable lengths'.format( index_field ))
  
      # adding all non zero index lengths
      index_length=index_length_stats[index_name].keys()[0]
      if index_length > 0: 
        index_length_list.append(index_length)

    if len(set(index_length_list)) > 1: raise ValueError('index lengths are not same in samplesheet {0}'.format( samplesheet_file ))

    # count non zero indexes in the samplesheet
    samplesheet_index_count = len(index_length_list)  

    # get the allowed index length from samplesheet
    allowed_index_length=list(set(index_length_list))[0]
    
    runinfo_data=RunInfo_xml(xml_file=runinfo_file)
    runinfo_reads_stats=runinfo_data.get_reads_stats()

    read_count=0
    index_count=0

    bases_mask_list=list()

    for read_id in (runinfo_reads_stats.keys()):
      runinfo_read_length = int(runinfo_reads_stats[read_id]['NumCycles'])
    
      # index 
      if runinfo_reads_stats[read_id]['IsIndexedRead'] == 'Y':
        index_count += 1
        real_index_count = int(runinfo_read_length - index_offset)
      
        # compare index length with samplesheet
        if real_index_count != allowed_index_length: 
          raise ValueError('Index length not matching in {0} and {1}, with offset {2}'.format(samplesheet_file,runinfo_file, index_offset))

        # calculate base mask for index
        if ( index_count > samplesheet_index_count ):

          # mask everything if this index is not required for demultiplexing  
          mask = 'n{0}'.format(runinfo_read_length)
        else:

          # format index
          mask = 'i{0}n{1}'.format(real_index_count, index_offset) if index_offset else 'i{0}'.format(real_index_count)
        bases_mask_list.append(mask)

      # read
      elif runinfo_reads_stats[read_id]['IsIndexedRead'] == 'N':
        read_count += 1
        real_read_count = runinfo_read_length - read_offset
   
        # calculate base mask for index
        mask = 'y{0}n{1}'.format(real_read_count, read_offset) if read_offset else 'y{0}'.format(real_read_count)     
        bases_mask_list.append(mask)  
      # something else
      else:
        raise ValueError('file {0} has incorrect index information'.format(runinfo_file))
    
    bases_mask_format=','.join(bases_mask_list)
    return bases_mask_format

