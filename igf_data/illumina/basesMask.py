import os,argparse
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml

class BasesMask:
  '''
  A class for bases mask value calculation for demultiplexing of sequencing runs

  :param samplesheet_file: A samplesheet file containing sample index barcodes
  :param runinfo_file: A runinfo xml file from sequencing run
  :param read_offset: Read offset value in bp
  :param index_offset: Index offset value in bp
  '''
  def __init__(self, samplesheet_file, runinfo_file, read_offset, index_offset):
    self.samplesheet_file = samplesheet_file
    self.runinfo_file     = runinfo_file
    self.read_offset      = read_offset
    self.index_offset     = index_offset

  def calculate_bases_mask(self, numcycle_label='numcycles', isindexedread_label='isindexedread'):
    '''
    A method for bases mask value calculation

    :param numcycle_label: Cycle label in runinfo xml file, default numcycles
    :param isindexedread_label: Index cycle label in runinfo xml file, default isindexedread
    :returns: A formatted bases mask value for bcl2fastq run
    '''
    try:
      samplesheet_file = self.samplesheet_file
      runinfo_file = self.runinfo_file
      read_offset = self.read_offset
      index_offset = self.index_offset
      samplesheet_data = SampleSheet(infile=samplesheet_file)
      index_length_stats = samplesheet_data.get_index_count()
      index_length_list = list()
      for index_name in index_length_stats.keys():
        index_type = len(index_length_stats[index_name].keys())
        # check if all the index seq are similar or not
        if index_type > 1: raise ValueError('column {0} has variable lengths'.format( index_type ))
        # adding all non zero index lengths
        index_length = list(index_length_stats[index_name].keys())[0]
        if index_length > 0: 
          index_length_list.append(index_length)
      if len(set(index_length_list)) > 1: 
        raise ValueError('index lengths are not same in samplesheet {0}'.format( samplesheet_file ))
      # count non zero indexes in the samplesheet
      samplesheet_index_count = len(index_length_list)
      # get the allowed index length from samplesheet
      allowed_index_length = list(set(index_length_list))[0]
      runinfo_data = RunInfo_xml(xml_file=runinfo_file)
      runinfo_reads_stats = runinfo_data.get_reads_stats()
      read_count = 0
      index_count = 0
      bases_mask_list = list()
      for read_id in (sorted(runinfo_reads_stats.keys())):
        temp_index_offset = 0
        runinfo_read_length = int(runinfo_reads_stats[read_id][numcycle_label])
        # index
        if runinfo_reads_stats[read_id][isindexedread_label] == 'Y':
          index_count += 1
          # resetting the index_offset if its not provided
          if not index_offset and allowed_index_length < runinfo_read_length:
            temp_index_offset = int(runinfo_read_length)-int(allowed_index_length)
          if temp_index_offset > 0:
            real_index_count = int(runinfo_read_length - temp_index_offset)     # use temp index offset
          else:
            real_index_count = int(runinfo_read_length - index_offset)          # use global index offset
          # compare index length with samplesheet
          if real_index_count != allowed_index_length: 
            raise ValueError(
                    'Index length not matching in {0} and {1}, with offset {2} (temp: {3})'.\
                      format(samplesheet_file,runinfo_file, index_offset, temp_index_offset))
          # calculate base mask for index
          if ( index_count > samplesheet_index_count ):
            # mask everything if this index is not required for demultiplexing  
            mask = 'n{0}'.format(runinfo_read_length)
          else:
            # format index
            if temp_index_offset > 0:
              mask = 'i{0}n{1}'.format(real_index_count, temp_index_offset)     # use caculated offset for mask
            elif index_offset:
              mask = 'i{0}n{1}'.format(real_index_count, index_offset)          # use global offset for mask
            else:
              mask = 'i{0}'.format(real_index_count)                            # use raw counts
          bases_mask_list.append(mask)
        # read
        elif runinfo_reads_stats[read_id][isindexedread_label] == 'N':
          read_count += 1
          real_read_count = runinfo_read_length - read_offset
          # calculate base mask for index
          mask = 'y{0}n{1}'.format(real_read_count, read_offset) if read_offset else 'y{0}'.format(real_read_count)     
          bases_mask_list.append(mask)  
        # something else
        else:
          raise ValueError('file {0} has incorrect index information'.format(runinfo_file))
      bases_mask_format = ','.join(bases_mask_list)
      return bases_mask_format
    except:
      raise
