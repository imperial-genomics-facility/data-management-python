import os, re
from shutil import copy, copytree
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml

class moveBclFilesForDemultiplexing:
  def __init__(self,input_dir,output_dir,samplesheet,run_info_xml):
    self.input_dir    = input_dir
    self.output_dir   = output_dir
    self.samplesheet  = samplesheet
    self.run_info_xml = run_info_xml

  def copy_bcl_files(self):
    '''
    Function for copying BCL files to the output directory
    '''
    input_dir      = self.input_dir
    output_dir     = self.output_dir
    bcl_files_list = self._generate_platform_specific_list()

    if len(bcl_files_list)==0:
      raise ValueError('no file list found for samplesheet {0}'.format(self.input_dir))
  
    for bcl_entity in bcl_files_list:
      input_target=os.path.join(input_dir,bcl_entity)

      if os.path.isdir(input_target):          
        # copy dir
        output_target=os.path.join(output_dir, bcl_entity)
        copytree(input_target, output_target)   

      else:
        output_target=os.path.join(output_dir, os.path.dirname(bcl_entity))

        if not os.path.exists(output_target):
          os.makedirs(output_target)
        # copy file
        copy(input_target, output_target)
         
  def _generate_platform_specific_list(self, lane_list=[]):
    '''
    An internal function for getting list of files and directories specific for each platform
    Returns a list
    '''

    # set pattern for HiSeq platforms
    hiseq_pattern=re.compile('^HISEQ',re.IGNORECASE)
    nextseq_pattern=re.compile('^NEXTSEQ',re.IGNORECASE)
    miseq_pattern=re.compile('^MISEQ',re.IGNORECASE)

    # read the samplesheet info
    samplesheet_data=SampleSheet(infile=self.samplesheet)
    platform_name=samplesheet_data.get_platform_name()
    
    bcl_files_list=list()

    if (re.search(hiseq_pattern, platform_name)):
      # check for hiseq4000
      runinfo_data=RunInfo_xml(xml_file=self.run_info_xml)
      platform_series=runinfo_data.get_platform_number()
      
      # hack for checking 4000 platform, need to replace with db check
      if platform_series.startswith('K'):
        # hiseq4000
        bcl_files_list=['Data/Intensities/s.locs', 'RunInfo.xml','runParameters.xml']
 
        if len(lane_list):
          # need to change the following lines if there are more than 9 lanes
          for lane in lane_list:
            bcl_files_list.append('Data/Intensities/BaseCalls/L00{0}'.format(lane))
        else:
          for lane in samplesheet_data.get_lane_count():
            bcl_files_list.append('Data/Intensities/BaseCalls/L00{0}'.format(lane))
      else:
        # hiseq2500
        raise ValueError('no method of for hiseq 2500'.format())
    elif(re.search(nextseq_pattern, platform_name)):
      # NextSeq
      bcl_files_list=['Data','InterOp','RunInfo.xml','RunParameters.xml']
    elif(re.search(miseq_pattern, platform_name)):
      # MiSeq
      bcl_files_list=['Data','RunInfo.xml','runParameters.xml']
    else:
      raise ValueError('Platform {0} not recognised'.format(platform_name))
    
    return bcl_files_list
  
