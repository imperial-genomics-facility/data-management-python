import os,re,fnmatch
from shutil import copy,copytree
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.utils.fileutils import copy_local_file

class moveBclFilesForDemultiplexing:
  def __init__(
    self, input_dir, output_dir, samplesheet,
    run_info_xml, platform_model=None):
    self.input_dir = input_dir
    self.output_dir = output_dir
    self.samplesheet = samplesheet
    self.run_info_xml = run_info_xml
    self.platform_model = platform_model


  def copy_bcl_files(self):
    '''
    Function for copying BCL files to the output directory
    '''
    try:
      input_dir = self.input_dir
      output_dir = self.output_dir
      bcl_files_list = self._generate_platform_specific_list()
      if len(bcl_files_list) == 0:
        raise ValueError(
                'No file list found for samplesheet {0}'.\
                  format(self.input_dir))
      for bcl_entity in bcl_files_list:
        input_target = os.path.join(input_dir, bcl_entity)
        output_target = output_target = \
          os.path.join(output_dir, bcl_entity)
        copy_local_file(input_target, output_target)
    except Exception as e:
      raise ValueError(
              'Failed to copy bcl file, error: {0}'\
                .format(e))

  def _generate_platform_specific_list(self,lane_list=()):
    '''
    An internal function for getting list of files and directories specific for each platform

    :param lane_list: A list of lanes to check
    :returns: A list of files
    '''
    try:
      lane_list = list(lane_list)
      platform_model = self.platform_model
      samplesheet_data = SampleSheet(infile=self.samplesheet)
      if platform_model is None:
        # set pattern for HiSeq platforms
        hiseq_pattern = re.compile('^HISEQ', re.IGNORECASE)
        nextseq_pattern = re.compile('^NEXTSEQ', re.IGNORECASE)
        miseq_pattern = re.compile('^FASTQ Only', re.IGNORECASE)
        # read the samplesheet info
        platform_name = samplesheet_data.get_platform_name()
        runinfo_data = RunInfo_xml(xml_file=self.run_info_xml)
        platform_series = runinfo_data.get_platform_number()
        if (re.search(hiseq_pattern, platform_name)):
          if platform_series.startswith('K'):
            platform_model = 'HISEQ4000'                                            # assign platform model for HISEQ4000
          else:
            platform_model = 'HISEQ2500'                                            # or may be its HISEQ2500
            raise ValueError('no method of for hiseq 2500')
        elif(re.search(nextseq_pattern, platform_name)):
          platform_model = 'NEXTSEQ'                                                # assign platform model for 'NEXTSEQ'
        elif(re.search(miseq_pattern, platform_name)):
          if platform_series.startswith('M'):
            platform_model = 'MISEQ'
          else:
            raise ValueError(
                    'Platform series {0} is not MiSeq'.\
                      format(platform_series))
        else:
          raise ValueError(
                  'Platform {0} not recognised'.\
                    format(platform_name))
      bcl_files_list = list()
      if platform_model == 'HISEQ4000':
        bcl_files_list = [
          'Data/Intensities/s.locs',
          'InterOp',
          'RunInfo.xml',
          'runParameters.xml']
        if len(lane_list):
          # need to change the following lines if there are more than 9 lanes
          for lane in lane_list:
            bcl_files_list.\
              append(
                'Data/Intensities/BaseCalls/L00{0}'.format(lane))
        else:
          for lane in samplesheet_data.get_lane_count():
            bcl_files_list.\
              append(
                'Data/Intensities/BaseCalls/L00{0}'.format(lane))
      elif platform_model == 'NOVASEQ6000':
        bcl_files_list = [
          'Data/Intensities/s.locs',
          'InterOp',
          'RunInfo.xml',
          'RunParameters.xml']
        if len(lane_list):
          # need to change the following lines if there are more than 9 lanes
          for lane in lane_list:
            bcl_files_list.\
              append(
                'Data/Intensities/BaseCalls/L00{0}'.format(lane))
        else:
          for lane in samplesheet_data.get_lane_count():
            bcl_files_list.\
              append(
                'Data/Intensities/BaseCalls/L00{0}'.format(lane))
      elif platform_model == 'NEXTSEQ':
        bcl_files_list = [
          'Data',
          'InterOp',
          'RunInfo.xml',
          'RunParameters.xml']
      elif platform_model == 'MISEQ':
        bcl_files_list = [
          'Data',
          'InterOp',
          'RunInfo.xml',
          'runParameters.xml']
      return bcl_files_list
    except Exception as e:
      raise ValueError(
              'Failed to get a platform specific bcl file list, error: {0}'.\
                format(e))


class moveBclTilesForDemultiplexing(moveBclFilesForDemultiplexing):
  """
  A class for copying BCL files for a list of tiles to a specific dir

  :param input_dir: Input run dir
  :param output_dir: Target output dir
  :param samplesheet: Samplesheet filepath
  :param run_info_xml: RunInfo.xml file path
  :param platform_model: Platform model, default None
  :param force: Force copy existing file, default False
  :param tiles_list: List of times to copy, default (1101,)

    obj = moveBclTilesForDemultiplexing(**kwargs)
    obj.copy_bcl_files()

  """
  def __init__(self, input_dir, output_dir, samplesheet, run_info_xml,
               force=False, platform_model=None, tiles_list=(1101,)):
    self.tiles_list = list(tiles_list)
    self.force = force
    super().__init__(
      input_dir,
      output_dir,
      samplesheet,
      run_info_xml,
      platform_model)

  def copy_bcl_files(self):
    """
    A function for transferring bcl files for selected tiles to a target dir
    """
    try:
      input_dir = self.input_dir
      output_dir = self.output_dir
      bcl_files_list, final_tiles_list = \
      self._generate_platform_specific_list()
      if len(bcl_files_list)==0:
        raise ValueError(
                'No file list found for run path {0}'.\
                  format(self.input_dir))
      bcl_files_list = list(set(bcl_files_list))                                 # make the list unique
      for bcl_entity in bcl_files_list:
        bcl_entity = bcl_entity.lstrip('/')
        input_target = os.path.join(input_dir, bcl_entity)
        output_target = os.path.join(output_dir, bcl_entity)
        if not os.path.exists(output_target) or \
           input_target.endswith('*.csv') or \
            (os.path.exists(output_target) and
             os.path.getsize(input_target) != os.path.getsize(output_target)):
          copy_local_file(input_target, output_target, force=self.force)             # skip existing cbcl file
      return final_tiles_list
    except Exception as e:
      raise ValueError(
              'Failed to copy bcl files for tiles, error: {0}'.\
                format(e))

  def _generate_platform_specific_list(self):
    """
    An internal method for generating platform specific file list
    """
    try:
      bcl_files_list = \
        super()._generate_platform_specific_list()
      final_bcl_list = list()
      final_tiles_list = list()
      for entry in bcl_files_list:
        if entry.startswith('Data/Intensities/BaseCalls/L00'):
          lookup_path = os.path.join(self.input_dir, entry)
          lane = os.path.basename(entry).replace('L00', '')
          for tile in self.tiles_list:
            tile_id = 's_{0}_{1}'.format(lane, tile)
            final_tiles_list.append(tile_id)
            if self.platform_model == 'HISEQ4000':
              for root, _, files in os.walk(lookup_path):
                for f in files:
                  if fnmatch.fnmatch(f, '{0}*'.format(tile_id)):
                    path = os.path.join(root, f)
                    path = path.replace(self.input_dir, '')
                    final_bcl_list.append(path)
            elif self.platform_model == 'NOVASEQ6000':
              temp_list = list()
              for root, _, files in os.walk(lookup_path):
                for f in files:
                  if f.endswith('.filter'):
                    if fnmatch.fnmatch(f, '{0}.filter'.format(tile_id)):
                      temp_list.append(os.path.join(root, f))
                  if f.endswith('.cbcl'):
                    temp_list.append(os.path.join(root, f))
              for path in temp_list:
                path = path.replace(self.input_dir, '')
                final_bcl_list.append(path)
        else:
          final_bcl_list.append(entry)
      return final_bcl_list, final_tiles_list
    except Exception as e:
      raise ValueError(
              'Failed to generate platform specific list got bcl tile, error: {0}'.\
                format(e))