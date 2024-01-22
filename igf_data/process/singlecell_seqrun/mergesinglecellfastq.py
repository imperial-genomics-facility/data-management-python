import pandas as pd
from typing import Tuple
import os, fnmatch, re, subprocess
from collections import defaultdict
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.fileutils import get_temp_dir, remove_dir, copy_local_file

class MergeSingleCellFastq:
  '''
  A class for merging single cell fastq files per lane per sample

  :param fastq_dir: A directory path containing fastq files
  :param samplesheet: A samplesheet file used demultiplexing of bcl files
  :param platform_name: A sequencing platform name
  :param singlecell_tag: A single cell keyword for description field, default '10X'
  :param sampleid_col: A keyword for sample id column of samplesheet, default 'Sample_ID'
  :param samplename_col: A keyword for sample name column of samplesheet, default 'Sample_Name'
  :param orig_sampleid_col: A keyword for original sample id column, default 'Original_Sample_ID'
  :param orig_samplename_col: A keyword for original sample name column, default 'Original_Sample_Name'
  :param description_col: A keyword for description column, default 'Description'
  :param project_col: A keyword for project column, default 'Sample_Project'
  :param pseudo_lane_col: A keyword for pseudo lane column, default 'PseudoLane'
  :param lane_col: A keyword for lane column, default 'Lane'
  :param force_overwrite: A toggle for overwriting output fastqs, default True
  :patam use_sample_id_as_fastq_prefix: Use Sample id as fastq name, default False for Sample name
  :param pseudo_lane_list: Pseudo lanes for NextSeq, default ('1', '2', '3', '4')
  :param use_bclconvert_settings: Use bclconvert settings, default False

  SampleSheet file should contain following columns:
     * Sample_ID: A single cell sample id in the following format, SampleId_{digit}
     * Sample_Name: A single cell sample name in the following format, SampleName_{digit}
     * Original_Sample_ID: An IGF sample id
     * Original_Sample_Name: A sample name provided by user
     * Description: A single cell label, default 10X
  '''
  def __init__(
        self,
        fastq_dir: str,
        samplesheet: str,
        platform_name: str,
        singlecell_tag: str ='10X',
        sampleid_col: str = 'Sample_ID',
        samplename_col: str = 'Sample_Name',
        use_ephemeral_space: bool = False,
        orig_sampleid_col : str = 'Original_Sample_ID',
        description_col: str = 'Description',
        orig_samplename_col: str = 'Original_Sample_Name',
        project_col: str = 'Sample_Project',
        lane_col: str = 'Lane',
        pseudo_lane_list: tuple = ('1', '2', '3', '4'),
        pseudo_lane_col: str = 'PseudoLane',
        force_overwrite: bool = True,
        use_bclconvert_settings: bool = False) -> None:
    """
    Add params
    """
    self.fastq_dir = fastq_dir
    self.samplesheet = samplesheet
    self.platform_name = platform_name
    self.singlecell_tag = singlecell_tag
    self.sampleid_col = sampleid_col
    self.samplename_col = samplename_col
    self.orig_sampleid_col = orig_sampleid_col
    self.description_col = description_col
    self.orig_samplename_col = orig_samplename_col
    self.project_col = project_col
    self.lane_col = lane_col
    self.pseudo_lane_list = pseudo_lane_list
    self.pseudo_lane_col = pseudo_lane_col
    self.force_overwrite = force_overwrite
    self.use_ephemeral_space = use_ephemeral_space
    self.use_bclconvert_settings = use_bclconvert_settings

  def _fetch_lane_and_sample_info_from_samplesheet(self) -> list:
    '''
    A internal method for grouping samples per lane based on the samplesheet
    returns a list containing sample and lane information per row
    '''
    try:
      samplesheet_data = \
        SampleSheet(infile=self.samplesheet)                                   # read samplesheet file
      if (self.orig_sampleid_col not in samplesheet_data._data_header) or \
         (self.orig_samplename_col not in samplesheet_data._data_header):
        raise ValueError(
          f'Samplesheet {self.samplesheet} does not have {self.orig_sampleid_col} or {self.orig_samplename_col} column')   # check for required columns in the samplesheet
      samplesheet_data.\
      filter_sample_data(
        condition_key=self.description_col,
        condition_value=self.singlecell_tag,
        method='include')                                                       # filter samplesheet for single cell data
      sample_lane_data = list()
      if self.platform_name=='NEXTSEQ' or \
         self.platform_name=='NEXTSEQ2000':                                     # hack for nextseq
        samplesheet_data.\
          add_pseudo_lane_for_nextseq(
            lanes=self.pseudo_lane_list)
        samplesheet_group_columns = [
          self.pseudo_lane_col,
          self.orig_sampleid_col,
          self.orig_samplename_col,
          self.project_col]
        if len(samplesheet_data._data) > 0:
          samplesheet_df = \
            pd.DataFrame(samplesheet_data._data)
          for group_tag, _ in samplesheet_df.groupby(samplesheet_group_columns):
            sample_lane_data.append({
              'lane_id': group_tag[0],
              'sample_id': group_tag[1],
              'sample_name': group_tag[2],
              'project_id': group_tag[3]})
      elif self.platform_name=='MISEQ':                                         # hack for miseq
        samplesheet_data.\
          add_pseudo_lane_for_miseq()
        samplesheet_group_columns = [
          self.pseudo_lane_col,
          self.orig_sampleid_col,
          self.orig_samplename_col,
          self.project_col]
        if len(samplesheet_data._data) > 0:
          samplesheet_df = \
            pd.DataFrame(samplesheet_data._data)
          for group_tag, _ in samplesheet_df.groupby(samplesheet_group_columns):
            sample_lane_data.append({
              'lane_id': group_tag[0],
              'sample_id': group_tag[1],
              'sample_name': group_tag[2],
              'project_id': group_tag[3]})
      elif self.platform_name=='HISEQ4000' or \
           self.platform_name=='NOVASEQ6000':                                     # check for hiseq4k
        samplesheet_group_columns = [
          self.lane_col,
          self.orig_sampleid_col,
          self.orig_samplename_col,
          self.project_col]
        if len(samplesheet_data._data) > 0:
          samplesheet_df = \
            pd.DataFrame(samplesheet_data._data)
          for group_tag, _ in samplesheet_df.groupby(samplesheet_group_columns):
            sample_lane_data.append({
              'lane_id': group_tag[0],
              'sample_id': group_tag[1],
              'sample_name': group_tag[2],
              'project_id': group_tag[3]})
      else:
        raise ValueError(
          f'platform {self.platform_name} not supported')
      return sample_lane_data
    except Exception as e:
      raise ValueError(
        f"Failed to parse samplesheet {self.samplesheet} with error {e}")


  @staticmethod
  def _group_singlecell_fastq(
        sample_data: list,
        fastq_dir: str,
        use_sample_id_as_fastq_prefix: bool = False) \
          -> Tuple[dict, dict]:
    '''
    A static method for grouping single cell fastq files

    :param sample_data: A list of sample entries from samplesheet
                It should contain following keys for each row:
                lane_id, sample_id, sample_name, project_id
    :param use_sample_id_as_fastq_prefix: Use sample id as fastq prefix, default False sample name
    :param fastq_dir: A directory path containing fastq files

    :returns: two dictionary of fastq group, one for single cell samples and 
    another for sample information
    '''
    try:
      samples_info = defaultdict(dict)
      sample_files_list = \
        defaultdict(lambda: \
          defaultdict(lambda: \
            defaultdict(lambda: \
              defaultdict(list))))                                              # output data structure
      for sample_record in sample_data:
        sample_lane = sample_record.get('lane_id')
        sample_id = sample_record.get('sample_id')
        sample_name = sample_record.get('sample_name')
        project_id = sample_record.get('project_id')
        samples_info[sample_id]['sample_name'] = sample_name
        samples_info[sample_id]['project_id'] = project_id
        sample_id_regex = re.compile(r'^{0}_\d$'.format(sample_id))              # regexp for sample id match
        if use_sample_id_as_fastq_prefix:
          fastq_name_prefix = sample_id
        else:
          fastq_name_prefix = sample_name
        file_name_regex = \
          re.compile(r'^{0}_(\d)_S\d+_L00{1}_([R,I][1,2,3,4])_\d+\.fastq(\.gz)?$'.\
                     format(
                       fastq_name_prefix,
                       sample_lane))                                            # regexp for fastq file match
        for root, _, files in os.walk(fastq_dir):
          for file in files:
            if fnmatch.fnmatch(file, "*.fastq.gz") and \
               not fnmatch.fnmatch(file, "Undetermined_*"):                     # skip undetermined reads
              if use_sample_id_as_fastq_prefix:
                if re.search(file_name_regex, file):
                  sm = re.match(file_name_regex, file)
                  if len(sm.groups())>=2:
                    fragment_id=sm.group(1)
                    read_type=sm.group(2)
                    sample_files_list[sample_lane][sample_id][read_type][fragment_id].\
                    append(os.path.join(root, file))
                  else:
                    raise ValueError(
                      f'Failed to determined sample info:{sample_id}, {file}')
              else:
                if re.search(sample_id_regex, os.path.basename(root)) and \
                   re.search(file_name_regex, file):
                  sm = re.match(file_name_regex, file)
                  if len(sm.groups())>=2:
                    fragment_id=sm.group(1)
                    read_type=sm.group(2)
                    sample_files_list[sample_lane][sample_id][read_type][fragment_id].\
                    append(os.path.join(root, file))                              # add fastqs to samples list
                  else:
                    raise ValueError(
                      f'Failed to determined sample info:{sample_id}, {file}')
      return sample_files_list, samples_info
    except Exception as e:
      raise ValueError(
        f"Failed to group fastq files with error {e}")


  def merge_fastq_per_lane_per_sample(self):
    '''
    A method for merging single cell fastq files present in input fastq_dir
    per lane per sample basis
    '''
    try:
      sample_data = \
        self._fetch_lane_and_sample_info_from_samplesheet()                     # get sample and lane information from samplesheet
      sample_files, samples_info = \
        self._group_singlecell_fastq(
          sample_data=sample_data,
          fastq_dir=self.fastq_dir,
          use_sample_id_as_fastq_prefix=self.use_bclconvert_settings)                                                       # get file groups
      all_intermediate_files=list()                                             # empty list for intermediate files
      s_count = 0                                                               # initial count for fastq S value
      for lane_id in sorted(sample_files.keys()):
        if self.platform_name=='NEXTSEQ' or \
           self.platform_name=='NEXTSEQ2000':
            s_count = 0                                                         # nextseq is weird, reset counter for each lane
        for sample_id in sorted(sample_files[lane_id].keys()):
          s_count += 1                                                          # assign new S value for fastq files
          sample_name = samples_info.get(sample_id)['sample_name']
          project_id = samples_info.get(sample_id)['project_id']                # get sample and project info
          if self.use_bclconvert_settings:
            fastq_name_prefix = sample_id
          else:
            fastq_name_prefix = sample_name
          if self.use_bclconvert_settings:
            output_path = \
              os.path.join(
                self.fastq_dir,
                project_id)                                                      # output location is under input fastq_dir
          else:
            output_path = \
              os.path.join(
                self.fastq_dir,
                project_id,
                sample_id)
          if not os.path.exists(output_path):
            os.makedirs(output_path, mode=0o770)                                # create outout directory

          for read_type in sample_files[lane_id][sample_id].keys():             # merge per read type
            output_filename = \
              f'{fastq_name_prefix}_S{s_count}_L00{lane_id}_{read_type}_001.fastq.gz'                                                    # assign new output filename
            final_path = \
              os.path.join(output_path, output_filename)                        # assign final output path
            if not self.force_overwrite and os.path.exists(final_path):
              raise ValueError(
                f'Failed to overwrite existing file {final_path}')
            input_list = list()
            for sc_fragment, file_path in \
              sorted(sample_files[lane_id][sample_id][read_type].items()):
              input_list.extend(file_path)                                      # create list of input fastqs for merge
            if len(input_list) != 4:
              raise ValueError(\
                'expecting 4 files, got {0} for sample {1}, lane {2}, read type {3}'.\
                  format(
                    len(input_list),
                    sample_id,
                    lane_id,
                    read_type))                                                 # checking input files list
            temp_dir = \
              get_temp_dir(
                use_ephemeral_space=self.use_ephemeral_space)                   # get a temp dir
            temp_file = \
              os.path.join(
                temp_dir,
                output_filename)                                                # assign temp filename
            cmd = ["cat"] + input_list + [">", temp_file]                       # shell command for merging fastq.gz files
            subprocess.\
              check_call(" ".join(cmd), shell=True)                             # exact same command for fastq merge as 10x pipeline
            copy_local_file(
              temp_file,
              final_path,
              force=True)                                                       # copy file to final location
            remove_dir(temp_dir)                                                # remove temp dir
            for file_path in input_list:
              all_intermediate_files.\
                append(file_path)                                               # add fastq to intermediate list
      for file_path in all_intermediate_files:
        os.remove(file_path)                                                    # remove intermediate files once merging is complete
    except:
      raise