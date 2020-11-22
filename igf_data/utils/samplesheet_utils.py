import os
import pandas as pd
from igf_data.illumina.basesMask import BasesMask
from igf_data.utils.sequtils import rev_comp
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.fileutils import get_temp_dir,copy_local_file,check_file_path
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import validate_samplesheet_for_seqrun


def get_formatted_samplesheet_per_lane(
    samplesheet_file,singlecell_barcode_json,runinfo_file,output_dir,filter_lane=None,
    single_cell_tag='10X',index1_rule=None,index2_rule=None):
  """
  A function for filtering and reformatting samplesheet files and splitting the data per lane

  :param samplesheet_file: Samplesheet file path
  :param singlecell_barcode_json: Singlecell barcode json path
  :param runinfo_file: Path to RunInfo.xml file
  :param output_dir: Output dir path
  :param filter_lane: Lane to filter Samplesheeet data, default None
  :param single_cell_tag: Tag for singlecell samples, default 10X
  :param index1_rule: Rules for I7 index modification, default None, use REVCOMP
  :param index2_rule: Rules for I5 index modification, default None, use REVCOMP
  :returns: A list of dictionaries containing the following keys

    * lane_id
    * samplesheet_file
    * bases_mask

  """
  try:
    tmp_dir = get_temp_dir()
    tmp_file = \
      os.path.join(
        tmp_dir,
        os.path.basename(samplesheet_file))
    sc_data = \
      ProcessSingleCellSamplesheet(
        samplesheet_file,
        singlecell_barcode_json,
        single_cell_tag)
    sc_data.\
      change_singlecell_barcodes(tmp_file)
    sa = SampleSheet(tmp_file)
    if 'Lane' not in sa._data_header:
      raise ValueError(
              'Lane not present in samplesheet {0}'.\
                format(samplesheet_file))
    if filter_lane is not None and \
       filter_lane in range(1,9):
      sa.filter_sample_data(
        condition_key='Lane',
        condition_value=str(filter_lane))
    lanes = sa.get_lane_count()
    file_list = list()
    for lane_id in lanes:
      sa = SampleSheet(tmp_file)
      df = pd.DataFrame(sa._data)
      lane_df = df[df['Lane']==lane_id].copy()
      if len(lane_df.index)==0:
        raise ValueError(
                'No data present in samplesheet {0}, lane {1}'.\
                  format(samplesheet_file,lane_id))
      min_index1 = \
        lane_df['index'].\
          map(lambda x: len(x)).min()
      lane_df.loc[:,'index'] = \
        lane_df['index'].\
          map(lambda x: x[0:min_index1])
      if 'index2' in lane_df.columns:
        min_index2 = \
          lane_df['index2'].\
            map(lambda x: len(x)).min()
        lane_df.loc[:,'index2'] = \
          lane_df['index2'].\
            map(lambda x: x[0:min_index2])
        lane_df.loc[:,'c_index'] = \
          lane_df['index']+lane_df['index2']
      else:
        lane_df.loc[:,'c_index'] = lane_df['index']
      lane_df.\
        drop_duplicates('c_index',inplace=True)
      lane_df.drop('c_index',axis=1,inplace=True)
      filename = \
        '{0}_{1}'.format(
          os.path.basename(samplesheet_file),
          lane_id)
      tmp_filepath = \
        os.path.join(tmp_dir,filename)
      target_filepath = \
        os.path.join(output_dir,filename)
      sa._data = \
        lane_df.to_dict(orient='records')
      if index1_rule is not None and \
         index1_rule=='REVCOMP':
        sa.get_reverse_complement_index('index')
      if index2_rule is not None and \
         index2_rule=='REVCOMP':
        sa.get_reverse_complement_index('index2')
      sa.print_sampleSheet(tmp_filepath)
      copy_local_file(
        tmp_filepath,
        target_filepath)
      bases_mask_object = \
        BasesMask(
          samplesheet_file=tmp_filepath,
          runinfo_file=runinfo_file,
          read_offset=1,
          index_offset=0)
      bases_mask_value = \
        bases_mask_object.\
          calculate_bases_mask()
      file_list.\
        append({
          'lane_id':lane_id,
          'samplesheet_file':target_filepath,
          'bases_mask':bases_mask_value})
    return file_list
  except Exception as e:
    raise ValueError(
            'Failed to format samplesheet, error: {0}'.\
              format(e))