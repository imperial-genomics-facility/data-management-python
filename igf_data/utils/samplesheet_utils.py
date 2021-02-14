import os
import traceback
import pandas as pd
from igf_data.illumina.basesMask import BasesMask
from igf_data.utils.sequtils import rev_comp
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.utils.fileutils import get_temp_dir,copy_local_file,check_file_path,remove_dir
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import validate_samplesheet_for_seqrun
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import check_for_registered_project_and_sample

def get_formatted_samplesheet_per_lane(
    samplesheet_file,singlecell_barcode_json,singlecell_dual_barcode_json,runinfo_file,output_dir,
    platform,filter_lane=None,single_cell_tag='10X',index1_rule=None,index2_rule=None):
  """
  A function for filtering and reformatting samplesheet files and splitting the data per lane

  :param samplesheet_file: Samplesheet file path
  :param singlecell_barcode_json: Singlecell barcode json path
  :param singlecell_dual_barcode_json: Single cell dual barcode json path
  :param runinfo_file: Path to RunInfo.xml file
  :param platform: Platform name for setting sc dual index workflow
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
    sc_dual_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=samplesheet_file,
        singlecell_dual_index_barcode_json=singlecell_dual_barcode_json,
        platform=platform,
        index2_rule=index2_rule)
    sc_dual_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=tmp_file)
    samplesheet_file = tmp_file
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
       int(filter_lane) in range(1,9):
      sa.filter_sample_data(
        condition_key='Lane',
        condition_value=str(filter_lane))
    lanes = sa.get_lane_count()
    file_list = list()
    for lane_id in lanes:
      sa = SampleSheet(tmp_file)
      sa.filter_sample_data(
        condition_key='Lane',
        condition_value=str(lane_id))
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
    traceback.print_exc()
    raise ValueError(
            'Failed to format samplesheet, error: {0}'.\
              format(e))


def samplesheet_validation_and_metadata_checking(
      samplesheet_file,schema_json_file,log_dir,seqrun_id,db_config_file):
  """
  A function for samplesheet validation and metadata checking

  :param samplesheet_file: A Samplesheet file path
  :param schema_json_file: A JSON schema for samplesheet validation checking
  :param log_dir: Path for log dir
  :param seqrun_id: Sequencing run id
  :param db_config_file: DB config file
  ;returns: A list of error file paths
  """
  try:
    tmp_dir = get_temp_dir()
    validation_output = list()
    _,error_file_list = \
      validate_samplesheet_for_seqrun(
        seqrun_info={seqrun_id:os.path.dirname(samplesheet_file)},
        schema_json=schema_json_file,
        output_dir=tmp_dir,
        samplesheet_file=os.path.basename(samplesheet_file))
    if len(error_file_list.keys()) > 0:
      tmp_err_file = \
        error_file_list.\
          get(seqrun_id)
      if tmp_err_file is None or \
         tmp_err_file == '':
        raise ValueError('No validation error file found')
      target_file = \
        os.path.join(
          log_dir,
          os.path.basename(tmp_err_file))
      copy_local_file(
        tmp_err_file,
        target_file)
      validation_output.\
        append(target_file)
    _,msg = \
      check_for_registered_project_and_sample(
        seqrun_info={seqrun_id:os.path.dirname(samplesheet_file)},
        dbconfig=db_config_file,
        samplesheet_file=os.path.basename(samplesheet_file))
    if msg != '' and \
       msg is not None:
      tmp_file = \
        os.path.join(
          tmp_dir,
          '{0}_metadata_error.txt'.\
            format(os.path.basename(samplesheet_file)))
      with open(tmp_file,'w') as fp:
        fp.write('{0}\n'.format(msg))
      target_file = \
        os.path.join(
          log_dir,
          os.path.basename(tmp_file))
      copy_local_file(
        tmp_file,
        target_file)
      validation_output.\
        append(target_file)
    remove_dir(tmp_dir)
    return validation_output
  except Exception as e:
    raise ValueError(
            'Failed samplesheet checking, error: {0}'.format(e))