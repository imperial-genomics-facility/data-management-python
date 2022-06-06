import pandas as pd
from copy import deepcopy
import os
import re
import stat
import logging
import subprocess
from typing import Tuple
from airflow.models import Variable
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_data.illumina.runparameters_xml import RunParameter_xml
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellDualIndexSamplesheet
from igf_data.process.singlecell_seqrun.processsinglecellsamplesheet import ProcessSingleCellSamplesheet
from igf_data.utils.box_upload import upload_file_or_dir_to_box
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import copy_remote_file
from igf_data.utils.fileutils import copy_local_file
from igf_data.utils.fileutils import check_file_path
from igf_data.utils.fileutils import read_json_data
from igf_data.utils.fileutils import get_date_stamp
from igf_data.utils.fileutils import get_date_stamp_for_file_name
from igf_data.utils.fileutils import calculate_file_checksum
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.utils.singularity_run_wrapper import execute_singuarity_cmd
from igf_data.utils.jupyter_nbconvert_wrapper import Notebook_runner
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SEQRUN_BASE_PATH = Variable.get('hpc_seqrun_path', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
SINGLECELL_BARCODE_JSON = Variable.get('singlecell_barcode_json', default_var=None)
SINGLECELL_DUAL_BARCODE_JSON = Variable.get('singlecell_dual_barcode_json', default_var=None)
BCLCONVERT_IMAGE = Variable.get('bclconvert_image_path', default_var=None)
INTEROP_NOTEBOOK_IMAGE = Variable.get('interop_notebook_image_path', default_var=None)
BCLCONVERT_REPORT_TEMPLATE = Variable.get('bclconvert_report_template', default_var=None)
BCLCONVERT_REPORT_LIBRARY = Variable.get("bclconvert_report_library", default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
FASTQC_IMAGE_PATH = Variable.get('fastqc_image_path', default_var=None)
FASTQSCREEN_IMAGE_PATH = Variable.get('fastqscreen_image_path', default_var=None)
FASTQSCREEN_CONF_PATH = Variable.get('fastqscreen_conf_path', default_var=None)

log = logging.getLogger(__name__)

def run_fastqScreen(
    fastqscreen_image_path: str,
    fastq_path: str,
    output_dir: str,
    fastqscreen_conf: str,
    fastqscreen_exe: str = 'fastq_screen',
    fastqscreen_options: tuple = (
      '--aligner bowtie2',
      '--force',
      '--quiet',
      '--subset 100000',
      '--threads 1')) \
    -> list:
  try:
    check_file_path(fastqscreen_image_path)
    check_file_path(fastq_path)
    check_file_path(output_dir)
    check_file_path(fastqscreen_conf)
    temp_dir = get_temp_dir()
    fastqscreen_cmd = [
      fastqscreen_exe,
      '-conf', fastqscreen_conf,
      '--outdir', temp_dir]
    fastqscreen_cmd.extend(fastqscreen_options)
    fastqscreen_cmd.append(fastq_path)
    bind_dir_list = [
      temp_dir,
      os.path.dirname(fastq_path),
      os.path.dirname(fastqscreen_conf)]
    execute_singuarity_cmd(
      image_path=fastqscreen_image_path,
      command_string=' '.join(fastqscreen_cmd),
      bind_dir_list=bind_dir_list)
    output_file_list = list()
    for file_path in os.listdir(temp_dir):
      if file_path.endswith('.txt') or \
         file_path.endswith('.html'):
        source_path = \
          os.path.join(temp_dir, file_path)
        dest_path = \
          os.path.join(output_dir, file_path)
        copy_local_file(source_path, dest_path, force=True)
        output_file_list.append(dest_path)
    return output_file_list
  except Exception as e:
    raise ValueError("Failed to run fastqscreen, error: {0}".format(e))


def run_fastqc(
    fastqc_image_path: str,
    fastq_path: str,
    output_dir: str,
    fastqc_exe: str = 'fastqc',
    fastqc_options: tuple = ('-q', '--noextract', '-ffastq', '-k7', '-t1')) \
    -> list:
  try:
    check_file_path(fastq_path)
    check_file_path(output_dir)
    check_file_path(fastqc_image_path)
    temp_dir = get_temp_dir()
    if isinstance(fastqc_options, tuple):
      fastqc_options = list(fastqc_options)
    fastqc_cmd = [
      fastqc_exe,
      '-o', temp_dir,
      '-d', temp_dir]
    fastqc_cmd.extend(fastqc_options)                                           # add additional parameters
    fastqc_cmd.append(fastq_path)
    bind_dir_list = [
      temp_dir,
      os.path.dirname(fastq_path)]
    execute_singuarity_cmd(
      image_path=fastqc_image_path,
      command_string=' '.join(fastqc_cmd),
      bind_dir_list=bind_dir_list)
    fastqc_zip = list()
    fastqc_html = list()
    for files in os.listdir(temp_dir):
      if files.endswith('.zip'):
        fastqc_zip.append(os.path.join(temp_dir, files))
      elif files.endswith('.html'):
        fastqc_html.append(os.path.join(temp_dir, files))
    if len(fastqc_html) == 0:
      raise ValueError("No fastqc html report found")
    output_file_list = list()
    for html_file in fastqc_html:
      dest_file = \
        os.path.join(
          output_dir,
          os.path.basename(html_file))
      copy_local_file(
        src_file=html_file,
        dest_file=dest_file)
      output_file_list.append(dest_file)
    for zip_file in fastqc_zip:
      dest_file = \
        os.path.join(
          output_dir,
          os.path.basename(zip_file))
      copy_local_file(
        src_file=zip_file,
        dest_file=dest_file)
      output_file_list.append(dest_file)
    return output_file_list
  except Exception as e:
    raise ValueError("Failed to run fastqc for {0}".format(fastq_path))

def fastqscreen_run_wrapper_for_known_samples_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_bclconvert_output = \
      context['params'].\
        get("xcom_key_for_bclconvert_output", "bclconvert_output")
    xcom_task_for_bclconvert_output = \
      context['params'].\
        get("xcom_task_for_bclconvert_output")
    xcom_key_for_collection_group = \
      context['params'].\
        get("xcom_key_for_collection_group", "collection_group")
    xcom_task_for_collection_group = \
      context['params'].\
        get("xcom_task_for_collection_group")
    fastqscreen_collection_type = 'FASTQSCREEN_HTML_REPORT'
    collection_table = 'run'
    bclconvert_output = \
      ti.xcom_pull(
        task_ids=xcom_task_for_bclconvert_output,
        key=xcom_key_for_bclconvert_output)
    collection_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_collection_group,
        key=xcom_key_for_collection_group)
    fastqscreen_temp_output_path = \
      os.path.join(bclconvert_output, 'fastqscreen_dir')
    os.makedirs(fastqscreen_temp_output_path, exist_ok=True)
    fastqscreen_collection_list = list()
    work_dir = get_temp_dir(use_ephemeral_space=True)
    for entry in collection_group:
      collection_name = entry.get('collection_name')
      dir_list = entry.get('dir_list')
      file_list = entry.get('file_list')
      ## RUN FASTQACREEN for the file
      fastq_output_dict = dict()
      for fastq_file_entry in file_list:
        fastq_file = fastq_file_entry.get('file_path')
        output_fastqc_list = \
          run_fastqScreen(
            fastqscreen_image_path=FASTQSCREEN_IMAGE_PATH,
            fastqscreen_conf=FASTQSCREEN_CONF_PATH,
            fastq_path=fastq_file,
            output_dir=work_dir)
        for file_entry in output_fastqc_list:
          dest_path = \
            os.path.join(
              fastqscreen_temp_output_path,
              os.path.basename(file_entry))
          copy_local_file(
            file_entry,
            dest_path, force=True)
          if file_entry.endswith('.html'):
            fastq_output_dict.append({
              'file_path': file_entry,
              'md5': calculate_file_checksum(file_entry)})
      ## LOAD FASTQC REPORT TO DB
      dir_list = [
        f if f != 'fastq' else 'fastqscreen'
          for f in dir_list]
      fastqscreen_collection_list.append({
        'collection_name': collection_name,
        'dir_list': dir_list,
        'file_list': fastq_output_dict})
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=fastqscreen_collection_type,
        collection_table=collection_table,
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_STORAGE',
        collection_list=fastqscreen_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def fastqc_run_wrapper_for_known_samples_func(**context):
  try:
    # TO DO
    # * get fastq file and sample name from xcom
    # * get fastqc temp output path from xcom
    # * get fastqc image
    # * run fastqc
    # * collect report per sample
    # * load report to disk and db
    # * copy fastqc results to temp output path
    ti = context['ti']
    xcom_key_for_bclconvert_output = \
      context['params'].\
        get("xcom_key_for_bclconvert_output", "bclconvert_output")
    xcom_task_for_bclconvert_output = \
      context['params'].\
        get("xcom_task_for_bclconvert_output")
    xcom_key_for_collection_group = \
      context['params'].\
        get("xcom_key_for_collection_group", "collection_group")
    xcom_task_for_collection_group = \
      context['params'].\
        get("xcom_task_for_collection_group")
    fastqc_collection_type = 'FASTQC_HTML_REPORT'
    collection_table = 'run'
    bclconvert_output = \
      ti.xcom_pull(
        task_ids=xcom_task_for_bclconvert_output,
        key=xcom_key_for_bclconvert_output)
    collection_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_collection_group,
        key=xcom_key_for_collection_group)
    fastqc_temp_output_path = \
      os.path.join(bclconvert_output, 'fastqc_dir')
    os.makedirs(fastqc_temp_output_path, exist_ok=True)
    fastqc_collection_list = list()
    work_dir = get_temp_dir(use_ephemeral_space=True)
    for entry in collection_group:
      collection_name = entry.get('collection_name')
      dir_list = entry.get('dir_list')
      file_list = entry.get('file_list')
      ## RUN FASTQC for the file
      fastq_output_dict = dict()
      for fastq_file_entry in file_list:
        fastq_file = fastq_file_entry.get('file_path')
        output_fastqc_list = \
          run_fastqc(
            fastqc_image_path=FASTQC_IMAGE_PATH,
            fastq_path=fastq_file,
            output_dir=work_dir)
        for file_entry in output_fastqc_list:
          dest_path = \
            os.path.join(
              fastqc_temp_output_path,
              os.path.basename(file_entry))
          copy_local_file(
            file_entry,
            dest_path, force=True)
          if file_entry.endswith('.html'):
            fastq_output_dict.append({
              'file_path': file_entry,
              'md5': calculate_file_checksum(file_entry)})
      ## LOAD FASTQC REPORT TO DB
      dir_list = [
        f if f != 'fastq' else 'fastqc'
          for f in dir_list]
      fastqc_collection_list.append({
        'collection_name': collection_name,
        'dir_list': dir_list,
        'file_list': fastq_output_dict})
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type=fastqc_collection_type,
        collection_table=collection_table,
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_STORAGE',
        collection_list=fastqc_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise

def get_flatform_name_and_flowcell_id_for_seqrun(
      seqrun_igf_id: str,
      db_config_file: str) -> Tuple[str, str]:
    try:
      check_file_path(db_config_file)
      dbparams = read_dbconf_json(db_config_file)
      sr = SeqrunAdaptor(**dbparams)
      sr.start_session()
      platform_name = \
        sr.fetch_platform_info_for_seqrun(
          seqrun_igf_id=seqrun_igf_id)
      seqrun = \
        sr.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_igf_id)
      return platform_name, seqrun.flowcell_id
    except Exception as e:
      raise ValueError(
              "Failed to get platform name and flowcell id for seqrun {0}, error: {1}".\
              format(seqrun_igf_id, e))


def get_project_id_samples_list_from_db(
      sample_igf_id_list: list,
      db_config_file: str) \
      -> dict:
    try:
      check_file_path(db_config_file)
      project_sample_dict = dict()
      dbparams = read_dbconf_json(db_config_file)
      sa = SampleAdaptor(**dbparams)
      sa.start_session()
      for sample_id in sample_igf_id_list:
        project_id = \
          sa.fetch_sample_project(sample_igf_id=sample_id)
        if project_id is None:
          raise ValueError(
                  "Failed to get project id for sample {0}".\
                  format(sample_id))
        project_sample_dict.\
          update({sample_id: project_id})
      sa.close_session()
      return project_sample_dict
    except Exception as e:
      raise ValueError(
              "Failed to get project id and samples list from db, error: {0}".format(e))


def copy_or_replace_file_to_disk_and_change_permission(
      source_path: str,
      destination_path: str,
      replace_existing_file: bool = False,
      make_file_and_dir_read_only : bool = True) \
      -> None:
  try:
    if os.path.exists(destination_path):
      if not replace_existing_file:
        raise ValueError(
                "File {0} already exists. Set replace_existing_file to True".\
                format(destination_path))
      else:
        # add write permission for user
        os.chmod(destination_path, stat.S_IWUSR)
        os.chmod(
          os.path.dirname(destination_path),
          stat.S_IWUSR |
          stat.S_IXUSR)
    if os.path.exists(os.path.dirname(destination_path)):
      os.chmod(
          os.path.dirname(destination_path),
          stat.S_IWUSR |
          stat.S_IXUSR)
    copy_local_file(
      source_path,
      destination_path,
      force=replace_existing_file)
    if make_file_and_dir_read_only:
      if os.path.isdir(destination_path):
        # make dir read only
        os.chmod(
          destination_path,
          stat.S_IRUSR |
          stat.S_IRGRP |
          stat.S_IXUSR |
          stat.S_IXGRP)
      elif os.path.isfile(destination_path):
        # make file read only
        os.chmod(
          destination_path,
          stat.S_IRUSR |
          stat.S_IRGRP)
        # make dir read only
        os.chmod(
          os.path.dirname(destination_path),
          stat.S_IRUSR |
          stat.S_IRGRP |
          stat.S_IXUSR |
          stat.S_IXGRP)
  except Exception as e:
    raise ValueError("Failed to copy file to new path, error: {0}".format(e))


def load_data_raw_data_collection(
      db_config_file: str,
      collection_list: list,
      collection_name_key: str = 'collection_name',
      collection_type_key: str = 'collection_type',
      collection_table_key: str = 'collection_table',
      file_path_key: str = 'file_path',
      md5_key: str = 'md5',
      size_key: str = 'size',
      location_key: str = 'location',
      cleanup_existing_collection: bool = False) \
      -> None:
    try:
      check_file_path(db_config_file)
      dbparam = read_dbconf_json(db_config_file)
      ca = CollectionAdaptor(**dbparam)
      ca.start_session()
      fa = FileAdaptor(**{'session': ca.session})
      try:
        collection_data_list = list()
        collection_df = pd.DataFrame(collection_list)
        if collection_name_key not in collection_df.columns or \
           collection_type_key not in collection_df.columns or \
           file_path_key not in collection_df.columns:
          raise KeyError("Missing key in collection entry")
        collection_lookup_columns = [
          collection_name_key,
          collection_type_key]
        unique_collections_list = \
          collection_df[collection_lookup_columns].\
          drop_duplicates().\
          to_dict(orient='records')
        for collection_entry in unique_collections_list:
          collection_name = collection_entry[collection_name_key]
          collection_type = collection_entry[collection_type_key]
          collection_exists = \
            ca.get_collection_files(
              collection_name=collection_name,
              collection_type=collection_type)
          if len(collection_exists.index) > 0 and \
             cleanup_existing_collection:
            remove_data = [{
              "name": collection_name,
              "type": collection_type }]
            ca.remove_collection_group_info(
              data=remove_data,
              autosave=False)
        unique_files_list = \
          collection_df[file_path_key].\
          drop_duplicates().\
          values.\
          tolist()
        for file_path in unique_files_list:
          file_exists = \
            fa.check_file_records_file_path(
              file_path=file_path)
          if file_exists:
            if cleanup_existing_collection:
              fa.remove_file_data_for_file_path(
                file_path=file_path,
                remove_file=False,
                autosave=False)
            else:
              raise ValueError(
                "File {0} already exists in database".format(file_path))
        for entry in collection_list:
          if collection_name_key not in entry or \
             collection_type_key not in entry or \
             file_path_key not in entry:
            raise KeyError("Missing key in collection entry")
          collection_name = entry[collection_name_key]
          collection_type = entry[collection_type_key]
          collection_table = entry[collection_table_key]
          file_path = entry[file_path_key]
          md5 = entry.get(md5_key, None)
          size = entry.get(size_key, None)
          location = entry.get(location_key, None)
          if not os.path.exists(file_path):
            raise ValueError("File {0} does not exist".format(file_path))
          collection_data = {
            'name': collection_name,
            'type': collection_type,
            'table': collection_table,
            'file_path': file_path}
          if md5 is not None:
            collection_data.update({'md5': md5})
          if size is not None:
            collection_data.update({'size': size})
          if location is not None:
            collection_data.update({'location': location})
          collection_data_list.append(collection_data)
        if len(collection_data_list) > 0:
          ca.load_file_and_create_collection(
            data=collection_data_list,
            calculate_file_size_and_md5=False,
            autosave=False)
        else:
          raise ValueError("No collection data to load")
        ca.commit_session()
        ca.close_session()
      except:
        ca.rollback_session()
        ca.close_session()
        raise
    except Exception as e:
      raise ValueError("Failed to load collection, error: {0}".format(e))


def load_raw_files_to_db_and_disk(
      db_config_file: str,
      collection_type: str,
      collection_table: str,
      base_data_path: str,
      file_location: str,
      replace_existing_file: bool,
      cleanup_existing_collection: bool,
      collection_list: list,
      collection_name_key: str = 'collection_name',
      dir_list_key: str = 'dir_list',
      file_list_key: str = 'file_list') \
      -> list:
    try:
      ## TO DO:
      # * get collection name from list
      # * get files from collection_list
      # * get file size
      # * calculate destination path
      # * create dir and cd to dest path
      # * replace existing file if true
      # * copy file to destination path
      # * change dir permission to read and execute for group
      # * change file path permission to read only for group
      # * build file collection
      # * clean up collection if exists and cleanup_existing_collection is True
      # * append file to collection if cleanup_existing_collection is False
      # * return collected file list group [{'collection_name': '', file_list': []}]
      check_file_path(db_config_file)
      ## get collection name and file list from collection_list
      file_collection_list = list()
      for entry in collection_list:
        if collection_name_key not in entry:
          raise KeyError("{0} key not found in collection list".\
                          format(collection_name_key))
        if file_list_key not in entry:
          raise KeyError("{0} key not found in collection list".\
                          format(file_list_key))
        if dir_list_key not in entry:
          raise KeyError("{0} key not found in collection list".\
                          format(dir_list_key))
        collection_name = entry.get(collection_name_key)
        file_list = entry.get(file_list_key)
        dir_list = entry.get(dir_list_key)
        if not isinstance(dir_list, list):
          raise TypeError("dir_list must be a list")
        if len(file_list) == 0:
          raise ValueError("No files found in collection {0}".\
                           format(collection_name))
        for file_list_entry in file_list:
          file_path = file_list_entry.get('file_path')
          file_md5 = file_list_entry.get('md5')
          check_file_path(file_path)
          file_size = os.path.getsize(file_path)
          ## get destination path
          if len(dir_list) == 0:
            destination_path = \
              os.path.join(
                base_data_path,
                os.path.basename(file_path))
          else:
            dir_list = [
              str(f) for f in dir_list]
            destination_path = \
              os.path.join(
                base_data_path,
                *dir_list,
                os.path.basename(file_path))
          ## add to collec list
          file_collection_list.append({
            'collection_name': collection_name,
            'collection_type': collection_type,
            'collection_table': collection_table,
            'file_path': destination_path,
            'md5': file_md5,
            'location': file_location,
            'size': file_size})
          ## check existing path and copy file
          ## TO DO
          copy_or_replace_file_to_disk_and_change_permission(
            source_path=file_path,
            destination_path=destination_path,
            replace_existing_file=replace_existing_file,
            make_file_and_dir_read_only=True)
      ## clean up existing collection and load data to db
      # TO DO
      load_data_raw_data_collection(
        db_config_file=db_config_file,
        collection_list=file_collection_list,
        cleanup_existing_collection=cleanup_existing_collection)
      return file_collection_list
    except Exception as e:
      raise ValueError("Failed to load raw files to db, error: {0}".format(e))


def register_experiment_and_runs_to_db(
      db_config_file: str,
      seqrun_id: str,
      lane_id: int,
      index_group: str,
      sample_group: list) \
      -> list:
    try:
      check_file_path(db_config_file)
      (platform_name, flowcell_id) = \
        get_flatform_name_and_flowcell_id_for_seqrun(
          seqrun_igf_id=seqrun_id,
          db_config_file=db_config_file)
      seqrun_date = \
        get_seqrun_date_from_igf_id(seqrun_id)
      sample_group_with_run_id = list()
      sample_id_list = list()
      exp_data = list()
      run_data = list()
      ## LOOP 1
      for entry in sample_group:
        if 'sample_id' not in entry:
          raise KeyError("Missing key sample_id")
        sample_id = entry.get('sample_id')
        sample_id_list.append(sample_id)
      project_sample_dict = \
        get_project_id_samples_list_from_db(
          sample_igf_id_list=sample_id_list,
          db_config_file=db_config_file)
      ## LOOP 2
      for entry in sample_group:
        if 'sample_id' not in entry:
          raise KeyError("Missing key sample_id")
        sample_id = entry.get('sample_id')
        project_id = project_sample_dict.get(sample_id)
        if project_id is None:
          raise ValueError(
                  "Failed to get project id for sample {0}".\
                  format(sample_id))
        # set library id
        library_id = sample_id
        # calcaulate experiment id
        experiment_id = \
          '{0}_{1}'.format(
            library_id,#
            platform_name)
        # calculate run id
        run_igf_id = \
          '{0}_{1}_{2}'.format(
            experiment_id,
            flowcell_id,
            lane_id)
        library_layout = 'SINGLE'
        for fastq in entry.get('fastq_list'):
          if fastq.endswith('_R2_001.fastq.gz'):
            library_layout = 'PAIRED'
        #sample_group_with_run_id.append({
        #  'project_igf_id': project_id,
        #  'sample_igf_id': sample_id,
        #  'library_id': library_id,
        #  'experiment_igf_id': experiment_id,
        #  'run_igf_id': run_igf_id,
        #  'library_layout': library_layout,
        #  'lane_number': lane_id,
        #  'fastq_list': entry.get('fastq_list')
        #})
        sample_group_with_run_id.append({
          'collection_name': run_igf_id,
          'dir_list': [
            project_id,
            'fastq',
            seqrun_date,
            flowcell_id,
            str(lane_id),
            index_group,
            sample_id],
          'file_list':[{
            'file_path': file_name,
            'md5': file_md5}
              for file_name, file_md5 in entry.get('fastq_list').items()]
          })
        exp_data.append({
          'project_igf_id': project_id,
          'sample_igf_id': sample_id,
          'library_name': library_id,
          'experiment_igf_id': experiment_id,
          'library_layout': library_layout
        })
        run_data.append({
          'experiment_igf_id': experiment_id,
          'run_igf_id': run_igf_id,
          'lane_number': str(lane_id),
          'seqrun_igf_id': seqrun_id
        })
      ## register exp and run data
      filtered_exp_data = list()
      filtered_run_data = list()
      dbparams = read_dbconf_json(db_config_file)
      base = BaseAdaptor(**dbparams)
      base.start_session()
      ea = \
        ExperimentAdaptor(**{'session': base.session})
      ra = \
        RunAdaptor(**{'session': base.session})
      for exp_entry in exp_data:
        if 'experiment_igf_id' not in exp_entry:
          raise KeyError("Missing key experiment_igf_id")
        experiment_exists = \
          ea.check_experiment_records_id(
            exp_entry.get('experiment_igf_id'))
        if not experiment_exists:
          filtered_exp_data.append(exp_entry)
      for run_entry in run_data:
        if 'run_igf_id' not in run_entry:
          raise KeyError("Missing key run_igf_id")
        run_exists = \
          ra.check_run_records_igf_id(
            run_entry.get('run_igf_id'))
        if not run_exists:
          filtered_run_data.append(run_entry)
      try:
        if len(filtered_exp_data) > 0:
          ea.store_project_and_attribute_data(
            data=filtered_exp_data,
            autosave=False)
        base.session.flush()
        if len(filtered_run_data) > 0:
          ra.store_run_and_attribute_data(
            data=filtered_run_data,
            autosave=False)
        base.session.flush()
        base.session.commit()
        base.close_session()
      except:
        base.session.rollback()
        base.close_session()
        raise
      return sample_group_with_run_id
    except Exception as e:
      raise ValueError("Failed to register experiment and runs, error: {0}".format(e))


def load_fastq_and_qc_to_db_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_checksum_sample_group = \
      context['params'].\
        get("xcom_key_for_checksum_sample_group", "checksum_sample_group")
    xcom_task_for_checksum_sample_group = \
      context['params'].\
        get("xcom_task_for_checksum_sample_group")
    #lane_id = context['params'].get('lane_id')
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    xcom_key_for_collection_group = \
      context['params'].\
        get("xcom_key_for_collection_group", "collection_group")
    project_index_column = \
      context['params'].get('project_index_column', 'project_index')
    project_index = \
      context['params'].get('project_index', 0)
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane_index')
    lane_index = \
      context['params'].get('lane_index', 0)
    ig_index_column = \
      context['params'].get('ig_index_column', 'index_group_index')
    ig_index = \
      context['params'].get('ig_index', 0)
    index_group_column = \
      context['params'].get('index_group_column', 'index_group')
    if project_index == 0 or \
       lane_index == 0 or \
       ig_index == 0:
      raise ValueError("project_index, lane_index or ig_index is not set")
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index_column not in df.columns or \
        lane_index_column not in df.columns or \
        ig_index_column not in df.columns:
      raise KeyError(""""
        project_index_column, lane_index_column or
        ig_index_column is not found""")
    ig_df = \
      df[
        (df[project_index_column]==project_index) &
        (df[lane_index_column]==lane_index) &
        (df[ig_index_column]==ig_index)]
    if len(ig_df.index) == 0:
      raise ValueError(
          "No index group found for project {0}, lane {1}, ig {2}".\
          format(project_index, lane_index, ig_index))
    index_group = \
      ig_df[index_group_column].values.tolist()[0]
    checksum_sample_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_checksum_sample_group,
        key=xcom_key_for_checksum_sample_group)
    ## TO DO: fix lane_id
    lane_id = lane_index
    dag_run = context.get('dag_run')
    if dag_run is None or \
       dag_run.conf is None or \
       dag_run.conf.get('seqrun_id') is None:
      raise ValueError('Missing seqrun_id in dag_run.conf')
    seqrun_id = dag_run.conf.get('seqrun_id')
    ## To DO:
    #  for each sample in the sample_group
    #    * get sample_id
    #    * get calculate experiment and run id
    #    * get check library_layout based on R1 and R2 reads
    #    * register experiment and run ids if they are not present
    #    * load fastqs with the run id as collection name
    #    * do these operations in batch mode
    fastq_collection_list = \
      register_experiment_and_runs_to_db(
        db_config_file=DATABASE_CONFIG_FILE,
        seqrun_id=seqrun_id,
        lane_id=lane_id,
        index_group=index_group,
        sample_group=checksum_sample_group)
    ## fastq_collection_list
    #  * [{
    #     'collection_name': '',
    #     'dir_list': [
    #       project_igf_id,
    #       fastq,
    #       run_date,
    #       flowcell_id,
    #       lane_id,
    #       index_group_id,
    #       sample_id],
    #     'file_list': [{'file_name': fastq_files, 'md5': md5}] }]
    file_collection_list = \
      load_raw_files_to_db_and_disk(
        db_config_file=DATABASE_CONFIG_FILE,
        collection_type='demultiplexed_fastq',
        collection_table='run',
        base_data_path= HPC_BASE_RAW_DATA_PATH,
        file_location='HPC_PROJECT',
        collection_list=fastq_collection_list,
        replace_existing_file=True,
        cleanup_existing_collection=True)
    ti.xcom_push(
      key=xcom_key_for_collection_group,
      value=fastq_collection_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_sample_info_from_sample_group(
    worker_index: int,
    sample_group: list) \
    -> list:
  try:
    if len(sample_group) == 0:
      raise ValueError("sample_group is empty")
    df = pd.DataFrame(sample_group)
    if 'worker_index' not in df.columns or \
       'sample_ids' not in df.columns:
      raise KeyError("worker_index or sample_ids is not in sample_group")
    df['worker_index'] = \
      df['worker_index'].astype(int)
    filt_df = df[df['worker_index'] == int(worker_index)]
    if len(filt_df.index) == 0:
      raise ValueError("worker_index {0} is not in sample_group".format(worker_index))
    sample_ids = \
      filt_df['sample_ids'].values.tolist()[0]
    if len(sample_ids) == 0:
      raise ValueError("sample_ids is empty")
    fastq_files_list = list()
    for entry in sample_ids:
      sample_name = entry.get('sample_id')
      fastq_list = entry.get('fastq_list')
      fastq_files_list.append({
        'sample_id': sample_name,
        'fastq_list': fastq_list})
    return fastq_files_list
  except Exception as e:
    raise ValueError("Failed to get sample info from sample group, error: {0}".format(e))


def get_checksum_for_sample_group_fastq_files(
    sample_group: list) \
    -> list:
  try:
    if len(sample_group) == 0:
      raise ValueError("sample_group is empty")
    check_sum_sample_group = list()
    for entry in sample_group:
      sample_id = entry.get("sample_id")
      fastq_list = entry.get("fastq_list")
      fastq_dict = dict()
      for fastq_file in fastq_list:
        fastq_md5 = calculate_file_checksum(fastq_file)
        fastq_dict.update({fastq_file: fastq_md5})
      check_sum_sample_group.append({
        'sample_id': sample_id,
        'fastq_list': fastq_dict})
    return check_sum_sample_group
  except Exception as e:
    raise ValueError("Failed to get checksum for sample group fastq files, error: {0}".format(e))


def calculate_fastq_md5_checksum_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_sample_group = \
      context['params'].\
        get("xcom_key_for_sample_group", "sample_group")
    xcom_task_for_sample_group = \
      context['params'].\
        get("xcom_task_for_sample_group")
    sample_group_id = \
      context['params'].\
        get("sample_group_id")
    xcom_key_for_checksum_sample_group = \
      context['params'].\
        get("xcom_key_for_checksum_sample_group", "checksum_sample_group")
    sample_group = \
      ti.xcom_pull(
        task_ids=xcom_task_for_sample_group,
        key=xcom_key_for_sample_group)
    if sample_group is None or \
       not isinstance(sample_group, list) or \
       len(sample_group) == 0:
      raise ValueError("sample_group is not a list")
    fastq_files_list = \
      get_sample_info_from_sample_group(
        worker_index=sample_group_id,
        sample_group=sample_group)
    sample_with_checksum_list = \
      get_checksum_for_sample_group_fastq_files(
        sample_group=fastq_files_list)
    ti.xcom_push(
      key=xcom_key_for_checksum_sample_group,
      value=sample_with_checksum_list)
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def get_jobs_per_worker(
    max_workers: int,
    total_jobs: int) \
    -> list:
  try:
    job_list = list()
    job_counter = 0
    if max_workers <= 0 or \
       total_jobs <= 0:
      raise ValueError("max_workers or total_jobs is not set")
    while job_counter <= total_jobs:
      for worker_index in range(1, max_workers + 1):
        job_counter += 1
        if job_counter <= total_jobs:
          job_list.append({
            'worker_index': worker_index,
            'jobs': job_counter})
    df = pd.DataFrame(job_list)
    df['jobs'] = df['jobs'].astype('str')
    grp_df = df.groupby('worker_index').agg({'jobs': ','.join}, axis=1)
    grp_df = grp_df.reset_index()
    grp_df['jobs'] = grp_df['jobs'].str.split(',')
    return grp_df.to_dict(orient='records')
  except Exception as e:
    raise ValueError("Failed to divide jobs per worker, error: {0}".format(e))


def get_sample_groups_for_bcl_convert_output(
    samplesheet_file: str,
    max_samples: int = 20) \
    -> list:
  try:
    sa = SampleSheet(samplesheet_file)
    df = pd.DataFrame(sa._data)
    sample_id_list = \
      df['Sample_ID'].drop_duplicates().tolist()
    sample_groups = \
      get_jobs_per_worker(
        max_workers=max_samples,
        total_jobs=len(sample_id_list))
    sample_groups_list = list()
    for s in sample_groups:
      sample_ids = [
        sample_id_list[int(i)-1]
          for i in s.get('jobs')]
      sample_groups_list.append({
        'sample_ids': sample_ids,
        'worker_index': s.get('worker_index')})
    return sample_groups_list
  except Exception as e:
    raise ValueError(
            "Failed to get sample groups for bcl convert output, error: {0}".\
            format(e))


def get_sample_id_and_fastq_path_for_sample_groups(
    samplesheet_file: str,
    lane_id: int,
    bclconv_output_path: str,
    sample_group: list) \
    -> list:
  try:
    check_file_path(samplesheet_file)
    check_file_path(bclconv_output_path)
    ## TO Do the following
    #  * get sample_lists for sample_index from sample_group
    #  * get Sample_Project for each sample_ids (its fail safe)
    #  * get fastq_path for each sample_id assuming following path
    #      bclconv_output_path/Sample_Project/sample_id_S\d+_L\d+_[RIU][1-4]_001.fastq.gz
    sa = SampleSheet(samplesheet_file)
    samplesheet_df = pd.DataFrame(sa._data)
    samplesheet_df = \
      samplesheet_df[['Sample_ID', 'Sample_Project']].\
      drop_duplicates()
    formatted_sample_group = list()
    for entry in sample_group:
      worker_index = entry.get('worker_index')
      new_sample_groups_list = list()
      sample_groups_list = entry.get('sample_ids')
      for sample_id in sample_groups_list:
        fastq_file_regexp = \
          r'{0}_S\d+_L00{1}_[RIU][1-4]_001.fastq.gz'.\
          format(sample_id, lane_id)
        filt_df = \
          samplesheet_df[samplesheet_df['Sample_ID']==sample_id].fillna('')
        if len(filt_df.index)==0:
          raise ValueError("Sample_ID {0} not found in samplesheet file {1}".\
                           format(sample_id, samplesheet_file))
        sample_project = \
          filt_df['Sample_Project'].values.tolist()[0]
        base_fastq_path = \
          os.path.join(
            bclconv_output_path,
            sample_project)
        fastq_list_for_sample = list()
        for file_name in os.listdir(base_fastq_path):
          if re.search(fastq_file_regexp, file_name):
            fastq_list_for_sample.\
              append(
                os.path.join(
                  base_fastq_path,
                  file_name))
        new_sample_groups_list.\
          append({
            'sample_id': sample_id,
            'sample_project': sample_project,
            'fastq_list': fastq_list_for_sample})
      formatted_sample_group.append({
        'worker_index': worker_index,
        'sample_ids': new_sample_groups_list})
    return formatted_sample_group
  except Exception as e:
    raise ValueError("Failed to get sample fastq path for sample groups, error: {0}".format(e))


def sample_known_qc_factory_func(**context):
  try:
    ti = context['ti']
    samplesheet_file_suffix = \
      context['params'].\
        get("samplesheet_file_suffix", "Reports/SampleSheet.csv")
    xcom_key_for_bclconvert_output = \
      context['params'].\
        get("xcom_key_for_bclconvert_output", "bclconvert_output")
    xcom_task_for_bclconvert_output = \
      context['params'].\
        get("xcom_task_for_bclconvert_output", None)
    max_samples = \
      context['params'].\
        get("max_samples", 0)
    lane_index = \
      context['params'].\
        get("lane_index")
    xcom_key_for_sample_group = \
      context['params'].\
        get("xcom_key_for_sample_group", "sample_group")
    next_task_prefix = \
      context['params'].get("next_task_prefix")
    bclconvert_output_dir = \
      ti.xcom_pull(
        task_ids=xcom_task_for_bclconvert_output,
        key=xcom_key_for_bclconvert_output)
    if bclconvert_output_dir is None:
      raise ValueError(
              f"Failed to get bcl convert output dir for task {xcom_task_for_bclconvert_output}")
    if max_samples == 0:
      raise ValueError("max_samples is not set")
    samplesheet_path = \
      os.path.join(
        bclconvert_output_dir,
        samplesheet_file_suffix)
    sample_groups_list = \
      get_sample_groups_for_bcl_convert_output(
        samplesheet_file=samplesheet_path,
        max_samples=max_samples)
    sample_group_with_fastq_path = \
      get_sample_id_and_fastq_path_for_sample_groups(
        samplesheet_file=samplesheet_path,
        lane_id=int(lane_index),
        bclconv_output_path=bclconvert_output_dir,
        sample_group=sample_groups_list)
    df = pd.DataFrame(sample_group_with_fastq_path)
    if 'worker_index' not in df.columns:
      raise ValueError("worker_index is not set")
    ti.xcom_push(
      key=xcom_key_for_sample_group,
      value=sample_group_with_fastq_path)
    sample_id_list = \
      df['worker_index'].\
      drop_duplicates().\
      values.tolist()
    task_list = list()
    for sample_id in sample_id_list:
      task_list.append(
        "{0}{1}".format(
          next_task_prefix,
          sample_id))
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise

def generate_bclconvert_report(
  seqrun_path: str,
  image_path: str,
  report_template: str,
  bclconvert_report_library_path: str,
  bclconvert_reports_path: str,
  dry_run: bool = False) \
  -> str:
  try:
    check_file_path(seqrun_path)
    check_file_path(image_path)
    check_file_path(report_template)
    check_file_path(bclconvert_reports_path)
    check_file_path(bclconvert_report_library_path)
    temp_run_dir = get_temp_dir()
    interop_dir = os.path.join(seqrun_path, 'InterOp')
    runinfo_xml = os.path.join(seqrun_path, 'RunInfo.xml')
    check_file_path(runinfo_xml)
    index_metric_bin = \
      os.path.join(
        bclconvert_reports_path,
        'IndexMetricsOut.bin')
    check_file_path(interop_dir)
    check_file_path(index_metric_bin)
    copy_local_file(
      interop_dir,
      os.path.join(temp_run_dir, 'InterOp'))
    copy_local_file(
      runinfo_xml,
      os.path.join(temp_run_dir, 'RunInfo.xml'))
    copy_local_file(
      index_metric_bin,
      os.path.join(
        temp_run_dir,
        'InterOp',
        'IndexMetricsOut.bin'),
      force=True)
    input_params = {
      'DATE_TAG': get_date_stamp(),
      'SEQRUN_IGF_ID': os.path.basename(seqrun_path.strip('/')),
      'REPORTS_DIR': bclconvert_reports_path,
      'RUN_DIR': temp_run_dir}
    container_bind_dir_list = [
      temp_run_dir,
      bclconvert_reports_path,
      bclconvert_report_library_path]
    temp_dir = get_temp_dir()
    nb = \
      Notebook_runner(
        template_ipynb_path=report_template,
        output_dir=temp_dir,
        input_param_map=input_params,
        container_paths=container_bind_dir_list,
        kernel='python3',
        use_ephemeral_space=True,
        singularity_options=['--no-home','-C', "--env", "PYTHONPATH={0}".format(bclconvert_report_library_path)],
        allow_errors=False,
        singularity_image_path=image_path,
        dry_run=dry_run)
    output_notebook_path, _ = \
      nb.execute_notebook_in_singularity()
    return output_notebook_path
  except Exception as e:
    raise ValueError(
            "Failed to generate bclconvert report, error: {0}".format(e))


def bclconvert_report_func(**context):
  try:
    ti = context['ti']
    xcom_key_for_reports = \
      context['params'].get('xcom_key_for_reports', 'bclconvert_reports')
    xcom_task_for_reports = \
      context['params'].get('xcom_task_for_reports', None)
    bclconvert_reports_path = \
      ti.xcom_pull(
        key=xcom_key_for_reports,
        task_ids=xcom_task_for_reports)
    dag_run = context.get('dag_run')
    seqrun_path = None
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise IOError("Failed to get seqrun_id from dag_run")
    report_file = \
      generate_bclconvert_report(
        seqrun_path=seqrun_path,
        image_path=INTEROP_NOTEBOOK_IMAGE,
        report_template=BCLCONVERT_REPORT_TEMPLATE,
        bclconvert_report_library_path=BCLCONVERT_REPORT_LIBRARY,
        bclconvert_reports_path=bclconvert_reports_path)
    copy_local_file(
      report_file,
      os.path.join(
        bclconvert_reports_path,
        os.path.basename(report_file)))
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def bclconvert_singularity_wrapper(
      image_path: str,
      input_dir: str,
      output_dir: str,
      samplesheet_file: str,
      bcl_num_conversion_threads: int = 1,
      bcl_num_compression_threads: int = 1,
      bcl_num_decompression_threads: int = 1,
      bcl_num_parallel_tiles: int = 1,
      lane_id : int = 0,
      tile_id_list: tuple = (),
      first_tile_only: bool = False,
      dry_run: bool = False) \
      -> str:
  try:
    check_file_path(image_path)
    check_file_path(input_dir)
    if os.path.exists(output_dir):
      raise ValueError(f"Output directory {output_dir} already exists")
    check_file_path(samplesheet_file)
    temp_dir = get_temp_dir(use_ephemeral_space=True)
    bclconvert_cmd = [
      "bcl-convert",
      "--bcl-input-directory", input_dir,
      "--output-directory", output_dir,
      "--sample-sheet", samplesheet_file,
      "--bcl-num-conversion-threads", str(bcl_num_conversion_threads),
      "--bcl-num-compression-threads", str(bcl_num_compression_threads),
      "--bcl-num-decompression-threads", str(bcl_num_decompression_threads),
      "--bcl-num-parallel-tiles", str(bcl_num_parallel_tiles),
      "--bcl-sampleproject-subdirectories", "true",
      "--strict-mode", "true"]
    if first_tile_only:
      bclconvert_cmd.\
        extend(["--first-tile-only", "true"])
    if lane_id > 0:
      bclconvert_cmd.\
        extend(["--bcl-only-lane", str(lane_id)])
    if len(tile_id_list) > 0:
      bclconvert_cmd.\
        extend(["--tiles", ",".join(tile_id_list)])
    bclconvert_cmd = \
      ' '.join(bclconvert_cmd)
    bind_paths = [
      '{0}:/var/log'.format(temp_dir),
      os.path.dirname(samplesheet_file),
      input_dir,
      os.path.dirname(output_dir)]
    cmd = execute_singuarity_cmd(
      image_path=image_path,
      command_string=bclconvert_cmd,
      bind_dir_list=bind_paths,
      dry_run=dry_run)
    return cmd
  except:
    raise

def run_bclconvert_func(**context):
  try:
    ti = context['ti']
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project_index')
    project_index = \
      context['params'].get('project_index', 0)
    project_column = \
      context['params'].get('project_column', 'project')
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane_index')
    lane_column = \
      context['params'].get('lane_column', 'lane')
    lane_index = \
      context['params'].get('lane_index', 0)
    ig_index_column = \
      context['params'].get('ig_index_column', 'index_group_index')
    index_group_column = \
      context['params'].get('index_group_column', 'index_group')
    ig_index = \
      context['params'].get('ig_index', 0)
    samplesheet_column = \
      context['params'].get('samplesheet_column', 'samplesheet_file')
    xcom_key_for_reports = \
      context['params'].get('xcom_key_for_reports', 'bclconvert_reports')
    xcom_key_for_output = \
      context['params'].get('xcom_key_for_output', 'bclconvert_output')
    bcl_num_conversion_threads = \
      context['params'].get('bcl_num_conversion_threads', '1')
    bcl_num_compression_threads = \
      context['params'].get('bcl_num_compression_threads', '1')
    bcl_num_decompression_threads = \
      context['params'].get('bcl_num_decompression_threads', '1')
    bcl_num_parallel_tiles = \
      context['params'].get('bcl_num_parallel_tiles', '1')
    dag_run = context.get('dag_run')
    seqrun_path = ''
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
    else:
      raise IOError("Failed to get seqrun_id from dag_run")
    if project_index == 0 or \
       lane_index == 0 or \
       ig_index == 0:
      raise ValueError('project_index, lane_index or ig_index is not set')
    if xcom_key is None or \
       xcom_task is None:
      raise ValueError('xcom_key or xcom_task is not set')
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index_column not in df.columns or \
        lane_index_column not in df.columns or \
        lane_column not in df.columns or \
        ig_index_column not in df.columns or \
        samplesheet_column not in df.columns:
      raise KeyError(""""
        project_index_column, lane_index_column, lane_column,
        ig_index_column or samplesheet_column is not found""")
    ig_df = \
      df[
        (df[project_index_column]==project_index) &
        (df[lane_index_column]==lane_index) &
        (df[ig_index_column]==ig_index)]
    if len(ig_df.index) == 0:
      raise ValueError(
          "No samplesheet found for project {0}, lane {1}, ig {2}".\
          format(project_index, lane_index, ig_index))
    samplesheet_file = \
      ig_df[samplesheet_column].values.tolist()[0]
    output_dir = \
      ig_df['output_dir'].values.tolist()[0]
    project_id = \
      ig_df[project_column].values.tolist()[0]
    lane_id = \
      ig_df[lane_column].values.tolist()[0]
    ig_id = \
      ig_df[index_group_column].values.tolist()[0]
    output_temp_dir = \
      get_temp_dir(use_ephemeral_space=True)
    demult_dir = \
      os.path.join(
        output_temp_dir,
        '{0}_{1}_{2}'.format(project_id, lane_id, ig_id))
    cmd = \
      bclconvert_singularity_wrapper(
        image_path=BCLCONVERT_IMAGE,
        input_dir=seqrun_path,
        output_dir=demult_dir,
        samplesheet_file=samplesheet_file,
        bcl_num_conversion_threads=int(bcl_num_conversion_threads),
        bcl_num_compression_threads=int(bcl_num_compression_threads),
        bcl_num_decompression_threads=int(bcl_num_decompression_threads),
        bcl_num_parallel_tiles=int(bcl_num_parallel_tiles),
        lane_id=int(lane_id))
    check_file_path(demult_dir)    # check if the output dir exists
    check_file_path(
      os.path.join(
        demult_dir,
        'Reports',
        'Demultiplex_Stats.csv'))  # check if the demultiplex stats file exists
    bclconvert_output_dir = \
      os.path.join(
        output_dir,
        '{0}_{1}_{2}'.format(
          project_id,
          lane_id,
          ig_id))                  # output dir for bclconvert
    reports_dir = \
      os.path.join(
        bclconvert_output_dir,
        'Reports')                 # output dir for bclconvert reports
    copy_local_file(
      demult_dir,
      bclconvert_output_dir)       # copy the output dir to the output dir
    check_file_path(reports_dir)   # check if the reports dir exists after copy
    ti.xcom_push(
      key=xcom_key_for_reports,
      value=reports_dir)
    ti.xcom_push(
      key=xcom_key_for_output,
      value=bclconvert_output_dir)
    message = \
      'Finished demultiplexing project {0}, lane {1}, ig {2} - cmd: {3}'.\
      format(project_id, lane_id, ig_id, cmd)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def trigger_ig_jobs(**context):
  try:
    ti = context['ti']
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project_index')
    project_index = \
      context['params'].get('project_index', 0)
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane_index')
    lane_index = \
      context['params'].get('lane_index', 0)
    ig_task_prefix = \
      context['params'].get('ig_task_prefix')
    max_index_groups = \
      context['params'].get('max_index_groups')
    ig_index_column = \
      context['params'].get('ig_index_column', 'index_group_index')
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    if len(formatted_samplesheets_list) == 0:
      raise ValueError(
              "No samplesheet found for seqrun {0}".\
              format(context['dag_run'].conf.get('seqrun_id')))
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    if project_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                     format(project_index_column))
    if lane_index == 0 :
      raise ValueError("Invalid lane index 0")
    if lane_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(lane_index_column))
    if ig_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(ig_index_column))
    df[project_index_column] = df[project_index_column].astype(int)
    df[lane_index_column] = df[lane_index_column].astype(int)
    df[ig_index_column] = df[ig_index_column].astype(int)
    project_df = df[df[project_index_column] == int(project_index)]
    lane_df = project_df[project_df[lane_index_column] == int(lane_index)]
    if len(lane_df.index) == 0:
      raise ValueError("No samplesheet found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    ig_counts = \
      lane_df[ig_index_column].\
      drop_duplicates().\
      values.\
      tolist()
    if len(ig_counts) == 0:
      raise ValueError("No index group found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    if len(ig_counts) > int(max_index_groups):
      raise ValueError("Too many index groups found for project {0}, lane {1}".\
                       format(project_index, lane_index))
    task_list = [
      '{0}{1}'.format(ig_task_prefix, ig)
         for ig in ig_counts]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def trigger_lane_jobs(**context):
  try:
    ti = context['ti']
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    xcom_task = \
      context['params'].get('xcom_task', 'format_and_split_samplesheet')
    project_index_column = \
      context['params'].get('project_index_column', 'project_index')
    project_index = \
      context['params'].get('project_index', 0)
    lane_index_column = \
      context['params'].get('lane_index_column', 'lane_index')
    lane_task_prefix = \
      context['params'].get('lane_task_prefix')
    max_lanes = \
      context['params'].get('max_lanes', 0)
    formatted_samplesheets_list = \
      ti.xcom_pull(task_ids=xcom_task, key=xcom_key)
    if len(formatted_samplesheets_list) == 0:
      raise ValueError(
              "No samplesheet found for seqrun {0}".\
              format(context['dag_run'].conf.get('seqrun_id')))
    df = pd.DataFrame(formatted_samplesheets_list)
    if project_index == 0 :
      raise ValueError("Invalid projext index 0")
    if project_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                     format(project_index_column))
    if lane_index_column not in df.columns:
      raise KeyError("Column {0} not found in samplesheet".\
                      format(lane_index_column))
    df[project_index_column] = df[project_index_column].astype(int)
    project_df = df[df[project_index_column] == int(project_index)]
    lane_counts = \
      project_df[lane_index_column].\
      drop_duplicates().\
      values.tolist()
    if len(lane_counts) == 0:
      raise ValueError("No lane found for project {0}".\
                      format(project_index))
    if len(lane_counts) > int(max_lanes):
      raise ValueError("Too many lanes {0} found for project {1}".\
                      format(lane_counts, project_index))
    task_list = [
      '{0}{1}'.format(lane_task_prefix, lane_count)
        for lane_count in lane_counts]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def format_and_split_samplesheet_func(**context):
  try:
    ti = context['ti']
    xcom_key = \
      context['params'].get('xcom_key', 'formatted_samplesheets')
    max_projects = \
      context['params'].get('max_projects', 0)
    project_task_prefix = \
      context['params'].get('project_task_prefix', 'demult_start_project_')
    dag_run = context.get('dag_run')
    task_list = ['mark_run_finished',]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      samplesheet_file = \
        os.path.join(seqrun_path, 'SampleSheet.csv')
      runinfo_xml_file = \
        os.path.join(seqrun_path, 'RunInfo.xml')
      check_file_path(samplesheet_file)
      samplesheet_dir = \
        get_temp_dir(use_ephemeral_space=True)
      formatted_samplesheets_list = \
        _get_formatted_samplesheets(
          samplesheet_file=samplesheet_file,
          runinfo_xml_file=runinfo_xml_file,
          samplesheet_output_dir=samplesheet_dir,
          singlecell_barcode_json=SINGLECELL_BARCODE_JSON,
          singlecell_dual_barcode_json=SINGLECELL_DUAL_BARCODE_JSON)
      ti.xcom_push(
        key=xcom_key,
        value=formatted_samplesheets_list)
      project_indices = \
        pd.DataFrame(formatted_samplesheets_list)['project_index'].\
        drop_duplicates().values.tolist()
      if len(project_indices) > max_projects:
        raise ValueError(
                "Too many projects {0}. Increase MAX_PROJECTS param from {1}".\
                format(project_indices, max_projects))
      task_list = [
        '{0}{1}'.format(project_task_prefix,project_index)
          for project_index in project_indices]
      if len(task_list) == 0:
        log.warning(
          "No project indices found in samplesheet {0}".\
            format(samplesheet_file))
        task_list = ['mark_run_finished']
    else:
      log.warning("No seqrun_id found in dag_run conf")
      task_list = ['mark_run_finished']
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _get_formatted_samplesheets(
  samplesheet_file: str,
  runinfo_xml_file: str,
  samplesheet_output_dir: str,
  singlecell_barcode_json: str,
  singlecell_dual_barcode_json: str,
  tenx_sc_tag: str='10X') -> list:
  try:
    check_file_path(samplesheet_file)
    check_file_path(runinfo_xml_file)
    check_file_path(samplesheet_output_dir)
    check_file_path(singlecell_barcode_json)
    check_file_path(singlecell_dual_barcode_json)
    lane_in_samplesheet = True
    formatted_samplesheets_list = list()
    temp_dir = \
      get_temp_dir()
    # 10X sc daul index conversion
    sc_dual_process = \
      ProcessSingleCellDualIndexSamplesheet(
        samplesheet_file=samplesheet_file,
        singlecell_dual_index_barcode_json=singlecell_dual_barcode_json,
        platform='MISEQ',
        index2_rule='NOCHANGE')
    temp_sc_dual_conv_samplesheet_file = \
      os.path.join(temp_dir, 'sc_dual_index_samplesheet.csv')
    sc_dual_process.\
      modify_samplesheet_for_sc_dual_barcode(
        output_samplesheet=temp_sc_dual_conv_samplesheet_file)
    # 10x sc single index conversion
    sc_data = \
      ProcessSingleCellSamplesheet(
        temp_sc_dual_conv_samplesheet_file,
        singlecell_barcode_json,
        tenx_sc_tag)
    temp_sc_conv_samplesheet_file = \
      os.path.join(temp_dir, 'sc_index_samplesheet.csv')
    sc_data.\
      change_singlecell_barcodes(temp_sc_conv_samplesheet_file)
    sa = SampleSheet(temp_sc_conv_samplesheet_file)
    if 'Lane' not in sa._data_header:
      lane_in_samplesheet = False
      ra = RunInfo_xml(runinfo_xml_file)
      lanes_count = \
        ra.get_lane_count()
    # project and lanes
    formatted_project_and_lane = list()
    for row in sa.get_project_and_lane():
      if lane_in_samplesheet:
        (project_name, lane_id) = row.split(':')
        formatted_project_and_lane.\
          append({
            'project_name': project_name.strip(),
            'lane': lane_id.strip()})
      else:
        (project_name,) = row.split(':')
        for lane_id in range(1, lanes_count+1):
          formatted_project_and_lane.\
            append({
              'project_name': project_name.strip(),
              'lane': lane_id})
    # samplesheet group list
    project_counter = 0
    for project_name, p_data in pd.DataFrame(formatted_project_and_lane).groupby('project_name'):
      project_counter += 1
      lane_counter = 0
      for lane_id, _ in p_data.groupby('lane'):
        lane_counter += 1
        sa = SampleSheet(temp_sc_conv_samplesheet_file)
        sa.filter_sample_data(
          condition_key="Sample_Project",
          condition_value=project_name,
          method="include")
        if lane_in_samplesheet:
          sa.filter_sample_data(
            condition_key="Lane",
            condition_value=lane_id,
            method="include")
        ig_counter = 0
        for ig, ig_sa in sa.group_data_by_index_length().items():
          unfiltered_ig_data = deepcopy(ig_sa._data)
          if 'Description' in ig_sa._data_header:
            df = pd.DataFrame(ig_sa._data)
            description_list = \
              df['Description'].\
              map(lambda x: x.upper()).\
              drop_duplicates().\
              values.tolist()
            for desc_item in description_list:
              ig_counter += 1
              ig_sa._data = deepcopy(unfiltered_ig_data)
              ig_sa.filter_sample_data(
                condition_key="Description",
                condition_value=desc_item,
                method="include")
              if desc_item == '':
                desc_item = 'NA'
              samplesheet_name = \
                'SampleSheet_{0}_{1}_{2}_{3}.csv'.\
                format(
                  project_name,
                  lane_id,
                  ig,
                  desc_item)
              ig_samplesheet_temp_path = \
                os.path.join(
                  temp_dir,
                  samplesheet_name)
              ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
              ig_sa.\
                print_sampleSheet(ig_samplesheet_temp_path)
              bases_mask = \
                _calculate_bases_mask(
                  samplesheet_file=ig_samplesheet_temp_path,
                  runinfoxml_file=runinfo_xml_file,
                  read_offset_cutoff=29)
              ig_final_sa = SampleSheet(ig_samplesheet_temp_path)
              ig_final_sa.\
                set_header_for_bclconvert_run(bases_mask=bases_mask)
              temp_dir = get_temp_dir(use_ephemeral_space=True)
              ig_final_sa.\
                print_sampleSheet(ig_samplesheet_path)
              formatted_samplesheets_list.\
                append({
                  'project': project_name,
                  'project_index': project_counter,
                  'lane': lane_id,
                  'lane_index': lane_counter,
                  'bases_mask': bases_mask,
                  'index_group': '{0}_{1}'.format(ig, desc_item),
                  'index_group_index': ig_counter,
                  'samplesheet_file': ig_samplesheet_path,
                  'output_dir': temp_dir})
          else:
            ig_counter += 1
            samplesheet_name = \
              'SampleSheet_{0}_{1}_{2}.csv'.\
              format(
                project_name,
                lane_id,
                ig)
            ig_samplesheet_temp_path = \
                os.path.join(
                  temp_dir,
                  samplesheet_name)
            ig_samplesheet_path = \
                os.path.join(
                  samplesheet_output_dir,
                  samplesheet_name)
            ig_sa.\
              print_sampleSheet(ig_samplesheet_temp_path)
            bases_mask = \
              _calculate_bases_mask(
                samplesheet_file=ig_samplesheet_temp_path,
                runinfoxml_file=runinfo_xml_file,
                read_offset_cutoff=29)
            ig_final_sa = SampleSheet(ig_samplesheet_temp_path)
            ig_final_sa.\
              set_header_for_bclconvert_run(bases_mask=bases_mask)
            ig_final_sa.\
              print_sampleSheet(ig_samplesheet_path)
            temp_dir = get_temp_dir(use_ephemeral_space=True)
            formatted_samplesheets_list.\
              append({
                'project': project_name,
                'project_index': project_counter,
                'lane': lane_id,
                'lane_index': lane_counter,
                'bases_mask': bases_mask,
                'index_group': ig,
                'index_group_index': ig_counter,
                'samplesheet_file': ig_samplesheet_path,
                'output_dir': temp_dir})
    return formatted_samplesheets_list
  except Exception as e:
    raise ValueError(
            "Failed to get formatted samplesheets and bases mask, error: {0}".\
            format(e))


def _calculate_bases_mask(
  samplesheet_file: str,
  runinfoxml_file: str,
  numcycle_label: str='numcycles',
  isindexedread_label: str='isindexedread',
  isreversecomplement_label: str='isreversecomplement',
  read_offset: int=1,
  read_offset_cutoff: int=50) -> str:
  try:
    samplesheet_data = SampleSheet(infile=samplesheet_file)
    index_length_stats = samplesheet_data.get_index_count()
    samplesheet_index_length_list = list()
    for index_name in index_length_stats.keys():
      index_type = len(index_length_stats.get(index_name).keys())
      if index_type > 1:
        raise ValueError(f'column {index_type} has variable lengths')
      index_length = list(index_length_stats.get(index_name).keys())[0]
      samplesheet_index_length_list.\
        append(index_length)
    runinfo_data = RunInfo_xml(xml_file=runinfoxml_file)
    runinfo_reads_stats = runinfo_data.get_reads_stats()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id].get(numcycle_label))
      if runinfo_reads_stats[read_id][isindexedread_label] == 'N':
        if int(runinfo_read_length) < read_offset_cutoff:
          read_offset = 0
    index_read_position = 0
    bases_mask_list = list()
    for read_id in (sorted(runinfo_reads_stats.keys())):
      runinfo_read_length = int(runinfo_reads_stats[read_id].get(numcycle_label))
      if runinfo_reads_stats[read_id][isindexedread_label] == 'Y':
        samplesheet_index_length = \
          samplesheet_index_length_list[index_read_position]
        index_diff = \
          int(runinfo_read_length) - int(samplesheet_index_length)
        if samplesheet_index_length == 0:
          bases_mask_list.\
            append(f'N{runinfo_read_length}')
          if index_read_position == 0:
            raise ValueError("Index 1 position can't be zero")
        elif index_diff > 0 and \
             samplesheet_index_length > 0:
          if runinfo_reads_stats[read_id].get(isreversecomplement_label) is not None and \
             runinfo_reads_stats[read_id].get(isreversecomplement_label) == 'Y':
            bases_mask_list.\
              append(f'N{index_diff}I{samplesheet_index_length}')
          else:
            bases_mask_list.\
              append(f'I{samplesheet_index_length}N{index_diff}')
        elif index_diff == 0:
          bases_mask_list.\
            append(f'I{samplesheet_index_length}')
        index_read_position += 1
      else:
        if int(read_offset) > 0:
          bases_mask_list.\
            append('Y{0}N{1}'.format(int(runinfo_read_length) - int(read_offset), read_offset))
        else:
          bases_mask_list.\
            append(f'Y{runinfo_read_length}')
    if len(bases_mask_list) < 2:
      raise ValueError("Missing bases mask values")
    return ';'.join(bases_mask_list)
  except Exception as e:
    raise ValueError(
            f"Failed to calculate bases mask, error: {e}")


def find_seqrun_func(**context):
  try:
    dag_run = context.get('dag_run')
    next_task_id = \
      context['params'].\
      get('next_task_id', 'mark_seqrun_as_running')
    no_work_task = \
      context['params'].\
      get('no_work_task', 'no_work')
    task_list = [no_work_task,]
    if dag_run is not None and \
       dag_run.conf is not None and \
       dag_run.conf.get('seqrun_id') is not None:
      seqrun_id = \
        dag_run.conf.get('seqrun_id')
      seqrun_path = \
        os.path.join(HPC_SEQRUN_BASE_PATH, seqrun_id)
      run_status = \
        _check_for_required_files(
          seqrun_path,
          file_list=[
            'RunInfo.xml',
            'RunParameters.xml',
            'SampleSheet.csv',
            'Data/Intensities/BaseCalls',
            'InterOp'])
      if run_status:
        _check_and_load_seqrun_to_db(
          seqrun_id=seqrun_id,
          seqrun_path=seqrun_path,
          dbconf_json_path=DATABASE_CONFIG_FILE)
        #seed_status = \
        #  _check_and_seed_seqrun_pipeline(
        #    seqrun_id=seqrun_id,
        #    pipeline_name=context['task'].dag_id,
        #    dbconf_json_path=DATABASE_CONFIG_FILE)
      if run_status:
        task_list = [next_task_id,]
    return task_list
  except Exception as e:
    log.error(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=e,
      reaction='fail')
    raise


def _check_for_required_files(seqrun_path: str, file_list: list) -> bool:
  try:
    for file_name in file_list:
      file_path = os.path.join(seqrun_path, file_name)
      if not os.path.exists(file_path):
        return False
    return True
  except Exception as e:
    raise IOError("Failed to get required files for seqrun {0}".format(seqrun_path))


def _check_and_load_seqrun_to_db(
    seqrun_id: str,
    seqrun_path: str,
    dbconf_json_path: str,
    runinfo_file_name: str = 'RunInfo.xml') \
    -> None:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    sra = SeqrunAdaptor(**dbconf)
    sra.start_session()
    run_exists = \
      sra.check_seqrun_exists(seqrun_id)
    if not run_exists:
      runinfo_file = os.path.join(seqrun_path, runinfo_file_name)
      runinfo_data = RunInfo_xml(xml_file=runinfo_file)
      platform_name = runinfo_data.get_platform_number()
      flowcell_id = runinfo_data.get_flowcell_name()
      seqrun_data = [{
        'seqrun_igf_id': seqrun_id,
        'platform_igf_id': platform_name,
        'flowcell_id': flowcell_id }]
      sra.store_seqrun_and_attribute_data(
        data=seqrun_data,
        autosave=True)
    sra.close_session()
  except Exception as e:
    raise ValueError(
            "Failed to load seqrun {0} to database, error: {1}".\
            format(seqrun_id, e))


def _check_and_seed_seqrun_pipeline(
    seqrun_id: str,
    pipeline_name: str,
    dbconf_json_path: str,
    seed_status: str = 'SEEDED',
    seed_table: str ='seqrun',
    no_change_status: str = 'RUNNING') -> bool:
  try:
    dbconf = read_dbconf_json(dbconf_json_path)
    base = BaseAdaptor(**dbconf)
    base.start_session()
    sra = SeqrunAdaptor(**{'session': base.session})
    seqrun_entry = \
      sra.fetch_seqrun_records_igf_id(
          seqrun_igf_id=seqrun_id)
    pa = PipelineAdaptor(**{'session': base.session})
    seed_status = \
      pa.create_or_update_pipeline_seed(
        seed_id=seqrun_entry.seqrun_id,
        pipeline_name=pipeline_name,
        new_status=seed_status,
        seed_table=seed_table,
        no_change_status=no_change_status)
    base.close_session()
    return seed_status
  except Exception as e:
    raise ValueError(
            "Faild to seed pipeline {0} for seqrun {1}, error: {2}".\
            format(pipeline_name, seqrun_id, e))