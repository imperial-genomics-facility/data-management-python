import os, re, json, logging
import pandas as pd
from igf_data.igfdb.igfTables import Project
from igf_data.igfdb.igfTables import User
from igf_data.igfdb.igfTables import ProjectUser
from igf_data.igfdb.igfTables import Sample
from igf_data.igfdb.igfTables import Platform
from igf_data.igfdb.igfTables import Flowcell_barcode_rule
from igf_data.igfdb.igfTables import Seqrun
from igf_data.igfdb.igfTables import Seqrun_stats
from igf_data.igfdb.igfTables import Experiment
from igf_data.igfdb.igfTables import Run
from igf_data.igfdb.igfTables import Analysis
from igf_data.igfdb.igfTables import Collection
from igf_data.igfdb.igfTables import File
from igf_data.igfdb.igfTables import Collection_group
from igf_data.igfdb.igfTables import Pipeline
from igf_data.igfdb.igfTables import Pipeline_seed
from igf_data.igfdb.igfTables import Project_attribute
from igf_data.igfdb.igfTables import Experiment_attribute
from igf_data.igfdb.igfTables import Collection_attribute
from igf_data.igfdb.igfTables import Sample_attribute
from igf_data.igfdb.igfTables import Seqrun_attribute
from igf_data.igfdb.igfTables import Run_attribute
from igf_data.igfdb.igfTables import File_attribute
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir, copy_local_file, check_file_path
from igf_portal.api_utils import upload_files_to_portal
from igf_portal.api_utils import get_data_from_portal

def get_db_data_and_create_json_dump(dbconfig_json, output_json_path):
  try:
    dbparam = read_dbconf_json(dbconfig_json)
    base = BaseAdaptor(**dbparam)
    table_list = [
      File_attribute,
      File,
      Collection_attribute,
      Collection,
      Collection_group,
      Pipeline_seed,
      Pipeline,
      Analysis,
      Platform,
      Flowcell_barcode_rule,
      Seqrun,
      Seqrun_stats,
      Run_attribute,
      Run,
      Experiment_attribute,
      Experiment,
      Sample_attribute,
      Sample,
      Project_attribute,
      Project,
      User,
      ProjectUser]
    db_data = dict()
    base.start_session()
    if os.path.exists(output_json_path):
      raise IOError(
              "Output file {0} already present, remove it before rerunning the script".\
                format(output_json_path))
    temp_dir = get_temp_dir()
    temp_json_output = \
      os.path.join(temp_dir, 'metadata.json')
    for table in table_list:
      data = \
      base.fetch_records(
        query=base.session.query(table),
        output_mode='dataframe')
      if table.__tablename__=='project':
        data['start_timestamp'] = \
          data['start_timestamp'].astype(str)
      if table.__tablename__=='user':
        data['date_created'] = \
          data['date_created'].astype(str)
      if table.__tablename__=='sample':
        data['date_created'] = \
          data['date_created'].astype(str)
      if table.__tablename__=='experiment':
        data['date_created'] = \
          data['date_created'].astype(str)
      if table.__tablename__=='run':
        data['date_created'] = \
          data['date_created'].astype(str)
      if table.__tablename__=='pipeline':
        data['date_stamp'] = \
          data['date_stamp'].astype(str)
      if table.__tablename__=='pipeline_seed':
        data['date_stamp'] = \
          data['date_stamp'].astype(str)
      if table.__tablename__=='collection':
        data['date_stamp'] = \
          data['date_stamp'].astype(str)
      if table.__tablename__=='file':
        data['date_created'] = \
          data['date_created'].astype(str)
        data['date_updated'] = \
          data['date_updated'].astype(str)
      if table.__tablename__=='seqrun':
        data['date_created'] = \
          data['date_created'].astype(str)
      if table.__tablename__=='platform':
        data['date_created'] = \
          data['date_created'].astype(str)
      db_data.update({
        table.__tablename__: data.to_dict(orient="records")})
    with open(temp_json_output, "w") as fp:
      json.dump(db_data, fp)
    check_file_path(temp_json_output)
    copy_local_file(
      temp_json_output,
      output_json_path)
  except Exception as e:
    raise ValueError(
            "Failed to create json dump, error: {0}".\
              format(e))


def _get_metadata_csv_files_from_metadata_dir(metadata_dir):
  try:
    check_file_path(metadata_dir)
    samplesheet_pattern = \
      re.compile('\S+_SampleSheet\S+', re.IGNORECASE)
    reformatted_pattern = \
      re.compile('\S+_reformatted\S+', re.IGNORECASE)
    formatted_csv_files = dict()
    raw_csv_files = dict()
    for root, _, files in os.walk(metadata_dir):
      for f in files:
        file_path = os.path.join(root, f)
        if not re.match(samplesheet_pattern, f):
          # detected metadata file
          if re.match(reformatted_pattern, f):
            # detected formatted csv
            name = f.replace('_reformatted.csv', '')
            formatted_csv_files.update({name: file_path})
          else:
            # detected raw csv
            name = f.replace('.csv', '')
            raw_csv_files.update({name: file_path})
    diff_raw = \
      list(set(raw_csv_files.keys()).difference(set(formatted_csv_files)))
    diff_formatted = \
      list(set(formatted_csv_files.keys()).difference(set(raw_csv_files)))
    if len(diff_raw) > 0 or \
       len(diff_formatted) > 0:
      raise ValueError(
              "Failed to get matching raw and formatted csv files. Missing formatted: {0}. Missing raw: {1}".\
                format(diff_raw, diff_formatted))
    final_metadata_dict = dict()
    for project_id in raw_csv_files.keys():
      raw_csv = raw_csv_files.get(project_id)
      formatted_csv = formatted_csv_files.get(project_id)
      final_metadata_dict.\
        update({
          project_id: {
            'raw_csv': raw_csv,
            'formatted_csv': formatted_csv}})
    return final_metadata_dict
  except Exception as e:
    raise ValueError(
            "Failed to get metadata csv files from archive dir, error: {0}".\
              format(e))


def _check_for_existing_raw_metadata_on_portal_db(project_list, portal_conf_file):
  try:
    new_projects = list()
    temp_dir = get_temp_dir()
    temp_list_file = \
      os.path.join(temp_dir, 'project_list.json')
    with open(temp_list_file, 'w') as fp:
      json_data = {'project_list': project_list}
      json.dump(json_data, fp)
    res = \
      upload_files_to_portal(
        portal_config_file=portal_conf_file,
        file_path=temp_list_file,
        url_suffix='/api/v1/raw_metadata/search_new_metadata')
    new_projects = res.get('new_projects')
    if new_projects is not None and \
       isinstance(new_projects, str) and \
       len(new_projects.split(',')) > 0:
      new_projects = \
        new_projects.split(',')
    return new_projects
  except Exception as e:
    raise ValueError(
            "Failed to check for existing raw metadata on portal db, error: {0}".\
              format(e))


def _create_json_for_metadata_upload(project_list, metadata_dict):
  try:
    temp_dir = get_temp_dir()
    temp_json = os.path.join(temp_dir, 'metadata.json')
    json_data = list()
    for project_id in project_list:
      project_data = metadata_dict.get(project_id)
      if project_data is None:
        raise KeyError(
                "Project {0} not found in the metadata list".\
                  format(project_id))
      raw_csv = project_data.get('raw_csv')
      formatted_csv = project_data.get('formatted_csv')
      if raw_csv is None or \
         formatted_csv is None:
        raise KeyError(
                "Raw or formatted csv file not found for ptoject {0}".\
                  format(project_id))
      check_file_path(raw_csv)
      check_file_path(formatted_csv)
      raw_csv_data = \
        pd.read_csv(raw_csv).to_dict(orient='records')
      formatted_csv_data = \
        pd.read_csv(formatted_csv).to_dict(orient='records')
      json_data.append({
        "metadata_tag": project_id,
        "raw_csv_data": raw_csv_data,
        "formatted_csv_data": formatted_csv_data})
    with open(temp_json, 'w') as jp:
      json.dump(json_data, jp)
    return temp_json
  except Exception as e:
    raise ValueError(
            "Failed to create json for raw metadata upload, error: {0}".\
              format(e))

def _add_new_raw_metadata_to_portal(portal_conf_file, json_file):
  try:
    check_file_path(json_file)
    res = \
      upload_files_to_portal(
        portal_config_file=portal_conf_file,
        file_path=json_file,
        url_suffix='/api/v1/raw_metadata/add_metadata')
    return res
  except Exception as e:
    raise ValueError(
            "Failed to upload raw metadata to portal, error: {0}".\
              format(e))


def get_raw_metadata_from_lims_and_load_to_portal(metadata_dir, portal_conf_file):
  try:
    final_metadata_dict = \
      _get_metadata_csv_files_from_metadata_dir(
        metadata_dir=metadata_dir)
    project_list = final_metadata_dict.keys()
    if len(project_list) > 0:
      new_projects = \
        _check_for_existing_raw_metadata_on_portal_db(
          project_list=project_list,
          portal_conf_file=portal_conf_file)
      if len(new_projects) > 0:
        json_file = \
          _create_json_for_metadata_upload(
            project_list=new_projects,
            metadata_dict=final_metadata_dict)
        res = \
          _add_new_raw_metadata_to_portal(
            portal_conf_file=portal_conf_file,
            json_file=json_file)
        logging.info(res)
  except Exception as e:
    raise ValueError(
            "Failed to load raw metadata to portal, error: {0}".\
              format(e))

