import os, json
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
    raise ValueError("Failed to create json dump, error: {0}".format(e))