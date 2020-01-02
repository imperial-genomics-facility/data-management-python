import argparse
import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.igfTables import Project
from igf_data.utils.fileutils import check_file_path


parser = argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-o','--output_path', required=True, help='Output project list file')
args = parser.parse_args()
dbconfig_path = args.dbconfig_path
output_path = args.output_path

if __name__=='__main__':
  try:
    check_file_path(dbconfig_path)
    dbparam = read_dbconf_json(dbconfig_path)
    pa = ProjectAdaptor(**dbparam)
    pa.start_session()
    project_list = pa.fetch_all_project_igf_ids()
    pa.close_session()
    if not isinstance(project_list,pd.DataFrame):
      raise ValueError(
              "Expecting a Pandas DataFrame, got: {0}".\
                format(type(project_list)))
    project_list.to_csv(output_path,index=False)
  except Exception as e:
    raise ValueError("Failed to fetch project ids, error: {0}".format(e))