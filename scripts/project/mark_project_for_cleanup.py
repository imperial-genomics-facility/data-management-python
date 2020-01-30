#!/usr/bin/env python

################################################################################
#
# A script for marking project as withdrawn in IGF database and fetching list of
# files for clean up
#
# USAGE: 
#   python mark_project_for_cleanup.py
#     -p PROJECTNAME
#     -d /path/DBCONF_FILE
#     -o /path/OUTPUT_DIR
#
# This script creates two files for each project
#  * A file containing all the run or experiment level files in HPC
#  * A file containing the IRODs dir for the project
#
################################################################################
import argparse
from igf_data.utils.projectutils import mark_project_and_list_files_for_cleanup

parser = argparse.ArgumentParser()
parser.add_argument('-p','--project_igf_id', required=True, help='Project igf id for cleanup')
parser.add_argument('-d','--dbconfig_file', required=True, help='Database configuration file path')
parser.add_argument('-o','--output_dir', required=True, help='Output dir to write files lists')
args = parser.parse_args()

project_igf_id = args.project_igf_id
dbconfig_file = args.dbconfig_file
output_dir = args.output_dir

if __name__=='__main__':
  try:
    mark_project_and_list_files_for_cleanup(
      project_igf_id=project_igf_id,
      dbconfig_file=dbconfig_file,
      outout_dir=output_dir)
  except Exception as e:
    raise ValueError("Failed to clean up project, error: {0}".format(e))