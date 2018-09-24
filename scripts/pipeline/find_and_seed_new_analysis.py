#!/usr/bin/env python
import argparse,os
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.pipelineutils import find_new_analysis_seeds

'''
A script for finding new experiment entries for seeding analysis pipeline

:usage: find_and_seed_new_analysis.py 
        [-h]
        -d DBCONFIG_PATH
        -s SLACK_CONFIG
        -p PIPELINE_NAME
        -t FASTQ_TYPE
        -f PROJECT_NAME_FILE
        [-m SPECIES_NAME]
        [-l LIBRARY_SOURCE]
        [-r]

:parameters:
  -h, --help            show this help message and exit
  -d , --dbconfig_path DBCONFIG_PATH
                        Database configuration json file
  -s , --slack_config SLACK_CONFIG
                        Slack configuration json file
  -p , --pipeline_name PIPELINE_NAME
                        IGF pipeline name
  -t , --fastq_type FASTQ_TYPE
                        Fastq collection type
  -f , --project_name_file PROJECT_NAME_FILE
                        File containing project names for seeding analysis
                        pipeline
  -m , --species_name SPECIES_NAME
                        Species name to filter analysis
  -l , --library_source LIBRARY_SOURCE
                        Library source to filter analysis
  -r , --reset_project_list
                        Clean up project info file
'''

parser=argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
parser.add_argument('-p','--pipeline_name', required=True, help='IGF pipeline name')
parser.add_argument('-t','--fastq_type', required=True, help='Fastq collection type')
parser.add_argument('-f','--project_name_file', required=True, help='File containing project names for seeding analysis pipeline')
parser.add_argument('-m','--species_name', action='append', default=None, help='Species name to filter analysis')
parser.add_argument('-l','--library_source', action='append', default=None, help='Library source to filter analysis')
parser.add_argument('-r','--reset_project_list', default=False, action='store_true', help='Clean up project info file')
args=parser.parse_args()

dbconfig_path=args.dbconfig_path
slack_config=args.slack_config
pipeline_name=args.pipeline_name
fastq_type=args.fastq_type
project_name_file=args.project_name_file
species_name=args.species_name
library_source=args.library_source
reset_project_list=args.reset_project_list

try:
  if not os.path.exists(project_name_file):
    raise IOError('File {0} not found'.\
                  format(project_name_file))

  slack_obj=IGF_slack(slack_config=slack_config)                                # get slack instance
  available_projects=find_new_analysis_seeds(\
                       dbconfig_path=dbconfig_path,
                       pipeline_name=pipeline_name,
                       project_name_file=project_name_file,
                       species_name_list=species_name,
                       fastq_type=fastq_type,
                       library_source_list=library_source
                     )
  if available_projects is not None:
    message='New projects available for seeding: {0}'.\
            format(available_projects)
    slack_obj.\
    post_message_to_channel(\
      message=message,
      reaction='pass')                                                          # post list of active projects to slack

  if reset_project_list:
    with open(project_name_file,'w') as fp:
      fp.write('')
    message='Resetting project list file: {0}'.\
            format(project_name_file)
    slack_obj.\
    post_message_to_channel(\
      message=message,
      reaction='pass')

except Exception as e:
  message='Error: {0}'.format(e)
  print(message)
  slack_obj.\
  post_message_to_channel(\
    message=message,
    reaction='fail')