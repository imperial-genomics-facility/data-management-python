import argparse,os
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.pipelineutils import find_new_analysis_seeds

parser=argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
parser.add_argument('-p','--pipeline_name', required=True, help='IGF pipeline name')
parser.add_argument('-t','--fastq_type', required=True, help='Fastq collection type')
parser.add_argument('-f','--project_name_file', required=True, help='File containing project names for seeding analysis pipeline')
parser.add_argument('-m','--species_name', action='append', default=None, help='Species name to filter analysis')
parser.add_argument('-l','--library_source', action='append', default=None, help='Library source to filter analysis')
args=parser.parse_args()

dbconfig_path=args.dbconfig_path
slack_config=args.slack_config
pipeline_name=args.pipeline_name
fastq_type=args.fastq_type
project_name_file=args.project_name_file
species_name=args.species_name
library_source=args.library_source

try:
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
      reaction='pass')
except Exception as e:
  message='Error: {0}'.format(e)
  slack_obj.\
  post_message_to_channel(\
    message=message,
    reaction='fail')