import argparse
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.process.project_info.project_pooling_info import Project_pooling_info

parser=argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
parser.add_argument('-o','--output', required=True, help='Gviz json output path')
args=parser.parse_args()

dbconfig=args.dbconfig
slack_config=args.slack_config
output=args.output

try:
  slack_obj=IGF_slack(slack_config=slack_config)
  pp = Project_pooling_info(dbconfig_file=dbconfig)
  pp.fetch_db_data_and_prepare_gviz_json(output_file_path=output)
  message = 'Updated project pooling stats'
  slack_obj.\
    post_message_to_channel(\
      message=message,
      reaction='pass')
except Exception as e:
  message = 'Failed to updated project pooling stats'
  slack_obj.\
    post_message_to_channel(\
      message=message,
      reaction='fail')
  raise