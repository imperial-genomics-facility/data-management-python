import argparse, json
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.dbutils import clean_and_rebuild_database

parser=argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
args=parser.parse_args()

dbconfig_path=args.dbconfig_path
slack_config=args.slack_config

slack_obj=IGF_slack(slack_config=slack_config)

try:
  clean_and_rebuild_database(dbconfig=dbconfig_path)
  slack_obj.post_message_to_channel(message='All old data removed from database and new tables are created',reaction='pass')
except Exception as e:
  message='Failed to remove old data and create new tables, error: {0}'.format(e)
  slack_obj.post_message_to_channel(message,reaction='fail')
  raise

  
