import argparse
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.pipelineutils import load_new_pipeline_data

parser=argparse.ArgumentParser()
parser.add_argument('-p','--pipeline_data', required=True, help='Pipeline data json file')
parser.add_argument('-u','--update', default=False, action='store_true', help='Update existing platform data, default: False')
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
args=parser.parse_args()


dbconfig_path=args.dbconfig_path
slack_config=args.slack_config
pipeline_data=args.pipeline_data
update_data=args.update

slack_obj=IGF_slack(slack_config=slack_config)

try:
  if update_data:
    raise NotImplementedError('methods notavailable for updaing existing data')
  else:
    load_new_pipeline_data(data_file=pipeline_data, dbconfig=dbconfig_path)
except Exception as e:
  message='Failed to load data to pipeline table, error: {0}'.format(e)
  slack_obj.post_message_to_channel(message,reaction='fail')
  raise
else:
  slack_obj.post_message_to_channel(message='Loaded new pipeline info to db',reaction='pass')



