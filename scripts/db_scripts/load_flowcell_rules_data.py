#!/usr/bin/env python
import argparse
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.platformutils import load_new_flowcell_data

parser = argparse.ArgumentParser()
parser.add_argument('-f','--flowcell_data', required=True, help='Flowcell rules data json file')
parser.add_argument('-u','--update', default=False, action='store_true', help='Update existing flowcell rules data, default: False')
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
args = parser.parse_args()

dbconfig_path = args.dbconfig_path
slack_config = args.slack_config
flowcell_data = args.flowcell_data
update_data = args.update

slack_obj = IGF_slack(slack_config=slack_config)

if __name__=='__main__':
  try:
    if update_data:
      raise NotImplementedError('methods notavailable for updaing existing data')
    else:
      load_new_flowcell_data(data_file=flowcell_data, dbconfig=dbconfig_path)
  except Exception as e:
    message = 'Failed to load data to flowcell rules table, error: {0}'.format(e)
    slack_obj.post_message_to_channel(message,reaction='fail')
    raise ValueError(message)