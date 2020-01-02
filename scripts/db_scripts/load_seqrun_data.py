#!/usr/bin/env python
import argparse
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.seqrunutils import load_new_seqrun_data


parser = argparse.ArgumentParser()
parser.add_argument('-p','--seqrun_data', required=True, help='Seqrun data json file')
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
args = parser.parse_args()

dbconfig_path = args.dbconfig_path
slack_config = args.slack_config
seqrun_data = args.seqrun_data

slack_obj = IGF_slack(slack_config=slack_config)

if __name__=='__main__':
  try:
    load_new_seqrun_data(data_file=seqrun_data, dbconfig=dbconfig_path)
  except Exception as e:
    message = 'Failed to load data to seqrun table, error: {0}'.format(e)
    slack_obj.post_message_to_channel(message,reaction='fail')
    raise ValueError(message)
  else:
    slack_obj.post_message_to_channel(message='Loaded new seqrun info to db',reaction='pass')