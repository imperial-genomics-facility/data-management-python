#!/usr/bin/env python
import argparse
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.process.data_transfer.sync_seqrun_data_on_remote import Sync_seqrun_data_from_remote

parser = argparse.ArgumentParser()
parser.add_argument('-r','--remote_server', required=True, help='Remote server address')
parser.add_argument('-p','--remote_base_path', required=True, help='Seqrun directory path in remote dir')
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-o','--output_dir', required=True, help='Local output directory path')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')

args = parser.parse_args()
remote_server = args.remote_server
remote_base_path = args.remote_base_path
dbconfig = args.dbconfig
output_dir = args.output_dir
slack_config = args.slack_config

if __name__=='__main__':
  try:
    slack_obj=IGF_slack(slack_config=slack_config)
    ## FIX ME
  except Exception as e:
    message = 'Error while syncing sequencing run directory from remote server: {0}'.format(e)
    slack_obj.post_message_to_channel(message,reaction='fail')
    raise ValueError(message)
