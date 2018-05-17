import argparse,os
from igf_data.process.metadata.experiment_metadata_updator import Experiment_metadata_updator

parser=argparse.ArgumentParser()
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
args=parser.parse_args()

dbconfig=args.dbconfig
slack_config=args.slack_config

try:
  emu=Experiment_metadata_updator(dbconfig_file=dbconfig,
                                  log_slack=True,
                                  slack_config=slack_config)
  emu.update_metadta_from_sample_attribute()                                    # update all metadata
except Exception as e:
  print('Error: {0}'.format(e))