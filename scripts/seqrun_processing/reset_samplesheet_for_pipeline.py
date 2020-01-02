#!/usr/bin/env python
import argparse
from igf_data.process.seqrun_processing.reset_samplesheet_md5 import Reset_samplesheet_md5

parser = argparse.ArgumentParser()
parser.add_argument('-p','--seqrun_path', required=True, help='Sequencing run directory path')
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
parser.add_argument('-a','--asana_config', required=True, help='Asana configuration file path')
parser.add_argument('-i','--asana_project_id', required=True, help='Asana project id')
parser.add_argument('-f','--input_list', required=True, help='Sequencing run id list file')
args = parser.parse_args()

seqrun_path = args.seqrun_path
dbconfig = args.dbconfig
slack_config = args.slack_config
asana_config = args.asana_config
asana_project_id = args.asana_project_id
input_list = args.input_list

if __name__=='__main__':
  try:
    rs = \
      Reset_samplesheet_md5(
        seqrun_path=seqrun_path,
        seqrun_igf_list=input_list,
        dbconfig_file=dbconfig,
        log_slack=True,
        log_asana=True,
        slack_config=slack_config,
        asana_project_id=asana_project_id,
        asana_config=asana_config)
    rs.run()
  except Exception as e:
    raise ValueError('Error: {0}'.format(e))