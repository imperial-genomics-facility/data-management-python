#!/usr/bin/env python
import argparse
from igf_data.process.seqrun_processing.find_and_register_new_project_data import Find_and_register_new_project_data

parser = argparse.ArgumentParser()
parser.add_argument('-p','--projet_info_path', required=True, help='Project metdata directory path')
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-t','--user_account_template', required=True, help='User account information email template file path')
parser.add_argument('-s','--log_slack', default=False, action='store_true', help='Toggle slack logging')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
parser.add_argument('-c','--check_hpc_user', default=False, action='store_true', help='Toggle HPC user checking')
parser.add_argument('-u','--hpc_user', required=True, help='HPC user name for ldap server checking')
parser.add_argument('-a','--hpc_address', required=True, help='HPC address for ldap server checking')
parser.add_argument('-l','--ldap_server', required=True, help='Ldap server address')
parser.add_argument('-i','--setup_irods', default=False, action='store_true', help='Setup iRODS account for user')
parser.add_argument('-m','--notify_user', default=False, action='store_true', help='Notify user about new account and password')
args = parser.parse_args()

projet_info_path = args.projet_info_path
dbconfig = args.dbconfig
user_account_template = args.user_account_template
log_slack = args.log_slack
slack_config = args.slack_config
check_hpc_user = args.check_hpc_user
hpc_user = args.hpc_user
hpc_address = args.hpc_address
ldap_server = args.ldap_server
setup_irods = args.setup_irods
notify_user = args.notify_user

if __name__=='__main__':
  try:
    fa = \
      Find_and_register_new_project_data(
        projet_info_path=projet_info_path,
        dbconfig=dbconfig,
        user_account_template=user_account_template,
        log_slack=log_slack,
        slack_config=slack_config,
        check_hpc_user=check_hpc_user,
        hpc_user=hpc_user,
        hpc_address=hpc_address,
        ldap_server=ldap_server,
        setup_irods=setup_irods,
        notify_user=notify_user)
    fa.process_project_data_and_account()
  except Exception as e:
    raise ValueError('ERROR: {0}'.format(e))