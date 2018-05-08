import argparse, os, warnings
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.projectutils import mark_project_barcode_check_off

parser=argparse.ArgumentParser()
parser.add_argument('-p','--projet_id_list', required=True, help='A file path listing project_igf_id')
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-s','--log_slack', default=False, action='store_true', help='Toggle slack logging')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
args=parser.parse_args()


projet_id_list=args.projet_id_list
dbconfig=args.dbconfig
log_slack=args.log_slack
slack_config=args.slack_config

if log_slack:
  slack_obj=IGF_slack(slack_config=slack_config)

try:
  if not os.path.exists(projet_id_list):
    raise IOError('File {0} not found'.format(projet_id_list))

  dbparam=read_dbconf_json(dbconfig)
  base = BaseAdaptor(**dbparam)
  project_id_list=list()
  failed_id_list=list()
  with open(projet_id_list,'r') as fp:
    project_id_list=[i.strip() for i in fp]                                     # reading list of project ids from the file

  if log_slack:
    message='Found {0} new projet ids to switch off barcode checking'.\
            format(len(project_id_list))
    slack_obj.post_message_to_channel(message, reaction='pass')

  for project_id in project_id_list:
    try:
      mark_project_barcode_check_off(project_igf_id=project_id,
                                     session_class=base.get_session_class(),
                                     barcode_check_attribute='barcode_check')   # change project attribute
      message='Switching off barcode checking for project {0}'.\
              format(project_id)
      if log_slack:
        slack_obj.post_message_to_channel(message, reaction='pass')

    except Exception as e:
      failed_id_list.apend(project_id)                                          # add project id to the failed list
      message='Failed to reset barcode checking as OFF for project {0}'.\
              format(project_id)
      if log_slack:
        slack_obj.post_message_to_channel(message, reaction='fail')

    with open(projet_id_list,'w') as fp:
      fp.write('\n'.join(failed_id_list))                                       # over write input list with None or with failed ids
except Exception as e:
  message='Error: {0}'.format(e)
  if log_slack:
    slack_obj.post_message_to_channel(message, reaction='fail')