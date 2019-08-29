import argparse, os
from datetime import datetime
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.seqrun_processing.find_and_process_new_seqrun import find_new_seqrun_dir, calculate_file_md5, load_seqrun_files_to_db, seed_pipeline_table_for_new_seqrun,check_for_registered_project_and_sample,validate_samplesheet_for_seqrun

parser=argparse.ArgumentParser()
parser.add_argument('-p','--seqrun_path', required=True, help='Seqrun directory path')
parser.add_argument('-m','--md5_path', required=True, help='Seqrun md5 output dir')
parser.add_argument('-d','--dbconfig_path', required=True, help='Database configuration json file')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration json file')
parser.add_argument('-a','--asana_config', required=True, help='Asana configuration json file')
parser.add_argument('-i','--asana_project_id', required=True, help='Asana project id')
parser.add_argument('-n','--pipeline_name', required=True, help='IGF pipeline name')
parser.add_argument('-j','--samplesheet_json_schema', required=True, help='JSON schema for samplesheet validation')
parser.add_argument('-e','--exclude_path', action='append', default=[], help='List of sub directories excluded from the search')
args=parser.parse_args()

seqrun_path=args.seqrun_path
md5_path=args.md5_path
dbconfig_path=args.dbconfig_path
slack_config=args.slack_config
asana_config=args.asana_config
asana_project_id=args.asana_project_id
pipeline_name=args.pipeline_name
exclude_path=args.exclude_path
samplesheet_json_schema=args.samplesheet_json_schema

slack_obj=IGF_slack(slack_config=slack_config)
asana_obj=IGF_asana(asana_config=asana_config, asana_project_id=asana_project_id)

try:
  new_seqruns=find_new_seqrun_dir(seqrun_path, dbconfig_path)
  new_seqruns,message=check_for_registered_project_and_sample(seqrun_info=new_seqruns,\
                                                              dbconfig=dbconfig_path)
  if message !='':
    msg_tmp_dir=get_temp_dir()                                                  # create temp dir
    time_tuple=datetime.now().timetuple()                                       # get timetuple for NOW
    time_stamp='{0}_{1}_{2}-{3}_{4}_{5}'.\
               format(time_tuple.tm_year,
                      time_tuple.tm_mon,
                      time_tuple.tm_mday,
                      time_tuple.tm_hour,
                      time_tuple.tm_min,
                      time_tuple.tm_sec)
    file_name='samplesheet_metadata_check_failed_{0}.txt'.format(time_stamp)
    file_name=os.path.join(msg_tmp_dir,file_name)
    with open(file_name,'w') as fp:
      fp.write(message)                                                         # write message file for slack
    message='samplesheet metadata check message : {0}'.format(time_stamp)
    slack_obj.post_file_to_channel(filepath=file_name,\
                                   message=message)                             # post samplesheet metadata check results to slack
    remove_dir(msg_tmp_dir)                                                     # remove temp dir

  if len(new_seqruns.keys()) > 0:
    temp_dir=get_temp_dir()                                                     # create temp dir
    new_seqruns,error_files=validate_samplesheet_for_seqrun(seqrun_info=new_seqruns,\
                                                            schema_json=samplesheet_json_schema,\
                                                            output_dir=temp_dir)# validate samplesheet for seqruns
    if len(error_files.keys())>0:
      for seqrun_name, error_file_path in error_files.items():
        message='Samplesheet validation failed for run {0}'.format(seqrun_name)
        slack_obj.post_file_to_channel(filepath=error_file_path,\
                                       message=message)                         # post validation results to slack

    remove_dir(temp_dir)                                                        # remove temp dir

  if len(new_seqruns.keys()) > 0:
    message='found {0} new sequence runs, calculating md5'.format(len(new_seqruns.keys()))
    slack_obj.post_message_to_channel(message,reaction='pass')

    new_seqrun_files_and_md5=calculate_file_md5(seqrun_info=new_seqruns, md5_out=md5_path, seqrun_path=seqrun_path, exclude_dir=exclude_path)
    slack_obj.post_message_to_channel(message='finished md5 calculation, loading seqrun to db',reaction='pass')

    load_seqrun_files_to_db(seqrun_info=new_seqruns, seqrun_md5_info=new_seqrun_files_and_md5, dbconfig=dbconfig_path)
    seed_pipeline_table_for_new_seqrun(pipeline_name=pipeline_name, dbconfig=dbconfig_path)

    for seqrun_name in new_seqruns.keys():
      message='found new sequencing run {0}'.format(seqrun_name)
      res=asana_obj.comment_asana_task(task_name=seqrun_name, comment=message)
      slack_obj.post_message_to_channel(message,reaction='pass')
      message='New asana task created for seqrun {0}, url: https://app.asana.com/0/{1}/{2}'.format(seqrun_name, asana_project_id, res['target']['gid'])
      slack_obj.post_message_to_channel(message,reaction='pass')
  else:
    slack_obj.post_message_to_channel(message='No new sequencing run found',reaction='sleep')
except Exception as e:
  message='Failed to load new seqruns, received following error: {0}'.format(e)
  slack_obj.post_message_to_channel(message,reaction='fail')
  raise
