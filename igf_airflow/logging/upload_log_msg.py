from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_ms_team import IGF_ms_team
import logging,time

def send_log_to_channels(
      slack_conf=None,asana_conf=None,ms_teams_conf=None,task_id=None,
      dag_id=None,asana_project_id=None,project_id=None,comment=None,reaction=''):
  try:
    if slack_conf is not None:
      message = \
        'Dag id: {0}, Task id: {1}, Project: {2}, Comment: {3}'.\
          format(dag_id,task_id,project_id,comment)
      igf_slack = IGF_slack(slack_conf)
      igf_slack.\
        post_message_to_channel(
          message=message,
          reaction=reaction)
    if asana_conf is not None and \
       asana_project_id is not None and \
       project_id is not None:
      message = \
        'Dag id: {0}, Task id: {1}, Comment: {2}'.\
          format(dag_id,task_id,comment)
      igf_asana = \
        IGF_asana(
          asana_config=asana_conf,
          asana_project_id=asana_project_id)
      igf_asana.\
        comment_asana_task(
          task_name=project_id,
          comment=message)
    if ms_teams_conf is not None:
      message = \
        '**Dag id**: {0}, **Task id**: {1}, **Project**: {2}, **Comment**: {3}'.\
          format(dag_id,task_id,project_id,comment)
      igf_ms = \
        IGF_ms_team(
          webhook_conf_file=ms_teams_conf)
      igf_ms.\
        post_message_to_team(
          message=message,
          reaction=reaction)
    time.sleep(2)
  except Exception as e:
    logging.error('Failed to log, error: {0}'.format(e))
    pass

def log_success(context):
  slack_conf = context['var']['value'].get('slack_conf')
  asana_conf = context['var']['value'].get('asana_conf')
  ms_teams_conf = context['var']['value'].get('ms_teams_conf')
  task_id = context['task'].task_id
  dag_id = context['task'].dag_id
  asana_project_id = context['params'].get('asana_project_id')
  project_id = context['params'].get('project_id')
  comment = context['params'].get('comment')
  send_log_to_channels(
    slack_conf=slack_conf,
    asana_conf=asana_conf,
    ms_teams_conf=ms_teams_conf,
    dag_id=dag_id,
    task_id=task_id,
    asana_project_id=asana_project_id,
    project_id=project_id,
    comment=comment,
    reaction='pass')

def log_failure(context):
  slack_conf = context['var']['value'].get('slack_conf')
  asana_conf = context['var']['value'].get('asana_conf')
  ms_teams_conf = context['var']['value'].get('ms_teams_conf')
  task_id = context['task'].task_id
  dag_id = context['task'].dag_id
  project_id = context['params'].get('project_id')
  comment = context['params'].get('comment')
  send_log_to_channels(
    slack_conf=slack_conf,
    asana_conf=asana_conf,
    ms_teams_conf=ms_teams_conf,
    task_id=task_id,
    dag_id=dag_id,
    asana_project_id=None,
    project_id=project_id,
    comment=comment,
    reaction='fail')

def log_sleep(context):
  slack_conf = context['var']['value'].get('slack_conf')
  asana_conf = context['var']['value'].get('asana_conf')
  ms_teams_conf = context['var']['value'].get('ms_teams_conf')
  task_id = context['task'].task_id
  dag_id = context['task'].dag_id
  project_id = context['params'].get('project_id')
  comment = context['params'].get('comment')
  send_log_to_channels(
    slack_conf=slack_conf,
    asana_conf=asana_conf,
    ms_teams_conf=ms_teams_conf,
    task_id=task_id,
    dag_id=dag_id,
    asana_project_id=None,
    project_id=project_id,
    comment=comment,
    reaction='sleep')