from re import A

from sqlalchemy.orm import eagerload
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
      try:
        igf_slack = IGF_slack(slack_conf)
        igf_slack.\
          post_message_to_channel(
            message=message,
            reaction=reaction)
      except Exception as e:
        logging.warn('Failed to upload message to slack, error: {0}'.format(e))
    if asana_conf is not None and \
       asana_project_id is not None and \
       project_id is not None:
      message = \
        'Dag id: {0}, Task id: {1}, Comment: {2}'.\
          format(dag_id,task_id,comment)
      try:
        igf_asana = \
          IGF_asana(
            asana_config=asana_conf,
            asana_project_id=asana_project_id)
        igf_asana.\
          comment_asana_task(
            task_name=project_id,
            comment=message)
      except Exception as e:
        logging.warn('Failed to add comment to Asana, error: {0}'.format(e))
    if ms_teams_conf is not None:
      try:
        message = \
          '**Dag id**: `{0}`, **Task id**: `{1}`, **Project**: `{2}`, **Comment**: `{3}`'.\
            format(dag_id,task_id,project_id,comment)
        igf_ms = \
          IGF_ms_team(
            webhook_conf_file=ms_teams_conf)
        igf_ms.\
          post_message_to_team(
            message=message,
            reaction=reaction)
      except Exception as e:
        logging.warn('Failed to send message to MS Teams channel, error: {0}'.format(e))
    time.sleep(2)
  except Exception as e:
    logging.warn('Failed to log, error: {0}'.format(e))


def log_success(context):
  try:
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
  except Exception as e:
    logging.warn('Faile to log success, error: {0}'.format(e))


def log_failure(context):
  try:
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
  except Exception as e:
    logging.warn('Failed to log failure, error: {0}'.format(e))


def log_sleep(context):
  try:
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
  except Exception as e:
    logging.warn('Failed to log sleep, error: {0}'.format(e))


def post_image_to_channels(
  image_file, remote_filename=None, slack_conf=None, asana_conf=None,
  ms_teams_conf=None, task_id=None, dag_id=None, asana_project_id=None,
  project_id=None, comment=None, reaction=''):
  try:
    if slack_conf is not None:
      message = \
        'Dag id: {0}, Task id: {1}, Project: {2}, Comment: {3}'.\
          format(dag_id,task_id,project_id,comment)
      igf_slack = IGF_slack(slack_conf)
      igf_slack.\
        post_file_to_channel(
          filepath=image_file,
          message=message)
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
        attach_file_to_asana_task(
          task_name=project_id,
          filepath=image_file,
          remote_filename=remote_filename,
          comment=message)
    if ms_teams_conf is not None:
      message = \
        '**Dag id**: `{0}`, **Task id**: `{1}`, **Project**: `{2}`, **Comment**: `{3}`'.\
          format(dag_id,task_id,project_id,comment)
      igf_ms = \
        IGF_ms_team(
          webhook_conf_file=ms_teams_conf)
      igf_ms.\
        post_image_to_team(
          image_path=image_file,
          message=message,
          reaction='')
    time.sleep(2)
  except Exception as e:
    logging.warn('Failed to upload image, error: {0}'.format(e))