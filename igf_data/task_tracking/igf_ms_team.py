import os,requests,json
from igf_data.utils.dbutils import read_json_data

class IGF_ms_team:
  def __init__(self,webhook_conf_file):
    try:
      webhook_conf =  read_json_data(webhook_conf_file)
    except Exception as e:
      raise ValueError(
        "Failed to parse webhook config file {0}, error: {1}".\
          format(webhook_conf_file,e))
    self.webhook_conf = webhook_conf

  def post_message_to_team(message,reaction=''):
    try:
      formatted_message = ''
      if reaction !='' or reaction is not None:
        if reaction == 'pass':
          reaction = ''
        elif reaction == 'fail':
          reaction = ''
        elif reaction == 'sleep':
          reaction = ''
        formatted_message = "{0}; {1}".format(reaction,message)
      else:
        formatted_message = message
      print(formatted_message)
    except Exception as e:
      raise ValueError('Failed to send message to team')
      