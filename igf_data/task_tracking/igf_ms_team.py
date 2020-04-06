import os,requests,json,time
from igf_data.utils.dbutils import read_json_data

class IGF_ms_team:
  def __init__(self,webhook_conf_file):
    try:
      webhook_conf =  read_json_data(webhook_conf_file)[0]
    except Exception as e:
      raise ValueError(
        "Failed to parse webhook config file {0}, error: {1}".\
          format(webhook_conf_file,e))
    self.webhook_conf = webhook_conf

  def post_message_to_team(self,title,message,reaction=''):
    try:
      webhook_url = self.webhook_conf.get('webhook_url')
      formatted_message = ''
      themeColor = '#FFFFFF'
      if reaction !='' or reaction is not None:
        if reaction == 'pass':
          reaction =  '&#x2705;'
          themeColor = '#00cc44'
        elif reaction == 'fail':
          reaction = '&#x274C'
          themeColor = '#DC143C'
        elif reaction == 'sleep':
          reaction = '&#128564'
          themeColor = '#000080'
        formatted_message = "{0}; {1}".format(reaction,message)
      else:
        formatted_message = message
      json_data = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageText",
        "themeColor": themeColor,
        "TextFormat":"markdown",
        "title": title,
        "text": formatted_message}
      r = requests.post(url=webhook_url,json=json_data)
      if r.status_code != 200:
        raise ValueError(
          "Failed to post message to team, error code: {0},{1}".\
            format(r.status_code,r.json()))
      time.sleep(2)
    except Exception as e:
      raise ValueError('Failed to send message to team, error: {0}'.format(e))
      