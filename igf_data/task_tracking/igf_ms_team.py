import os,requests,json,time,base64
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.fileutils import check_file_path

class IGF_ms_team:
  def __init__(self,webhook_conf_file):
    try:
      webhook_conf =  read_json_data(webhook_conf_file)[0]
    except Exception as e:
      raise ValueError(
        "Failed to parse webhook config file {0}, error: {1}".\
          format(webhook_conf_file,e))
    self.webhook_conf = webhook_conf

  def post_message_with_mention(self, message, aad_id, name, email_id):
    try:
      webhook_url = self.webhook_conf.get('webhook_url')
      json_data = {
        "type": "message",
        "attachments": [
          {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
              "type": "AdaptiveCard",
              "body": [
                {
                  "type": "TextBlock",
                  "text": "Hi <at>{0}</at>. {1}".format(aad_id, message),
                  "wrap": True,
                  "width": "stretch"
                }
              ],
              "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
              "version": "1.0",
              "msteams": {
                "entities": [
                  {
                    "type": "mention",
                    "text": "<at>{0}</at>".format(aad_id),
                    "mentioned": {
                      "id": email_id,
                      "name": name
                    }
                  }
                ]
              }
            }
          }]
        }
      r = \
        requests.post(
          url=webhook_url,
          json=json_data)
      if r.status_code != 200:
        raise ValueError(
          "Failed to post message to team, error code: {0},{1}".\
            format(r.status_code,r.json()))
      time.sleep(2)
    except Exception as e:
      raise ValueError(
              'Failed to post message with mention, error: {0}'.format(e))


  def post_image_to_team(self,image_path,message='',reaction=''):
    '''
    A method for posting image file to MS team chanel

    :param image_path: An image file path
    :param message: An optional message, default ''
    :param reaction: A text for image reacion theme, default ''
    '''
    try:
      webhook_url = self.webhook_conf.get('webhook_url')
      check_file_path(image_path)
      encoded_image = base64.b64encode(open(image_path, "rb").read()).decode()
      formatted_image = "data:image/png;base64,{0}".format(encoded_image)
      if reaction !='' or reaction is not None:
        if reaction == 'pass':
          reaction =  'good'
        elif reaction == 'fail':
          reaction = 'attention'
        elif reaction == 'sleep':
          reaction = 'default'
      if reaction == '':
        reaction = 'default'
      #json_data = {
      #  "@context": "https://schema.org/extensions",
      #  "@type": "MessageText",
      #  "themeColor": themeColor,
      #  "TextFormat":"markdown",
      #  "text": formatted_message}
      json_data = {
        "type":"message",
        "attachments":[{
          "contentType":"application/vnd.microsoft.card.adaptive",
          "contentUrl":None,
          "content":{
            "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
            "type":"AdaptiveCard",
            "version":"1.2",
            "body":[{
              "type": "Image",
              "url": formatted_image
              },{
              "type":"TextBlock",
              "text":message,
              "color":reaction
            }]
          }
        }]
      }
      r = requests.post(url=webhook_url,json=json_data)
      if r.status_code != 200:
        raise ValueError(
          "Failed to post message to team, error code: {0},{1}".\
            format(r.status_code,r.json()))
      time.sleep(2)
    except Exception as e:
      raise ValueError('Failed to send image file to team, error: {0}, file: {1}'.\
              format(e,image_path))


  def post_message_to_team(self, message, reaction=''):
    try:
      webhook_url = self.webhook_conf.get('webhook_url')
      header_msg = ''
      if reaction !='' or reaction is not None:
        if reaction == 'pass':
          reaction = 'good'
          header_msg = '**Message**'
        elif reaction == 'fail': 
          reaction = 'attention'
          header_msg = '**Error**'
        elif reaction == 'sleep':
          reaction = 'default'
      if reaction == '':
        reaction = 'default'
      json_data = {
        "type":"message",
        "attachments":[{
          "contentType":"application/vnd.microsoft.card.adaptive",
          "content":{
            "$schema":"http://adaptivecards.io/schemas/adaptive-card.json",
            "type":"AdaptiveCard",
            "version":"1.4",
            "body":[{
              "type": "TextBlock",
              "text": header_msg,
              "wrap": True,
              "color": reaction,
              "style": "heading",
              "size": "bolder" },{
              "type": "TextBlock",
              "text": message,
              "wrap": True}]
          }
        }]
      }
      r = requests.post(url=webhook_url, json=json_data)
      if r.status_code != 200:
        raise ValueError(
          "Failed to post message to team, error code: {0},{1}".\
            format(r.status_code,r.json()))
      time.sleep(2)
    except Exception as e:
      raise ValueError('Failed to send message to team, error: {0}'.format(e))
