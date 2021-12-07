import os, json
#from slackclient import SlackClient
# FIX FOR slackclient-2.9.3
from slack import WebClient

class IGF_slack:
  '''
  A class for looging messages to the slack channel
  Configuration for the slack channel can be provided using a json file
  e.g. { "slack_token" : "XXXX", "slack_channel" : "ABCD", "slack_bot_id" : "R2D2" }
  '''
  def __init__(self, slack_config, **slack_label):
    slack_label.setdefault('slack_token_label','slack_token')
    slack_label.setdefault('slack_channel_label','slack_channel')
    slack_label.setdefault('slack_bot_id_label','slack_bot_id')
    self.slack_token_label = slack_label['slack_token_label']
    self.slack_channel_label = slack_label['slack_channel_label']
    self.slack_bot_id_label = slack_label['slack_bot_id_label']
    self.slack_token = None
    self.slack_channel_id = None
    self.slack_bot_id = None
    self.slack_config = slack_config
    self._read_and_set_slack_config()                                           # read config file and set parameters
    #self.slackobject = SlackClient(self.slack_token)                            # create slackclient instance
    # FIX FOR slackclient-2.9.3
    self.slackobject = WebClient(self.slack_token)
    self.slack_token = None                                                     # reset slack token 

  def post_message_to_channel(
        self, message, reaction='sleep',
        mention_all_channel=False):
    '''
    A method for posting message to the slack channel
    :param message: a text message
    :param reaction: pass / fail / sleep
    :param mention_all_channel: Mention all channel, default False
    '''
    try:
      if reaction.lower()=='pass':
        reaction = ':large_green_circle:'
      elif reaction.lower()=='fail':
        reaction = ':red_circle:'
      elif reaction.lower()=='sleep':
        reaction = ':large_blue_circle:'
      if mention_all_channel:
        message = \
          '{0} Hey <!channel>, {1}'.format(reaction, message)
      else:
        message = \
          '{0} {1}'.format(reaction, message)
      self.slackobject.api_call(
        "chat.postMessage",
        channel=self.slack_channel_id,
        text=message)
      # FIX FOR slackclient-2.9.3
      #res = \
      #  self.slackobject.chat_postMessage(
      #    channel=self.slack_channel_id,
      #    text=message)
    except:
      raise


  def post_file_to_channel(self, filepath, message=None):
    '''
    A method for uploading a file to slack
    required params:
    filepath: A filepath for upload
    message: An optional message
    '''
    try:
      if not os.path.exists(filepath):
        raise IOError('file {0} not found'.format(filepath))
      if os.stat(filepath).st_size > 5000000:
        message = 'skipped uploading file {0}, size {1}'.\
        format(os.path.basename(filepath), os.stat(filepath).st_size)            # skip uploading files more than 5Mb in size
      else:
        self.slackobject.api_call(
          "files.upload",
          channels=self.slack_channel_id,
          initial_comment=message,
          file=open(os.path.join(filepath),'rb'))                               # share files in slack channel
        # FIX FOR slackclient-2.9.3
        #res = \
        #  self.slackobject.files_upload(
        #    channels=self.slack_channel_id,
        #    file=filepath)
    except:
      raise


  def post_message_to_channel_thread(self, message, thread_id, reaction=''):
    '''
    A method for posting reply message to the slack channel thread
    required params:
    message: a text message
    thread_id: a thread ts id
    optional:
    reaction: pass / fail / sleep
    '''
    try:
      if reaction=='pass':
        reaction = ':large_green_circle:'
      elif reaction=='fail':
        reaction = ':red_circle:'
      elif reaction=='sleep':
        reaction = ':zzz:'
      message = '{0} {1}'.format(reaction, message)
      self.slackobject.api_call(
        "chat.postMessage",
        channel=self.slack_channel_id,
        text=message,
        thread_ts=thread_id,
        is_im=True)
      # FIX FOR slackclient-2.9.3
      #res = \
      #  self.slackobject.\
      #    chat_postMessage(
      #      channel=self.slack_channel_id,
      #      text=message,
      #      thread_ts=thread_id)
    except:
      raise


  def _read_and_set_slack_config(self):
    '''
    An internal method for reading slack json file
    '''
    try:
      slack_params = dict()
      with open(self.slack_config,'r') as json_data:
        slack_params = json.load(json_data)
        if self.slack_token_label in slack_params:
          self.slack_token = slack_params[self.slack_token_label]
        if self.slack_channel_label in slack_params:
          self.slack_channel_id = slack_params[self.slack_channel_label]
        if self.slack_bot_id_label in slack_params:
          self.slack_bot_id=slack_params[self.slack_bot_id_label]
    except Exception as e:
      raise ValueError(
        'Failed to get Slack config, error: {0}'.format(e))

