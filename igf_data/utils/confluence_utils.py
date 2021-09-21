import requests
from atlassian import Confluence
from igf_data.utils.fileutils import read_json_data

def update_confluence_page(
      confluence_conf_file, page_id, page_title,
      html_data, url='https://wiki.imperial.ac.uk'):
  """
  """
  try:
    confluence_conf = \
      read_json_data(confluence_conf_file)[0]
    if confluence_conf is not None and \
       isinstance(confluence_conf, list):
      confluence_conf = \
        confluence_conf[0]
    s = requests.Session()
    api_key = \
      confluence_conf.get('api_key')
    if api_key is None:
      raise ValueError('No API key found')
    s.headers['Authorization'] = \
      'Bearer {0}'.format(api_key)
    confluence = \
      Confluence(
        url=url,
        session=s)
    _ = \
      confluence.\
        update_page(
          page_id=page_id,
          title=page_title,
          body=html_data)
  except Exception as e:
    raise ValueError(
            'Failed to upload confluence page, error: {0}'.format(e))