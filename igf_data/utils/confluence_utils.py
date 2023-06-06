import requests
from typing import Any
from atlassian import Confluence
from igf_data.utils.fileutils import read_json_data

def update_confluence_page(
      confluence_conf_file: str,
      page_id: int,
      page_title: str,
      html_data: str,
      url: str = 'https://wiki.imperial.ac.uk') \
        -> None:
  """
  """
  try:
    # confluence_conf = \
    #   read_json_data(confluence_conf_file)[0]
    # if confluence_conf is not None and \
    #    isinstance(confluence_conf, list):
    #   confluence_conf = \
    #     confluence_conf[0]
    # s = requests.Session()
    # api_key = \
    #   confluence_conf.get('api_key')
    # if api_key is None:
    #   raise ValueError('No API key found')
    # s.headers['Authorization'] = \
    #   'Bearer {0}'.format(api_key)
    # confluence = \
    #   Confluence(
    #     url=url,
    #     session=s)
    confluence = \
      _pre_process_confluence_api_request(
        confluence_conf_file=confluence_conf_file,
        url=url)
    _ = \
      confluence.\
        update_page(
          page_id=page_id,
          title=page_title,
          body=html_data)
  except Exception as e:
    raise ValueError(
      f'Failed to upload confluence page {page_id}, error: {e}')


def _pre_process_confluence_api_request(
      confluence_conf_file: str,
      url: str)\
        -> Confluence:
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
    return confluence
  except Exception as e:
    raise ValueError(
      f'Failed to pre-process confluence api request, error: {e}')


def read_confluence_page(
      confluence_conf_file: str,
      page_id: int,
      url: str = 'https://wiki.imperial.ac.uk') \
        -> str:
  """
  """
  try:
    confluence = \
      _pre_process_confluence_api_request(
        confluence_conf_file=confluence_conf_file,
        url=url)
    data = \
      confluence.\
        get_page_by_id(
          page_id,
          expand='body.storage')
    page_body = \
      data.\
        get('body').\
        get('storage').\
        get('value')
    return page_body
  except Exception as e:
    raise ValueError(
      f'Failed to upload confluence page {page_id}, error: {e}')