import os, requests, json
from urllib.parse import urljoin
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.fileutils import check_file_path

def get_request(url, headers=None, verify=False):
  try:
    res = \
      requests.get(
        url=url,
        headers=headers,
        verify=verify)
    if res.status_code != 200:
      raise ValueError(
              "Failed get request, got status: {0}".\
                format(res.status_code))
    res = res.json()
    return res
  except Exception as e:
    raise ValueError(e)

def post_request(url,  data, headers=None, verify=False, file_attachment=None):
  try:
    files = None
    if file_attachment is not None:
      files = {
        'file': (
          os.path.join(file_attachment),
          open(file_attachment, 'rb'), 'application/json')}
    res = \
      requests.post(
        url=url,
        data=data,
        headers=headers,
        verify=verify,
        files=files)
    if res.status_code != 200:
      raise ValueError(
              "Failed post request, got status: {0}".\
                format(res.status_code))
    res = res.json()
    return res
  except Exception as e:
    raise


def get_login_token(portal_config_file, verify=False, url_suffix='/api/v1/security/login'):
  try:
    portal_config = read_json_data(portal_config_file)
    if isinstance(portal_config, list):
      portal_config = portal_config[0]
    base_url = portal_config.get('base_url')
    login_data = portal_config.get('login_data')
    if login_data is None:
      raise KeyError("Missing logging info")
    if isinstance(login_data, dict):
      login_data = json.dumps(login_data)
    if base_url is None:
      raise KeyError("Missing base url")
    url = urljoin(base_url, url_suffix)
    json_res = \
      post_request(
        url=url,
        data=login_data,
        headers={"Content-Type": "application/json"},
        verify=verify)
    token = json_res.get('access_token')
    return token
  except Exception as e:
    raise ValueError("Failed to get token from portal, error: {0}".format(e))


def get_data_from_portal(portal_config_file, url_suffix, verify=False):
  try:
    check_file_path(portal_config_file)
    portal_config = read_json_data(portal_config_file)
    if isinstance(portal_config, list):
      portal_config = portal_config[0]
    base_url = portal_config.get('base_url')
    if base_url is None:
      raise KeyError("Missing base url")
    url = urljoin(base_url, url_suffix)
    token = \
      get_login_token(
        portal_config_file=portal_config_file,
        verify=verify)
    res = \
      get_request(
        url=url,
        headers={"accept": "application/json", "Authorization": "Bearer {0}".format(token)},
        verify=verify)
    return res
  except Exception as e:
    raise ValueError(e)


def upload_files_to_portal(portal_config_file, file_path, url_suffix, verify=False):
  try:
    check_file_path(file_path)
    portal_config = read_json_data(portal_config_file)
    if isinstance(portal_config, list):
      portal_config = portal_config[0]
    base_url = portal_config.get('base_url')
    if base_url is None:
      raise KeyError("Missing base url")
    url = urljoin(base_url, url_suffix)
    token = \
      get_login_token(
        portal_config_file=portal_config_file,
        verify=verify)
    res = \
      post_request(
        url=url,
        data=None,
        headers={"accept": "application/json", "Authorization": "Bearer {0}".format(token)},
        file_attachment=file_path,
        verify=verify)
    return res
  except Exception as e:
    raise ValueError(
            "Failed to upload file {0} to portal, error: {1}".\
              format(os.path.basename(file_path), e))