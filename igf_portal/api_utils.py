import os, requests
from urllib.parse import urljoin
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.fileutils import check_file_path

def get_request(portal_config_file, url_suffix):
  pass

def post_request(url,  data, headers=None, verify=False, file_attachment=None):
  try:
      res = \
        requests.post(
          url,
          data=data,
          headers=headers,
          verify=verify,
          files=file_attachment)
      if res.status_code != 200:
        raise ValueError(
                "Failed post request, got status: {0}".\
                  format(res.status_code))
      return res
  except Exception as e:
    raise


def get_login_token(portal_config_file, verify=False, url_suffix='/api/v1/security/login'):
  try:
    portal_config = read_json_data(portal_config_file)
    base_url = portal_config.get('base_url')
    login_data = portal_config.get('login_data')
    if login_data is None:
      raise KeyError("Missing logging info")
    if base_url is None:
      raise KeyError("Missing base url")
    url = urljoin(base_url, url_suffix)
    res = \
      post_request(
        url=url,
        data=login_data,
        headers={"Content-Type": "application/json"},
        verify=verify)
    json_res = res.json()
    token = json_res.get('access_token')
    return token
  except Exception as e:
    raise ValueError("Failed to get token from portal, error: {0}".format(e))


def upload_files_to_portal(portal_config_file, file_path, url_suffix, verify=False):
  try:
    check_file_path(file_path)
    portal_config = read_json_data(portal_config_file)
    base_url = portal_config.get('base_url')
    if base_url is None:
      raise KeyError("Missing base url")
    login_data = portal_config.get('login_data')
    if login_data is None:
      raise KeyError("Missing logging info")
    url = urljoin(base_url, url_suffix)
    token = \
      get_login_token(
        portal_config_file=portal_config_file,
        verify=verify)
    res = \
      post_request(
        url=url,
        data=login_data,
        headers={'accept': 'application/json', "Authorization": "Bearer {0}".format(token)},
        file_attachment=file_path,
        verify=verify)
  except Exception as e:
    raise ValueError(
            "Failed to upload file {0} to portal, error: {1}".\
              format(os.path.basename(file_path), e))