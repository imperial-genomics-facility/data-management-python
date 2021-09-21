from urllib.parse import quote
import requests, json, time

def search_epmc_for_keyword(search_term, base_url='https://www.ebi.ac.uk/europepmc/webservices/rest/search'):
  try:
    if search_term is None or \
       search_term =="":
      raise ValueError('No search term found')
    encoded_search_term = \
      quote(search_term)
    formatted_url = \
      '{0}?query={1}&format=json&sort_date:y%20BDESC'.\
          format(base_url, encoded_search_term)
    all_data = list()
    cursor = ''
    while True:
      data, cursor = \
        get_pmc_data(
          url=formatted_url,
          cursor=cursor)
      if len(data) > 0 or cursor !='':
        all_data.extend(data)
        time.sleep(2)
      if cursor == '':
        break
    return all_data
  except Exception as e:
    raise ValueError('Failed to search ePMC, error: {0}'.format(e))


def get_pmc_data(url, cursor=''):
  '''
  A method for fetching pmc data

  :param orcid_id: An orcid id
  :param cursor: A cursor string, default empty string
  '''
  try:
    data = list()
    url_str = \
      '{0}&cursorMark={1}'.\
        format(url, cursor)
    response = requests.get(url_str)
    if response.ok:
      json_data = \
        json.loads(response.content.decode('utf-8'))
      data = json_data['resultList']['result']
      if 'nextCursorMark' in json_data and \
         cursor != json_data['nextCursorMark']:
        cursor = json_data['nextCursorMark']
      else:
        cursor = ''
    return data, cursor
  except Exception as e:
    raise ValueError(e)