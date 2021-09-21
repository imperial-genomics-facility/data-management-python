import logging
import pandas as pd
from airflow.models import Variable
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_data.utils.epmc_utils import search_epmc_for_keyword
from igf_data.utils.confluence_utils import update_confluence_page

CONFLUENCE_CONFIG_FILE = Variable.get('confluence_config')
SLACK_CONF = Variable.get('slack_conf', default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf', default_var=None)
WIKI_PUBLICATION_PAGE_ID = Variable.get('wiki_publication_page_id', default_var=None)
WIKI_PUBLICATION_PAGE_TITLE = Variable.get('wiki_publication_page_title', default_var=None)

def update_wiki_publication_page_func(**context):
  try:
    all_data = \
      search_epmc_for_keyword(
        search_term="\"Imperial BRC Genomics Facility\"")
    columns = [
      'title',
      'authorString',
      'journalTitle',
      'firstPublicationDate',
      'firstIndexDate',
      'doi']
    all_df = \
      pd.DataFrame(all_data)[columns]
    all_df['doi'] = \
      all_df['doi'].\
        map(lambda x: 'https://doi.org/{0}'.format(x))
    html_data = \
      all_df.\
        to_html(index=False)
    update_confluence_page(
      confluence_conf_file=CONFLUENCE_CONFIG_FILE,
      page_id=WIKI_PUBLICATION_PAGE_ID,
      page_title=WIKI_PUBLICATION_PAGE_TITLE,
      html_data=html_data)
    message = \
      'Updated wiki page {0}'.\
        format(WIKI_PUBLICATION_PAGE_TITLE)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='pass')
  except Exception as e:
    logging.error(e)
    message = \
      'Wiki update error: {0}'.\
        format(e)
    send_log_to_channels(
      slack_conf=SLACK_CONF,
      ms_teams_conf=MS_TEAMS_CONF,
      task_id=context['task'].task_id,
      dag_id=context['task'].dag_id,
      comment=message,
      reaction='fail')