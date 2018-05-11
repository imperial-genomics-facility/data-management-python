from sqlalchemy.sql import not_
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.task_tracking.igf_asana import IGF_asana
from igf_data.igfdb.igfTables import Project,Sample,Seqrun,Experiment,Run,Collection,File,Pipeline,Pipeline_seed


class Modify_pipeline_seed:
  '''
  A class for changing pipeline run status in the pipeline_seed table
  '''
  def __init__(self,igf_id_list,table_name,pipeline_name,dbconfig_file,
               log_slack=True,log_asana=True,slack_config=None,
               asana_project_id=None,asana_config=None):
    '''
    :param igf_id_list: A list of igf ids to uniquely identify the entity
    :param table_name: A database table name to look for the igf id
                       available options are 'project','sample','experiment','run',
                                             'file','seqrun','collection'
    :param pipeline_name: A pipeline name to change the status of the seed
    :param dbconfig_file: A file containing the database configuration
    :param log_slack: A boolean flag for toggling Slack messages, default True
    :param log_asana: Aboolean flag for toggling Asana message, default True
    :param slack_config: A file containing Slack tokens, default None
    :param asana_config: A file containing Asana tokens, default None
    :param asana_project_id: A numeric Asana project id, default is None
    '''
    try:
      self.igf_id_list=igf_id_list
      if table_name not in ('project','sample','experiment','run','file','seqrun','collection'):
        raise ValueError('Table {0} not supported for pipeline seed'.\
                         format(table_name))
      self.table_name=table_name
      self.pipeline_name=pipeline_name
      dbparams = read_dbconf_json(dbconfig_file)
      self.base_adaptor=BaseAdaptor(**dbparams)
      if log_slack and slack_config is None:
        raise ValueError('Missing slack config file')
      elif log_slack and slack_config:
        self.igf_slack = IGF_slack(slack_config)                                # add slack object

      if log_asana and \
         (asana_config is None or \
          asana_project_id is None):
        raise ValueError('Missing asana config file or asana project id')
      elif log_asana and asana_config and asana_project_id:
        self.igf_asana=IGF_asana(asana_config,asana_project_id)                 # add asana object
    except:
      raise

  def _fetch_pipeline_seed_entry(self,select_seed_status=None,restrict_seed_status=None):
    '''
    An internal method for fetching unique pipeline seed entry from database
    :param select_seed_status: A list of seed status to include from the query, default None
    :param restrict_seed_status: A list of seed status to exclude from the query, default None
    '''
    try:
      query=None
      if self.table_name =='seqrun':
        query=self.session.\
                   query(Pipeline_seed).\
                   join(Seqrun).\
                   join(Pipeline).\
                   filter(Pipeline_seed.seed_id==Seqrun.seqrun_id).\
                   filter(Pipeline_seed.seed_table==self.table_name).\
                   filter(Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
                   filter(Pipeline.pipeline_name==self.pipeline_name)           # get base query for seqrun table
      else:
        raise ValueError('Table {0} not supported for pipeline status reset'.\
                         format(self.table))

      if select_seed_status is not None and \
           isinstance(select_seed_status,list) and \
           len(select_seed_status) > 0:
          query=query.filter(Pipeline_seed.status.in_(select_seed_status))      # add generic select filter

      if restrict_seed_status is not None and \
           isinstance(restrict_seed_status,list) and \
           len(restrict_seed_status)>0:
          query.filter(not_(Pipeline_seed.status.in_(restrict_seed_status)))    # add generic restrict filter

      return query.one_or_none()                                                # fetch unique value for pipeline seed
    except:
      raise

  def reset_pipeline_seed_for_rerun(self,restricted_status_list=['SEEDED','RUNNING']):
    '''
    A method for setting the pipeline for re-run if the first run has failed or aborted
    This method will set the pipeline_seed.status as 'SEEDED' only if its not already
    'SEEDED' or 'RUNNING'
    :param restricted_status_list: A list of pipeline status to exclude from the search,
                                   default ['SEEDED','RUNNING']
    '''
    try:
      results=self._fetch_pipeline_seed_entry(restrict_seed_status=restricted_status_list) # get results for pipeline_seed table
      if results is not None:
        ddd
      else:
        raise ValueError('No entry found for seqrun {0}, pipeline {0}'.\
                         format(self.igf_id,self.pipeline_name))
    except:
      raise