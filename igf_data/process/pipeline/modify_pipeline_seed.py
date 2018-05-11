from sqlalchemy.sql import not_
from igf_data.igfdb.igfTables import Project,Sample,Seqrun,Experiment,Run,Collection,File,Pipeline,Pipeline_seed
class Modify_pipeline_seed:
  '''
  A class for changing pipeline run status in the pipeline_seed table
  '''
  def __init__(self,igf_id,table_name,pipeline_name,dbconfig_file):
    '''
    :param igf_id: A igf id to uniquely identify the entity
    :param table_name: A database table name to look for the igf id
    :param pipeline_name: A pipeline name to change the status of the seed
    :param session: A database session
    '''
    self.igf_id=igf_id
    self.table_name=table_name
    self.pipeline_name=pipeline_name
    self.session=session


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