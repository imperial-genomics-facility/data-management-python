import os,warnings
import string
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Base,Experiment,Project,Sample,Sample_attribute
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.task_tracking.igf_slack import IGF_slack

class Experiment_metadata_updator:
  '''
  A class for updating metadata for experiment table in database
  '''
  def __init__(self,dbconfig_file,log_slack=True,slack_config=None):
    '''
    :param dbconfig_file: A database configuration file path
    :param log_slack: A boolean flag for toggling Slack messages, default True
    :param slack_config: A file containing Slack tokens, default None
    '''
    try:
      dbparams = read_dbconf_json(dbconfig_file)
      self.base_adaptor=BaseAdaptor(**dbparams)
      self.log_slack=log_slack
      if log_slack and slack_config is None:
        raise ValueError('Missing slack config file')
      elif log_slack and slack_config:
        self.igf_slack = IGF_slack(slack_config)                                # add slack object
    except:
      raise

  @staticmethod
  def _text_sum(a=None):
    if isinstance(a,list):
      return ';'.join(a)
    else:
      return a


  def update_metadta_from_sample_attribute(self,experiment_igf_id=None,
                                           sample_attribute_names=('library_source',
                                                                   'library_strategy',
                                                                   'experiment_type')):
    '''
    A method for fetching experiment metadata from sample_attribute tables
    :param experiment_igf_id: An experiment igf id for updating only a selected experiment, default None for all experiments
    :param sample_attribute_names: A list of sample attribute names to look for experiment metadata,
                                   default: library_source, library_strategy, experiment_type
    '''
    try:
      sample_attribute_names = list(sample_attribute_names)
      db_connected=False
      base=self.base_adaptor
      base.start_session()
      db_connected=True
      query=base.session.\
            query(Experiment.experiment_igf_id).\
            distinct(Experiment.experiment_id).\
            join(Sample).\
            join(Sample_attribute).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Sample.sample_id==Sample_attribute.sample_id).\
            filter(Experiment.library_source=='UNKNOWN').\
            filter(Experiment.library_strategy=='UNKNOWN').\
            filter(Experiment.experiment_type=='UNKNOWN').\
            filter(Sample_attribute.attribute_value.notin_('UNKNOWN')).\
            filter(Sample_attribute.attribute_name.in_(sample_attribute_names)) # base query for db lookup
      if experiment_igf_id is not None:
        query=query.filter(Experiment.experiment_igf_id==experiment_igf_id)     # look for specific experiment_igf_id

      exp_update_count=0
      exps=base.fetch_records(query, output_mode='object')                      # fetch exp records as generator expression
      for row in exps:
        experiment_id=row[0]
        ea=ExperimentAdaptor(**{'session':base.session})
        attributes=ea.fetch_sample_attribute_records_for_experiment_igf_id(experiment_igf_id=experiment_id, 
                                                                output_mode='object',
                                                                attribute_list=sample_attribute_names)
        exp_update_data=dict()
        for attribute_row in attributes:
          exp_update_data.update({attribute_row.attribute_name:attribute_row.attribute_value})

        if len(exp_update_data.keys())>0:
          exp_update_count+=1
          ea.update_experiment_records_by_igf_id(experiment_igf_id=experiment_id,
                                                 update_data=exp_update_data,
                                                 autosave=False)                # update experiment entry if attribute records are found

      base.commit_session()
      base.close_session()
      db_connected=False
      if self.log_slack:
        message='Update {0} experiments from sample attribute records'.\
                format(exp_update_count)
        self.igf_slack.post_message_to_channel(message=message,
                                               reaction='pass')
    except Exception as e:
      if db_connected:
        base.rollback_session()
        base.close_session()
        message='Error while updating experiment records: {0}'.format(e)
        warnings.warn(message)
        if self.log_slack:
          self.igf_slack.post_message_to_channel(message=message,
                                                 reaction='fail')
      raise

if __name__ == '__main__':
  from sqlalchemy import create_engine

  dbparams = read_dbconf_json('data/dbconfig.json')
  dbname=dbparams['dbname']
  if os.path.exists(dbname):
    os.remove(dbname)

  base=BaseAdaptor(**dbparams)
  Base.metadata.create_all(base.engine)
  base.start_session()
  project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                 'project_name':'test_22-8-2017_rna',
                 'description':'Its project 1',
                 'project_deadline':'Before August 2017',
                 'comments':'Some samples are treated with drug X',
                }]
  pa=ProjectAdaptor(**{'session':base.session})
  pa.store_project_and_attribute_data(data=project_data)
  sample_data=[{'sample_igf_id':'IGF00001',
                'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                'library_strategy':'RNA-SEQ',
                'experiment_type':'POLYA-RNA'},
               {'sample_igf_id':'IGF00003',
                'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                'experiment_type':'POLYA-RNA'},
               {'sample_igf_id':'IGF00002',
                'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',},
              ]
  sa=SampleAdaptor(**{'session':base.session})
  sa.store_sample_and_attribute_data(data=sample_data)
  experiment_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                    'sample_igf_id':'IGF00001',
                    'experiment_igf_id':'IGF00001_HISEQ4000',
                    'library_name':'IGF00001'},
                   {'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                    'sample_igf_id':'IGF00003',
                    'experiment_igf_id':'IGF00003_HISEQ4000',
                    'library_name':'IGF00001'},
                   {'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                    'sample_igf_id':'IGF00002',
                    'experiment_igf_id':'IGF00002_HISEQ4000',
                    'library_name':'IGF00002'},
                  ]
  ea=ExperimentAdaptor(**{'session':base.session})
  ea.store_project_and_attribute_data(data=experiment_data)
  base.close_session()

  emu=Experiment_metadata_updator(dbconfig_file='data/dbconfig.json',
                                  log_slack=False)
  emu.update_metadta_from_sample_attribute()
  if os.path.exists(dbname):
    os.remove(dbname)