import pandas as pd
from datetime import datetime,date,timedelta
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.igfTables import Base, Project,Sample,Experiment,Run,Seqrun

class Project_status:
  '''
  A class for project status info calculation
  
  :param igf_session_class: Database session class
  :param project_igf_id: Project igf id for database lookup
  :param seqrun_work_day: Duration for seqrun jobs in days, default 2 
  '''
  def __init__(self,igf_session_class,project_igf_id,seqrun_work_day=2):
    self.project_igf_id=project_igf_id
    self.base_adaptor=BaseAdaptor(**{'session_class':igf_session_class})
    self.seqrun_work_day=seqrun_work_day

  def get_status_description(self):
    '''
    A method for getting description for status json data
    :returns: A dictionary containing status info
    '''
    try:
      description={\
        'task_id':('string', 'Task ID'),
        'task_name':('string', 'Task Name'),
        'start_date':('date', 'Start Date'),
        'end_date':('date', 'End Date'),
        'duration':('number', 'Percent Complete'),
        'percent_complete':('number', 'Duration'),
        'dependencies':('string', 'Dependencies'),
      }
      return description
    except:
      raise

  def get_status_column_order(self):
    '''
    A method for fetching column order for status json data
    :return: A list data containing the column order
    '''
    try:
      columns_order=[\
        'task_id',
        'task_name',
        'start_date',
        'end_date',
        'duration',
        'percent_complete',
        'dependencies'
      ]
      return columns_order
    except:
      raise

  @staticmethod
  def _reformat_seqrun_data(data,seqrun_work_day,task_id_label='task_id',
                            task_name_label='task_name',resource_label='resource',
                            resource_name='Demultiplexing',start_date_label='start_date',
                            end_date_label='end_date',duration_label='duration',
                            percent_complete_label='percent_complete',
                            dependencies_label='dependencies'):
    '''
    An internal static method for reformatting seqrun data series
    
    :param data: A pandas data series
    :returns: A pandas data series containing the required keys
    '''
    try:
      new_data=dict()
      new_data.update(\
      {task_id_label:data['flowcell_id'],
       task_name_label:'Flowcell {0}'.format(data['flowcell_id']),
       resource_label:resource_name,
       start_date_label:data['date_created'],
       end_date_label:data['date_created']+timedelta(days=seqrun_work_day),
       duration_label:int(timedelta(days=seqrun_work_day).total_seconds()*1000),
       percent_complete_label:100,
       dependencies_label:None,
      })

      new_data=pd.Series(new_data)
      return  new_data
    except:
      raise

  def get_seqrun_info(self):
    '''
    A method for fetching all active sequencing runs for a project
    :returns: A dictionary containing seqrun information
    '''
    try:
      base=self.base_adaptor
      base.start_session()
      query=base.session.\
            query(Seqrun.seqrun_igf_id,
                  Seqrun.flowcell_id,
                  Seqrun.date_created).\
            join(Run).\
            join(Experiment).\
            join(Sample).\
            join(Project).\
            filter(Seqrun.seqrun_id==Run.seqrun_id).\
            filter(Experiment.experiment_id==Run.experiment_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Project.project_id==Sample.project_id).\
            filter(Project.project_igf_id==self.project_igf_id)
      results=base.fetch_records(query=query,
                                 output_mode='dataframe')
      base.close_session()
      results=results.\
              apply(lambda data: self._reformat_seqrun_data(\
                                   data,
                                   seqrun_work_day=self.seqrun_work_day),
                    axis=1)
      results=results.to_dict(orient='records')
      return results
    except:
      raise


if __name__=='__main__':
  import os, unittest, sqlalchemy
  from sqlalchemy import create_engine
  from igf_data.utils.dbutils import read_dbconf_json
  from igf_data.igfdb.projectadaptor import ProjectAdaptor
  from igf_data.igfdb.sampleadaptor import SampleAdaptor
  from igf_data.igfdb.platformadaptor import PlatformAdaptor
  from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
  from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
  from igf_data.igfdb.runadaptor import RunAdaptor

  
  dbconfig = 'data/dbconfig.json'
  dbparam=read_dbconf_json(dbconfig)
  base = BaseAdaptor(**dbparam)
  engine = base.engine
  dbname=dbparam['dbname']
  Base.metadata.drop_all(engine)
  if os.path.exists(dbname):
      os.remove(dbname)
  
  Base.metadata.create_all(engine)
  
  platform_data=[{ "platform_igf_id" : "M001",
                   "model_name" : "MISEQ" ,
                   "vendor_name" : "ILLUMINA" ,
                   "software_name" : "RTA",
                   "software_version" : "RTA1.18.54"}]                          # platform data
  flowcell_rule_data=[{"platform_igf_id":"M001",
                       "flowcell_type":"MISEQ",
                       "index_1":"NO_CHANGE",
                       "index_2":"NO_CHANGE"}]                                  # flowcell data
  project_data=[{'project_igf_id':'ProjectA'}]                                  # project data
  sample_data=[{'sample_igf_id':'SampleA',
                'project_igf_id':'ProjectA'}]                                   # sample data
  seqrun_data=[{'seqrun_igf_id':'SeqrunA', 
                'flowcell_id':'000000000-D0YLK', 
                'platform_igf_id':'M001',
                'flowcell':'MISEQ'},
                {'seqrun_igf_id':'SeqrunB', 
                'flowcell_id':'000000000-D0YLJ', 
                'platform_igf_id':'M001',
                'flowcell':'MISEQ'},
                {'seqrun_igf_id':'SeqrunC', 
                'flowcell_id':'000000000-D0YLI', 
                'platform_igf_id':'M001',
                'flowcell':'MISEQ'}
              ]                                                                 # experiment data
  experiment_data=[{'experiment_igf_id':'ExperimentA',
                    'sample_igf_id':'SampleA',
                    'library_name':'SampleA',
                    'platform_name':'MISEQ',
                    'project_igf_id':'ProjectA'}]
  run_data=[{'run_igf_id':'RunA',
             'experiment_igf_id':'ExperimentA',
             'seqrun_igf_id':'SeqrunA',
             'lane_number':'1'},
             {'run_igf_id':'RunB',
             'experiment_igf_id':'ExperimentA',
             'seqrun_igf_id':'SeqrunB',
             'lane_number':'1'},
             {'run_igf_id':'RunC',
             'experiment_igf_id':'ExperimentA',
             'seqrun_igf_id':'SeqrunC',
             'lane_number':'1'}
           ]                                                                    # run data
  base.start_session()
  pl=PlatformAdaptor(**{'session':base.session})
  pl.store_platform_data(data=platform_data)                                    # loading platform data
  pl.store_flowcell_barcode_rule(data=flowcell_rule_data)                       # loading flowcell rules data
  pa=ProjectAdaptor(**{'session':base.session})
  pa.store_project_and_attribute_data(data=project_data)                        # load project data
  sa=SampleAdaptor(**{'session':base.session})
  sa.store_sample_and_attribute_data(data=sample_data)                          # store sample data
  sra=SeqrunAdaptor(**{'session':base.session})
  sra.store_seqrun_and_attribute_data(data=seqrun_data)                         # load seqrun data
  ea=ExperimentAdaptor(**{'session':base.session})
  ea.store_project_and_attribute_data(data=experiment_data)                     # load experiment data
  ra=RunAdaptor(**{'session':base.session})
  ra.store_run_and_attribute_data(data=run_data)                                # load run data
  base.commit_session()
  base.close_session()
  
  ps=Project_status(igf_session_class=base.get_session_class(),
                    project_igf_id='ProjectA')
  print(ps.get_seqrun_info())
  print(ps.get_status_description())
  print(ps.get_status_column_order())
  Base.metadata.drop_all(engine)
  os.remove(dbname)
  