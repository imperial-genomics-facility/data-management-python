import pandas as pd
from datetime import datetime,date,timedelta
from dateutil.parser import parse
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.seqrunutils import get_seqrun_date_from_igf_id
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from igf_data.igfdb.igfTables import Base, Project,Sample,Experiment,Run,Seqrun,Pipeline,Pipeline_seed

class Project_status:
  '''
  A class for project status fetch and gviz json file generation for Google chart grantt plot
  
  :param igf_session_class: Database session class
  :param project_igf_id: Project igf id for database lookup
  :param seqrun_work_day: Duration for seqrun jobs in days, default 2
  :param analysis_work_day: Duration for analysis jobs in days, default 1
  :param sequencing_resource_name: Resource name for sequencing data, default Sequencing
  :param demultiplexing_resource_name: Resource name for demultiplexing data,default Demultiplexing
  :param analysis_resource_name: Resource name for analysis data, default Primary Analysis
  :param task_id_label: Label for task id field, default task_id
  :param task_name_label: Label for task name field, default task_name
  :param resource_label: Label for resource field, default resource
  :param start_date_label: Label for start date field, default start_date
  :param end_date_label: Label for end date field, default end_date
  :param duration_label: Label for duration field, default duration
  :param percent_complete_label: Label for percent complete field, default percent_complete
  :param dependencies_label: Label for dependencies field, default dependencies
  '''
  def __init__(self,igf_session_class,project_igf_id,seqrun_work_day=2,
               analysis_work_day=1,sequencing_resource_name='Sequencing',
               demultiplexing_resource_name='Demultiplexing',
               analysis_resource_name='Primary Analysis',
               task_id_label='task_id',task_name_label='task_name',
               resource_label='resource',dependencies_label='dependencies',
               start_date_label='start_date',end_date_label='end_date',
               duration_label='duration',percent_complete_label='percent_complete'):
    self.project_igf_id=project_igf_id
    self.base_adaptor=BaseAdaptor(**{'session_class':igf_session_class})
    self.seqrun_work_day=seqrun_work_day
    self.analysis_work_day=analysis_work_day
    self.sequencing_resource_name=sequencing_resource_name
    self.demultiplexing_resource_name=demultiplexing_resource_name
    self.analysis_resource_name=analysis_resource_name
    self.task_id_label=task_id_label
    self.task_name_label=task_name_label
    self.resource_label=resource_label
    self.start_date_label=start_date_label
    self.end_date_label=end_date_label
    self.duration_label=duration_label
    self.percent_complete_label=percent_complete_label
    self.dependencies_label=dependencies_label


  def generate_gviz_json_file(self,output_file,demultiplexing_pipeline,
                              analysis_pipeline,active_seqrun_igf_id=None):
    '''
    A wrapper method for writing a gviz json file with project status information
    
    :param output_file: A filepath for writing project status
    :param analysis_pipeline: Name of the analysis pipeline
    :param demultiplexing_pipeline: Name of the demultiplexing pipeline
    :param analysis_pipeline: name of the analysis pipeline
    :param active_seqrun_igf_id: Igf id go the active seqrun, default None
    :returns: None
    '''
    try:
      data=list()
      description=self.get_status_description()                                 # get gviz description
      column_order=self.get_status_column_order()                               # get gviz column order
      seqrun_data=self.get_seqrun_info(\
                         active_seqrun_igf_id=active_seqrun_igf_id,
                         demultiplexing_pipeline=demultiplexing_pipeline)       # get seqrun data
      if len(seqrun_data)>0:
        data.extend(seqrun_data)                                                # add seqrun data

      analysis_data=self.get_analysis_info(\
                           analysis_pipeline=analysis_pipeline)                 # get analysis status
      if len(analysis_data)>0:
        data.extend(analysis_data)                                              # add analysis status

      if len(data)>0:
        convert_to_gviz_json_for_display(\
          data=data,
          description=description,
          columns_order=column_order,
          output_file=output_file)                                              # create gviz json file
      else:
        with open(output_file,'w') as fp:
          fp.write('')                                                          # create an empty file

    except:
      raise


  def get_status_description(self):
    '''
    A method for getting description for status json data
    
    :returns: A dictionary containing status info
    '''
    try:
      description={\
        'task_id':('string', 'Task ID'),
        'task_name':('string', 'Task Name'),
        'resource':('string', 'Resource'),
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
        'resource',
        'start_date',
        'end_date',
        'duration',
        'percent_complete',
        'dependencies'
      ]
      return columns_order
    except:
      raise


  def _add_seqrun_info(self,flowcell_id,seqrun_igf_id):
    '''
    An internal method for adding sequencing run info to the status page
    
    :param flowcell_id:
    :param seqrun_igf_id:
    :returns: A dictionary with seqrun data for the grantt plot
    '''
    try:
      start_date=parse(get_seqrun_date_from_igf_id(seqrun_igf_id))              # fetch seqrun date
      end_date=start_date+timedelta(days=self.seqrun_work_day)                  # calculate seqrun finish date
      duration=int((end_date-start_date).total_seconds()*1000)                  # calculate seqrun duration
      percent_complete=100                                                      # seqrun is already done
      new_data=dict()
      new_data.update(\
      {self.task_id_label:'Run {0}'.format(flowcell_id),
       self.task_name_label:'Run {0}'.format(flowcell_id),
       self.resource_label:self.sequencing_resource_name,
       self.start_date_label:start_date,
       self.end_date_label:end_date,
       self.duration_label:duration,
       self.percent_complete_label:percent_complete,
       self.dependencies_label:None,
      })
      return new_data
    except:
      raise


  def _reformat_seqrun_data(self,data,active_seqrun_igf_id=None):
    '''
    An internal method for reformatting seqrun data series
    
    :param data: A pandas data series containing seqrun entries for a project
    :param active_seqrun_igf_id: Igf id of the active seqrun, default None
    :returns: A list of dictionaries containing the required entries for sequencing runs
    '''
    try:
      if 'date_created' not in data:
        raise ValueError('Missing seqrun creation date')

      start_date=data['date_created']
      if 'status' in data and \
          data['status']=='FINISHED':
        end_date=data['date_stamp']
        percent_complete=100
      else:
        end_date=data['date_created']+timedelta(days=self.seqrun_work_day)
        percent_complete=0

      if 'status' in data and \
         data['status']!='FINISHED':
        if active_seqrun_igf_id is not None and \
           active_seqrun_igf_id==data['seqrun_igf_id']:
          percent_complete=100
          end_date=data['date_stamp']
        else:
          percent_complete=0

      duration=int((end_date-start_date).total_seconds()*1000)
      if duration < 86400000:
        end_date=None                                                           # minimum duration is 1 day for the plot

      new_data=dict()
      new_data.update(\
      {self.task_id_label:data['flowcell_id'],
       self.task_name_label:'Flowcell {0}'.format(data['flowcell_id']),
       self.resource_label:self.demultiplexing_resource_name,
       self.start_date_label:data['date_created'],
       self.end_date_label:end_date,
       self.duration_label:duration,
       self.percent_complete_label:percent_complete,
       self.dependencies_label:'Run {0}'.format(data['flowcell_id']),
      })

      seqrun_data=self._add_seqrun_info(\
                    flowcell_id=data['flowcell_id'],
                    seqrun_igf_id=data['seqrun_igf_id'])                        # fetch seqrun information
      new_data_list=[seqrun_data,new_data]
      return  new_data_list
    except:
      raise


  def get_analysis_info(self,analysis_pipeline):
    '''
    A method for fetching all active experiments and their run status for a project
    
    :param analysis_pipeline: Name of the analysis pipeline
    :return: A list of dictionary containing the analysis information
    '''
    try:
      base=self.base_adaptor
      base.start_session()
      query=base.session.\
            query(Experiment.experiment_igf_id,
                  Pipeline_seed.status,
                  Pipeline_seed.date_stamp,
                  Seqrun.flowcell_id).\
            join(Run).\
            join(Sample).\
            join(Project).\
            join(Pipeline_seed,Experiment.experiment_id==Pipeline_seed.seed_id).\
            join(Pipeline).\
            join(Seqrun).\
            filter(Run.experiment_id==Experiment.experiment_id).\
            filter(Seqrun.seqrun_id==Run.seqrun_id).\
            filter(Experiment.sample_id==Sample.sample_id).\
            filter(Sample.project_id==Project.project_id).\
            filter(Pipeline_seed.seed_table=='experiment').\
            filter(Sample.status=='ACTIVE').\
            filter(Experiment.status=='ACTIVE').\
            filter(Run.status=='ACTIVE').\
            filter(Seqrun.reject_run=='N').\
            filter(Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
            filter(Pipeline.pipeline_name==analysis_pipeline).\
            filter(Project.project_igf_id==self.project_igf_id)
      results=base.fetch_records(query=query,
                                 output_mode='dataframe')
      base.close_session()
      new_data=list()
      if len(results.index)>0:
        flowcell_ids=list(set(results['flowcell_id'].values))
        results=results.drop(['flowcell_id'],axis=1).drop_duplicates()
        status_data=[ {grp:len(g_data.index),'total':len(results.index)} 
                        for grp, g_data in results.groupby('status')]
        pct_complete=0
        incomplete_exp=0
        for status in status_data:
          if 'FINISHED' in status:
            pct_complete=int(status['FINISHED']/status['total']*100)            # get percent complete
            incomplete_exp=int(status['total']-status['FINISHED'])

        first_update=results['date_stamp'].min()
        first_update_status=results[results['date_stamp']==first_update]['status'].values[0]
        if first_update_status=='SEEDED':
          start_date=first_update
        else:
          start_date=first_update-timedelta(days=self.analysis_work_day)        # get analysis start date

        last_update=results['date_stamp'].max()
        if incomplete_exp>0:
          end_date=last_update+incomplete_exp*timedelta(days=self.analysis_work_day) # expected end date
        else:
          end_date=last_update                                                  # end date if all done

        duration=int((end_date-start_date).total_seconds()*1000)                # calculate analysis duration
        new_data.append(\
          {self.task_id_label:'Primary Analysis',
           self.task_name_label:'Primary Analysis',
           self.resource_label:self.analysis_resource_name,
           self.start_date_label:start_date,
           self.end_date_label:end_date,
           self.duration_label:duration,
           self.percent_complete_label:pct_complete,
           self.dependencies_label:','.join(flowcell_ids),
          })
      return new_data
    except:
      raise


  def get_seqrun_info(self,active_seqrun_igf_id=None,
                      demultiplexing_pipeline=None):
    '''
    A method for fetching all active sequencing runs for a project
    
    :param active_seqrun_igf_id: Seqrun igf id for the current run, default None
    :param demultiplexing_pipeline: Name of the demultiplexing pipeline, default None
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
            filter(Seqrun.reject_run=='N').\
            filter(Experiment.experiment_id==Run.experiment_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Project.project_id==Sample.project_id).\
            filter(Project.project_igf_id==self.project_igf_id)
      if demultiplexing_pipeline is not None:
        query=base.session.\
              query(Seqrun.seqrun_igf_id,
                  Seqrun.flowcell_id,
                  Seqrun.date_created,
                  Pipeline_seed.status,
                  Pipeline_seed.date_stamp).\
              join(Run).\
              join(Experiment).\
              join(Sample).\
              join(Project).\
              join(Pipeline_seed,Seqrun.seqrun_id==Pipeline_seed.seed_id).\
              join(Pipeline).\
              filter(Seqrun.seqrun_id==Run.seqrun_id).\
              filter(Experiment.experiment_id==Run.experiment_id).\
              filter(Sample.sample_id==Experiment.sample_id).\
              filter(Project.project_id==Sample.project_id).\
              filter(Project.project_igf_id==self.project_igf_id).\
              filter(Pipeline_seed.seed_table=='seqrun').\
              filter(Pipeline.pipeline_name==demultiplexing_pipeline)

      results=base.fetch_records(query=query,
                                 output_mode='dataframe')
      base.close_session()
      results.drop_duplicates(inplace=True)
      new_data=list()
      if len(results.index)>0:
        new_data.extend(\
          results.\
            apply(lambda data: self._reformat_seqrun_data(\
                               data,
                               active_seqrun_igf_id=active_seqrun_igf_id),
                  axis=1))
        new_data=[entry for data in new_data 
                        for entry in data]
      return new_data
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
  from igf_data.igfdb.pipelineadaptor import PipelineAdaptor

  
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
  seqrun_data=[{'seqrun_igf_id':'180810_K00345_0063_AHWL7CBBXX', 
                'flowcell_id':'000000000-D0YLK', 
                'platform_igf_id':'M001',
                'flowcell':'MISEQ'},
                {'seqrun_igf_id':'180610_K00345_0063_AHWL7CBBXX', 
                'flowcell_id':'000000000-D0YLJ', 
                'platform_igf_id':'M001',
                'flowcell':'MISEQ'},
                {'seqrun_igf_id':'180410_K00345_0063_AHWL7CBBXX', 
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
             'seqrun_igf_id':'180810_K00345_0063_AHWL7CBBXX',
             'lane_number':'1'},
             {'run_igf_id':'RunB',
             'experiment_igf_id':'ExperimentA',
             'seqrun_igf_id':'180610_K00345_0063_AHWL7CBBXX',
             'lane_number':'1'},
             {'run_igf_id':'RunC',
             'experiment_igf_id':'ExperimentA',
             'seqrun_igf_id':'180410_K00345_0063_AHWL7CBBXX',
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
  pipeline_data=[{ "pipeline_name" : "DemultiplexIlluminaFastq",
                   "pipeline_db" : "sqlite:////bcl2fastq.db", 
                 }]

  pipeline_seed_data=[{'pipeline_name':'DemultiplexIlluminaFastq',
                       'seed_id':1, 'seed_table':'seqrun'},
                       {'pipeline_name':'DemultiplexIlluminaFastq',
                       'seed_id':2, 'seed_table':'seqrun'},
                       {'pipeline_name':'DemultiplexIlluminaFastq',
                       'seed_id':3, 'seed_table':'seqrun'},
                     ]
  pla=PipelineAdaptor(**{'session':base.session})
  pla.store_pipeline_data(data=pipeline_data)
  pla.create_pipeline_seed(data=pipeline_seed_data)
  pipeline_data=[{ "pipeline_name" : "PrimaryAnalysis",
                   "pipeline_db" : "sqlite:////analysis.db", 
                 }]

  pipeline_seed_data=[{'pipeline_name':'PrimaryAnalysis',
                       'seed_id':1, 'seed_table':'experiment'},
                      {'pipeline_name':'PrimaryAnalysis',
                       'seed_id':2, 'seed_table':'experiment'},
                      {'pipeline_name':'PrimaryAnalysis',
                       'seed_id':3, 'seed_table':'experiment'}
                     ]
  pla.store_pipeline_data(data=pipeline_data)
  pla.create_pipeline_seed(data=pipeline_seed_data)
  base.commit_session()
  base.close_session()
  
  ps=Project_status(igf_session_class=base.get_session_class(),
                    project_igf_id='ProjectA')
  #print(ps.get_seqrun_info(demultiplexing_pipeline='DemultiplexIlluminaFastq'))
  #print(ps.get_seqrun_info(active_seqrun_igf_id='SeqrunA'))
  #print(ps.get_seqrun_info(demultiplexing_pipeline='DemultiplexIlluminaFastq',
  #                         active_seqrun_igf_id='180410_K00345_0063_AHWL7CBBXX'))
  #print(ps.get_status_description())
  #print(ps.get_status_column_order())
  #print(ps.get_analysis_info(analysis_pipeline='PrimaryAnalysis'))
  #ps.generate_gviz_json_file(output_file='a',
  #                           demultiplexing_pipeline='DemultiplexIlluminaFastq',
  #                           analysis_pipeline='PrimaryAnalysis',
  #                           active_seqrun_igf_id='180410_K00345_0063_AHWL7CBBXX')
  Base.metadata.drop_all(engine)
  os.remove(dbname)
  