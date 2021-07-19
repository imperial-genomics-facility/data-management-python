import os
import pandas as pd
from copy import deepcopy
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import copy_local_file
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.igfTables import Project
from igf_data.igfdb.igfTables import Sample
from igf_data.igfdb.igfTables import Experiment
from igf_data.igfdb.igfTables import Collection
from igf_data.igfdb.igfTables import Collection_group
from igf_data.igfdb.igfTables import File
from igf_data.igfdb.igfTables import Collection_attribute
from igf_data.igfdb.igfTables import Pipeline
from igf_data.igfdb.igfTables import Pipeline_seed
from igf_data.igfdb.igfTables import Analysis
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display


class Project_analysis:
  '''
  A class for fetching all the analysis files linked to a project

  :param igf_session_class: A database session class
  :param collection_type_list: A list of collection type for database lookup
  :param remote_analysis_dir: A remote path prefix for analysis file look up, default analysis
  :param attribute_collection_file_type: A filetype list for fetching collection attribute records, default ('ANALYSIS_CRAM')

  '''
  def __init__(
        self, igf_session_class, collection_type_list, remote_analysis_dir='analysis', use_ephemeral_space=0,
        attribute_collection_file_type=('ANALYSIS_CRAM'), pipeline_name='PrimaryAnalysisCombined',
        pipeline_seed_table='experiment', pipeline_finished_status='FINISHED', sample_id_label='SAMPLE_ID'):
    try:
      self.igf_session_class = igf_session_class
      if not isinstance(collection_type_list, list):
        raise ValueError('Expecting a list of collection types for db look up')
      self.collection_type_list = collection_type_list
      self.remote_analysis_dir = remote_analysis_dir
      self.sample_id_label = sample_id_label
      self.attribute_collection_file_type = attribute_collection_file_type
      self.pipeline_name = pipeline_name
      self.pipeline_seed_table = pipeline_seed_table
      self.pipeline_finished_status = pipeline_finished_status
      self.use_ephemeral_space = use_ephemeral_space
    except:
      raise

  @staticmethod
  def _add_html_tag(
        data_series, project_igf_id=None, file_path_column='file_path', 
        sample_igf_id_column='sample_igf_id', remote_prefix='analysis',
        ftp_prefix='/www/html/report/project/'):
    '''
    An internal static method for reformatting filepath with html code

    :param data_series: A pandas series with 'file_path' and 'sample_igf_id'
    :param sample_igf_id_column: A column name for sample igf id, default sample_igf_id
    :param file_path_column: A column name for file path, default file_path
    :param remote_prefix: A remote path prefix, default analysis
    :param project_igf_id: Project id, default None
    :param ftp_prefix: FTP prefix path for project, default '/www/html/report/project/'
    :returns: A pandas series
    '''
    try:
      if file_path_column in data_series and \
         sample_igf_id_column in data_series:
        file_path = data_series.get(file_path_column)
        base_path = os.path.basename(file_path)
        sample_igf_id = data_series.get(sample_igf_id_column)
        if project_igf_id is None:
          remote_path = \
            os.path.join(
              remote_prefix,
              sample_igf_id,
              base_path)
        else:
          remote_path = \
            os.path.relpath(
              file_path,
              os.path.join(ftp_prefix, project_igf_id))
        file_path = \
          '<a href=\"{0}\">{1}</a>'.\
            format(
              remote_path,
              base_path)
        data_series[file_path_column] = file_path
      return data_series
    except:
      raise


  def _fetch_collection_attributes(self, project_igf_id):
    '''
    An internal method for fetching collection attribute records
    :param project_igf_id: A project igf id for database lookup
    :returns: A Pandas dataframe
    '''
    try:
      base = BaseAdaptor(**{'session_class':self.igf_session_class})
      base.start_session()
      if self.pipeline_seed_table=='experiment':
        subquery = \
          base.session.\
            query(Experiment.experiment_igf_id).\
            join(Sample, Sample.sample_id==Experiment.sample_id).\
            join(Project, Project.project_id==Sample.project_id).\
            join(Pipeline_seed, Pipeline_seed.seed_id==Experiment.experiment_id).\
            join(Pipeline, Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
            filter(Pipeline_seed.seed_table==self.pipeline_seed_table).\
            filter(Pipeline_seed.status==self.pipeline_finished_status).\
            filter(Pipeline.pipeline_name==self.pipeline_name).\
            filter(Project.project_igf_id==project_igf_id).\
            subquery()
      elif self.pipeline_seed_table=='sample':
        subquery = \
          base.session.\
            query(Sample.sample_id).\
            join(Project, Project.project_id==Sample.project_id).\
            join(Pipeline_seed, Pipeline_seed.seed_id==Sample.sample_id).\
            join(Pipeline, Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
            filter(Pipeline_seed.seed_table==self.pipeline_seed_table).\
            filter(Pipeline_seed.status==self.pipeline_finished_status).\
            filter(Pipeline.pipeline_name==self.pipeline_name).\
            filter(Project.project_igf_id==project_igf_id).\
            subquery()
      elif self.pipeline_seed_table=='analysis':
        subquery = \
          base.session.\
            query(Sample.sample_igf_id).\
            join(Project,Project.project_id==Sample.project_id).\
            join(Analysis,Analysis.project_id==Project.project_id).\
            join(Pipeline_seed,Pipeline_seed.seed_id==Analysis.analysis_id).\
            join(Pipeline,Pipeline.pipeline_id==Pipeline_seed.pipeline_id).\
            filter(Pipeline_seed.seed_table==self.pipeline_seed_table).\
            filter(Pipeline_seed.status==self.pipeline_finished_status).\
            filter(Project.project_igf_id==project_igf_id).\
            filter(Pipeline.pipeline_name==self.pipeline_name).\
            subquery()
      else:
        raise ValueError(
                'Only sample and experiment tables are supported, got {0}'.\
                  format(self.pipeline_seed_table))
      query = \
        base.session.\
          query(
            Collection.name,
            Collection_attribute.attribute_name,
            Collection_attribute.attribute_value).\
          join(Collection,Collection.collection_id==Collection_attribute.collection_id).\
          filter(Collection.type.in_(self.attribute_collection_file_type)).\
          filter(Collection.name.in_(subquery))
      records = base.fetch_records(query=query)
      base.close_session()
      final_df = pd.DataFrame()
      if len(records.index) > 0:
        for sample,s_data in records.groupby('name'):
          attribute = s_data[['attribute_name','attribute_value']]
          attribute = attribute.set_index('attribute_name').T
          attribute[self.sample_id_label] = sample
          attribute.set_index(self.sample_id_label,inplace=True)
          if len(final_df.index)==0:
            final_df=deepcopy(attribute)
          else:
            final_df = pd.concat([final_df,attribute],sort=True)
      return final_df
    except:
      raise


  def get_analysis_data_for_project(
        self, project_igf_id, output_file, chart_json_output_file=None,
        csv_output_file=None, gviz_out=True, file_path_column='file_path',
        type_column='type', sample_igf_id_column='sample_igf_id'):
    '''
    A method for fetching all the analysis files for a project

    :param project_igf_id: A project igf id for database lookup
    :param output_file: An output filepath, either a csv or a gviz json
    :param gviz_out: A toggle for converting output to gviz output, default is True
    :param sample_igf_id_column: A column name for sample igf id, default sample_igf_id
    :param file_path_column: A column name for file path, default file_path
    :param type_column: A column name for collection type, default type
    '''
    try:
      base = BaseAdaptor(**{'session_class':self.igf_session_class})
      base.start_session()                                                      # connect to database
      if self.pipeline_seed_table=='experiment':
        query = \
          base.session.\
            query(
              Sample.sample_igf_id,
              Collection.type,
              File.file_path).\
            join(Project, Project.project_id==Sample.project_id).\
            join(Experiment, Sample.sample_id==Experiment.sample_id).\
            join(Collection, Experiment.experiment_igf_id==Collection.name).\
            join(Collection_group, Collection.collection_id==Collection_group.collection_id).\
            join(File,File.file_id==Collection_group.file_id).\
            filter(Project.project_igf_id==project_igf_id).\
            filter(Sample.project_id==Project.project_id).\
            filter(Sample.sample_id==Experiment.sample_id).\
            filter(Collection.collection_id==Collection_group.collection_id).\
            filter(Collection_group.file_id==File.file_id).\
            filter(Collection.table=='experiment').\
            filter(Collection.type.in_(self.collection_type_list))
      elif self.pipeline_seed_table=='sample' or \
           self.pipeline_seed_table=='analysis':
        query = \
          base.session.\
            query(
              Sample.sample_igf_id,
              Collection.type,
              File.file_path).\
            join(Project, Project.project_id==Sample.project_id).\
            join(Collection, Sample.sample_igf_id==Collection.name).\
            join(Collection_group, Collection.collection_id==Collection_group.collection_id).\
            join(File, File.file_id==Collection_group.file_id).\
            filter(Project.project_igf_id==project_igf_id).\
            filter(Sample.project_id==Project.project_id).\
            filter(Collection.collection_id==Collection_group.collection_id).\
            filter(Collection_group.file_id==File.file_id).\
            filter(Collection.table=='sample').\
            filter(Collection.type.in_(self.collection_type_list))
      else:
        raise ValueError(
                'Only sample and experiment tables are supported, got {0}'.\
                  format(self.pipeline_seed_table))
      results = \
        base.fetch_records(
          query=query,
          output_mode='dataframe')
      base.close_session()
      temp_dir = get_temp_dir(use_ephemeral_space=self.use_ephemeral_space)
      temp_output = \
        os.path.join(
          temp_dir,
          os.path.basename(output_file))                                        # get a temp output file
      if gviz_out and len(results) > 0:                                          # checking for empty results
        results = pd.DataFrame(results).fillna('')                              # convert to dataframe
        results = \
          results.\
            apply(lambda x: \
              self._add_html_tag(
                data_series=x,
                project_igf_id=project_igf_id,
                sample_igf_id_column=sample_igf_id_column,
                file_path_column=file_path_column),
              axis=1,
              result_type='expand')                                             # add html tags to filepath
        analysis_data = list()
        description = {'Sample':('string','Sample')}
        column_order = ['Sample']
        for sample, sg_data in results.groupby(sample_igf_id_column):           # reformat analysis data
          row_data = dict({'Sample':sample})
          for analysis_type, tg_data in sg_data.groupby(type_column):
            row_data.\
              update({analysis_type:' ;'.join(tg_data[file_path_column].values)})
          analysis_data.append(row_data)                                        # collect analysis data for a sample
        for analysis_type, tg_data in results.groupby(type_column):
          description.\
            update({analysis_type:('string', analysis_type)})
          column_order.append(analysis_type)                                    # fetched description and column order
        convert_to_gviz_json_for_display(
          description=description,
          data=analysis_data,
          columns_order=column_order,
          output_file=temp_output)                                              # write temp gviz json file
      else:
        results.to_csv(temp_output,index=False)                                 # write temp csv file
      copy_local_file(
        temp_output,
        output_file,
        force=True)                                                             # move temp file
      if chart_json_output_file is not None:
        chart_data = \
          self._fetch_collection_attributes(project_igf_id=project_igf_id)      # fetch chart data
        if len(chart_data.index) > 0:
          chart_data = \
            chart_data.\
              applymap(lambda x: str(x).replace(',','').replace('%',''))
          if csv_output_file is not None:
            chart_data.to_csv(
              csv_output_file,
              index=True)                                                       # dump csv output file
          chart_data = chart_data.reset_index().fillna(0)                       # bugfix for missing sample names
          chart_data.columns = \
            [col.\
             replace('(','').\
             replace(')','').\
             replace('%','PCT').\
             replace('-','_')
               for col in chart_data.columns]                                   # bugfix for chart data column names
          temp_chart_output = \
            os.path.join(
              temp_dir,
              os.path.basename(chart_json_output_file))                         # get a temp output file
          column_order = list(chart_data.columns)
          description = {
            column_name : ('string',column_name)
              for column_name in column_order}                                  # prepare chart description
          convert_to_gviz_json_for_display(
            description=description,
            columns_order=column_order,
            data=chart_data.to_dict(orient='records'),
            output_file=temp_chart_output)
          copy_local_file(
            temp_chart_output,
            chart_json_output_file,
            force=True)
    except:
      raise

if __name__=='__main__':
  import json
  from sqlalchemy import create_engine
  from igf_data.utils.dbutils import read_dbconf_json
  from igf_data.utils.fileutils import remove_dir
  from igf_data.igfdb.fileadaptor import FileAdaptor
  from igf_data.igfdb.projectadaptor import ProjectAdaptor
  from igf_data.igfdb.sampleadaptor import SampleAdaptor
  from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
  from igf_data.igfdb.collectionadaptor import CollectionAdaptor
  
  dbconfig = 'data/dbconfig.json'
  dbparam=read_dbconf_json(dbconfig)
  base = BaseAdaptor(**dbparam)
  engine = base.engine
  dbname=dbparam['dbname']
  Base.metadata.drop_all(engine)
  if os.path.exists(dbname):
    os.remove(dbname)
  Base.metadata.create_all(engine)
  base.start_session()
  project_data=[{'project_igf_id':'ProjectA'}]
  pa=ProjectAdaptor(**{'session':base.session})
  pa.store_project_and_attribute_data(data=project_data)                        # load project data
  sample_data=[{'sample_igf_id':'SampleA',
                'project_igf_id':'ProjectA'}]                                   # sample data
  sa=SampleAdaptor(**{'session':base.session})
  sa.store_sample_and_attribute_data(data=sample_data)                          # store sample data
  experiment_data=[{'experiment_igf_id':'ExperimentA',
                    'sample_igf_id':'SampleA',
                    'library_name':'SampleA',
                    'platform_name':'MISEQ',
                    'project_igf_id':'ProjectA'}]                               # experiment data
  ea=ExperimentAdaptor(**{'session':base.session})
  ea.store_project_and_attribute_data(data=experiment_data)
  temp_dir=get_temp_dir()
  temp_files=['a.csv','b.csv']
  for temp_file in temp_files:
    with open(os.path.join(temp_dir,temp_file),'w') as fp:
      fp.write('A')
  collection_data=[{'name':'ExperimentA',
                    'type':'AnalysisA_html',
                    'table':'experiment',
                    'file_path':os.path.join(temp_dir,temp_file)}
                    for temp_file in temp_files]
  ca=CollectionAdaptor(**{'session':base.session})
  ca.load_file_and_create_collection(data=collection_data,
                                     calculate_file_size_and_md5=False)
  base.close_session()
  output_file=os.path.join(temp_dir,'test.json')
  prj_data=Project_analysis(igf_session_class=base.session_class,
                            collection_type_list=['AnalysisA_html'])
  prj_data.get_analysis_data_for_project(project_igf_id='ProjectA',
                                         output_file=output_file)
  with open(output_file,'r') as jp:
    json_data=json.load(jp)
  print(json_data)
  if os.path.exists(dbname):
    os.remove(dbname)
  remove_dir(temp_dir)