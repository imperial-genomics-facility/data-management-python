import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import check_file_path
from sqlalchemy.sql import func
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from igf_data.igfdb.igfTables import Project,Sample,Sample_attribute,Experiment,Run,Run_attribute


class Project_pooling_info:
  '''
  A python class for generating project specific sample pooling information.
  This class fetches sample information from db and creates a gviz format json for remote website.

  :param dbconfig_file: A json file containing databse config details
  :param platform_list: List of platforms to consider for db query, default ['HISEQ4000','NEXTSEQ']
  :param expected_read_tag: Label for expected reads, default expected_read
  :param r1_read_tag: Label for r1 read count, default R1_READ_COUNT
  :param total_read_tag: Label for total read count tag, default total_read
  :param project_column: Label for project column in dataframe, default project
  :param remote_prefix: URI refix for projects, default http://eliot.med.ic.ac.uk/report/project/
  '''
  def __init__(self,dbconfig_file,
               platform_list=('HISEQ4000','NEXTSEQ'),
               expected_read_tag='expected_read',
               r1_read_tag='R1_READ_COUNT',
               total_read_tag='total_read',
               project_column='project',
               remote_prefix='http://eliot.med.ic.ac.uk/report/project/'
              ):
    self.dbconfig_file=dbconfig_file
    self.platform_list=list(platform_list)
    self.expected_read_tag=expected_read_tag
    self.r1_read_tag=r1_read_tag
    self.total_read_tag=total_read_tag
    self.project_column=project_column
    self.remote_prefix=remote_prefix

  def _fetch_project_info_from_db(self):
    '''
    An internal method for fetching data from db

    :returns: A dataframe containing following columns
    
              project_igf_id,
              sample_igf_id,
              expected_read,
              total_read
    '''
    try:
      check_file_path(self.dbconfig_file)
      dbconf = read_dbconf_json(self.dbconfig_file)
      sa = SampleAdaptor(**dbconf)
      sa.start_session()
      query = sa.session.\
              query(Project.project_igf_id,
                    Sample.sample_igf_id,
                    func.max(Sample_attribute.attribute_value).label(self.expected_read_tag),
                    func.sum(Run_attribute.attribute_value).label(self.total_read_tag)
                   ).\
              outerjoin(Sample,Project.project_id==Sample.project_id).\
              outerjoin(Sample_attribute, Sample.sample_id==Sample_attribute.sample_id).\
              outerjoin(Experiment, Sample.sample_id==Experiment.sample_id).\
              outerjoin(Run,Experiment.experiment_id==Run.experiment_id).\
              outerjoin(Run_attribute,Run.run_id==Run_attribute.run_id).\
              filter((Experiment.platform_name.in_(self.platform_list))|(Experiment.platform_name.is_(None))).\
              filter(Sample_attribute.attribute_name==self.expected_read_tag).\
              filter((Run_attribute.attribute_name==self.r1_read_tag)|(Run_attribute.attribute_name.is_(None))).\
              group_by(Sample.sample_igf_id)
      records = sa.fetch_records(query=query,
                                 output_mode='dataframe')
      sa.close_session()
      records[self.total_read_tag] = records[self.total_read_tag].fillna(0).astype(int)
      return records
    except:
      raise

  def _transform_db_data(self,data):
    '''
    An internal method for transforming raw data for web output

    :param data: A pandas dataframe
    :returns: A pandas dataframe
    '''
    try:
      if not isinstance(data,pd.DataFrame):
        raise ValueError('Expecting a pandas dataframe and got {0}'.\
                         format(type(data)))

      data_list = list()
      failed_samples = list()
      data.fillna(0,inplace=True)
      for project, p_data in data.groupby('project_igf_id'):
        total_samples = 0
        pass_samples = 0
        fail_samples = 0
        zero_samples = 0
        total_samples = len(list(p_data.groupby('sample_igf_id').groups.keys()))
        for sample, s_data in p_data.groupby('sample_igf_id'):
          if s_data[self.expected_read_tag].astype(int).max() < s_data[self.total_read_tag].astype(int).sum():
            pass_samples += 1
          elif s_data[self.total_read_tag].sum() == 0:
            zero_samples += 1
          else:
            fail_samples += 1
            failed_samples.append(sample)

        data_list.\
          append({'project': project,
                  'total_samples': total_samples,
                  'pass_samples': pass_samples,
                  'fail_samples': fail_samples,
                  'zero_samples': zero_samples,
                  'expected_read': s_data[self.expected_read_tag].max(),
                  'failed_list': ','.join(failed_samples)
                })

      data_list = pd.DataFrame(data_list)                                       # combine data for all projects
      return data_list
    except:
      raise

  def _add_html_tag(self,data_series):
    '''
    An internal method for reformatting filepath with html code
    
    :param data_series: A pandas series with 'file_path' and 'sample_igf_id'
    :param project_column: A column name for file path, default file_path
    :param remote_prefix: A remote path prefix, default analysis
    :returns: A pandas series
    '''
    try:
      if self.project_column in data_series:
        project = data_series.get(self.project_column)
        project_path = '<a href=\"{0}{1}\">{1}</a>'.\
                       format(self.remote_prefix,
                              project)
        data_series[self.project_column]=project_path
      return data_series
    except:
      raise

  def fetch_db_data_and_prepare_gviz_json(self,output_file_path):
    '''
    A method for fetching pooling information from db and prepare gviz json format output

    :param output_file_path: Path of writing gviz json
    :returns: None
    '''
    try:
      records = self._fetch_project_info_from_db()                              # fetch data from database
      records = self._transform_db_data(data=records)                           # transform raw data
      records = records.apply(lambda x: self._add_html_tag(x),
                              axis=1)                                           # add html tags to projects
      records['total_samples'] = records['total_samples'].astype(int)
      records['expected_read'] = records['expected_read'].astype(int)
      records['pass_samples'] = records['pass_samples'].astype(int)
      records['fail_samples'] = records['fail_samples'].astype(int)
      records['zero_samples'] = records['zero_samples'].astype(int)
      convert_to_gviz_json_for_display(\
        data=records.to_dict(orient='records'),
        description={'project' : ('string','Project'),
                     'total_samples': ('number','Total samples'),
                     'expected_read': ('number','Expected reads'),
                     'pass_samples': ('number','Pass samples'),
                     'fail_samples': ('number','Failed samples'),
                     'zero_samples': ('number','Zero read samples'),
                     'failed_list': ('string','List of failed samples'),
                    },
        columns_order=['project',
                       'total_samples',
                       'expected_read',
                       'pass_samples',
                       'fail_samples',
                       'zero_samples',
                       'failed_list'
                       ],
        output_file=output_file_path
      )
    except:
      raise