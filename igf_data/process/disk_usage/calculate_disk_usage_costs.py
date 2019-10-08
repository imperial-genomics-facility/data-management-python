import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.utils.disk_usage_utils import get_sub_directory_size_in_gb
from igf_data.utils.fileutils import copy_remote_file, get_temp_dir, remove_dir
from igf_data.igfdb.igfTables import Project,ProjectUser,User,Sample,Experiment,Seqrun,Run

def calculate_igf_disk_usage_costs(list_of_dir_paths,dbconf_file,costs_unit=2.8):
  '''
  A function for calculating disk usage costs

  :param list_of_dir_paths: A list of dictionary containing directory paths to check for disk usage
                            e.g. [{'tag':'location1','path':'/path'}]
  :param dbconf_file: A db config file path
  :param costs_unit: A cost unit, default 2.8
  :returns: A list of dictionaries, a list of column names and a dictionary of descriptions for gviz conversion
  '''
  try:
    merged_df = pd.DataFrame(columns=['path'])
    for entry in list_of_dir_paths:
      tag_name = entry.get('tag')
      path = entry.get('path')
      storage_stats, _, _ = \
        get_sub_directory_size_in_gb(input_path=path)
      temp_df = pd.DataFrame(storage_stats,columns=['path',tag_name])
      merged_df = \
        merged_df.\
        set_index('path').\
        join(\
          temp_df.set_index('path'),
          how='outer').\
        fillna(0).\
        reset_index()
    
    dbparam = read_dbconf_json(dbconf_file)
    base = BaseAdaptor(**dbparam)
    base.start_session()
    query = \
    base.session.\
    query(\
      Project.project_igf_id,
      User.name,
      User.email_id,
      Seqrun.flowcell_id).\
    join(ProjectUser,Project.project_id==ProjectUser.project_id).\
    join(User,User.user_id==ProjectUser.user_id).\
    join(Sample,Project.project_id==Sample.project_id).\
    join(Experiment,Sample.sample_id==Experiment.sample_id).\
    join(Run,Experiment.experiment_id==Run.experiment_id).\
    join(Seqrun,Run.seqrun_id==Seqrun.seqrun_id)
    records = base.fetch_records(query=query)
    base.close_session()
    final_df = \
      merged_df.\
      set_index('index').\
      join(\
        records.set_index('project_igf_id'),
        how='outer').\
      fillna(0)
    final_df = \
      final_df[final_df['email_id'] != 'igf@imperial.ac.uk']
    project_grouped_data = \
      final_df.\
      drop_duplicates().\
      reset_index().\
      groupby('index').\
      agg({\
        'raw_data':'min',
        'results_data':'min',
        'name':'min',
        'email_id':'min',
        'flowcell_id':';'.join
      })
    user_groupped_data = \
      project_grouped_data.\
      reset_index().\
      groupby('email_id').\
      agg({\
        'index':';'.join,
        'raw_data':'sum',
        'results_data':'sum',
        'name':'min',
        'flowcell_id':';'.join
      })
    formatted_df = \
      user_groupped_data.\
      apply(\
        lambda x: __calculate_storage_costs(series=x),
        axis=1).\
      sort_values(\
        'costs_pm',
        ascending=False,
        inplace=True)
    formatted_data = \
      formatted_df.\
      reset_index().\
      to_dict(orient='records')
    return formatted_data

  except Exception as e:
    raise ValueError('Error: {0}'.format(e))

def __calculate_storage_costs(series):
  try:
    raw_data = series.get('raw_data')
    results_data = series.get('results_data')
    costs_unit = 2.8
    if (raw_data + results_data) > 0:
      costs_pm = (raw_data + results_data) * 2.8
    else:
      costs_pm = 0
    series['costs_pm'] = costs_pm
    return series
  except Exception as e:
    raise ValueError('Error: {0}'.format(e))