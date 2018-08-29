from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_dbconf_json


def load_new_pipeline_data(data_file, dbconfig):
  '''
  A method for loading new data for pipeline table
  '''
  try:
    formatted_data=read_json_data(data_file)
    dbparam=read_dbconf_json(dbconfig)
    pp=PipelineAdaptor(**dbparam)
    pp.start_session()
    pp.store_pipeline_data(data=formatted_data)
    pp.close_session()
  except:
    raise

def find_new_analysis_seeds(dbconfig_path,slack_config,pipeline_name,project_name_file,
                            species_name_list,fastq_type,library_source_list):
  '''
  A utils method for finding and seeding new experiments for anlsysis
  
  :param dbconfig_path: A database configuration file
  :param slack_config: A slack configuration file
  :param pipeline_name:Pipeline name
  :param fastq_type: Fastq collection type
  :param project_name_file: A file containing the list of projects for seeding pipeline
  :param species_name_list: A list of species to consider for seeding analysis
  :param library_source_list: A list of library source info to consider for seeding analysis
  :returns: List of alvailabe experiments or None
  '''
  try:
    slack_obj=IGF_slack(slack_config=slack_config)                              # get slack instance
    available_exps=None
    if not os.path.exists(project_name_file):
      raise IOError('File {0} not found'.format(project_name_file))

    with open(project_name_file,'r') as fp:
      project_list=fp.readlines()                                               # read list of projects from file,
      project_list=[i.strip() for i in project_list]
      if len(project_list)==0:
        project_list=None

    dbparam=read_dbconf_json(dbconfig_path)
    pl=PipelineAdaptor(**dbparam)
    pl.start_session()
    available_exps=pl.\
                   seed_new_experiments(\
                     pipeline_name=pipeline_name,
                     species_name_list=species_name_list,
                     fastq_type=fastq_type,
                     project_list=project_list,
                     library_source_list=library_source_list
                   )
    pl.close_session()
    return available_exps
  except:
    raise