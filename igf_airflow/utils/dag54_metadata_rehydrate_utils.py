import os
import json
import logging
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from igf_airflow.utils.generic_airflow_utils import (
    send_airflow_failed_logs_to_channels,
)
from igf_airflow.utils.dag25_copy_seqruns_to_hpc_utils import (
    _create_interop_report,
    _load_interop_data_to_db,
    _load_interop_overview_data_to_seqrun_attribute,
    register_new_seqrun_to_db)
from igf_portal.api_utils import upload_files_to_portal
from igf_data.utils.fileutils import (
  get_temp_dir,
  copy_local_file
)
from igf_data.illumina.runinfo_xml import RunInfo_xml
from igf_airflow.utils.dag20_portal_metadata_utils import (
  _get_all_known_projects
)
from igf_data.igfdb.igfTables import Project
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.projectadaptor import ProjectAdaptor


log = logging.getLogger(__name__)

DATABASE_CONFIG_FILE = Variable.get(
  'database_config_file',
  default_var=None
)
MS_TEAMS_CONF = Variable.get(
  'ms_teams_conf',
  default_var=None
)
IGF_PORTAL_CONF = Variable.get(
  'igf_portal_conf',
  default_var=None
)

def _get_project_igf_id_for_project_id(
    project_id: int,
    database_config_file: str
) -> str|None:
  try:
    dbparam = read_dbconf_json(database_config_file)
    pa = ProjectAdaptor(**dbparam)
    pa.start_session()
    query = (
      pa.session
      .query(
        Project.project_igf_id
      )
      .filter(
        Project.project_id==project_id
      )
    )
    project_igf_id = pa.fetch_records(
      query=query,
      output_mode='one_or_none'
    )
    if project_igf_id is None:
      return None
    (project_igf_id,) = project_igf_id
    return project_igf_id
  except Exception as e:
    raise ValueError(
      f"Failed to get project name, error: {e}"
    )

def _remove_target_project_from_list(
    project_list_file: str,
    project_name: str
) -> str:
  try:
    temp_dir = get_temp_dir()
    mod_project_list_file = os.path.join(
      temp_dir,
      'project_list.txt'
    )
    with open(mod_project_list_file, "w") as fpw:
      with open(project_list_file, "r") as fpr:
        for line in fpr:
          if line.strip() != project_name:
            fpw.write(line)
    return mod_project_list_file
  except Exception as e:
    raise ValueError(
      f"Failed to filter project, error: {e}"
    )


@task(
  task_id="get_known_projects_func",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',  
  multiple_outputs=True)
def get_known_projects_func(
  xcom_key: str = 'known_projects',
  project_id_key: str = "project_id",
  dag_run_key: str = "dag_run"
  ) -> dict:
  try:
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    project_id = None
    if (
      dag_run is not None and
      dag_run.conf is not None and
      dag_run.conf.get(project_id_key) is not None
    ):
      project_id = dag_run.conf.get(
        project_id_key
      )
    if project_id is None:
      raise ValueError(
        'project_id not found in dag_run.conf'
      )
    ## get project_name
    project_name = _get_project_igf_id_for_project_id(
      project_id=project_id,
      database_config_file=DATABASE_CONFIG_FILE
    )
    project_list_file = _get_all_known_projects(
        db_conf_file=DATABASE_CONFIG_FILE
    )
    mod_project_list_file = _remove_target_project_from_list(
      project_list_file=project_list_file,
      project_name=project_name
    )
    return {xcom_key: mod_project_list_file}
  except Exception as e:
    message = f"Failed to get known project list, error: {e}"
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=message
    )
    raise ValueError(e)

def _find_and_move_metadata_for_current_project(
    metadata_dir: str,
    project_name: str,
    new_metadata_dir: str
) -> None:
  try:
    for f in os.listdir(metadata_dir):
      if project_name in f:
        copy_local_file(
          source_path=os.path.join(metadata_dir, f),
          destination_path=os.path.join(new_metadata_dir, f)
        )
  except Exception as e:
    raise ValueError(
      f"Failed to move current metadata file, error: {e}"
    )

@task(
  task_id="get_current_metadata_files_func",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G',  
  multiple_outputs=True)
def get_current_metadata_files_func(
  metadata_dir: str,
  metadata_dir_key: str ='metadata_dir',
  project_id_key: str = "project_id",
  dag_run_key: str = "dag_run"
) -> dict:
  try:
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    project_id = None
    if (
      dag_run is not None and
      dag_run.conf is not None and
      dag_run.conf.get(project_id_key) is not None
    ):
      project_id = dag_run.conf.get(
        project_id_key
      )
    if project_id is None:
      raise ValueError(
        'project_id not found in dag_run.conf'
      )
    ## get project_name
    project_name = _get_project_igf_id_for_project_id(
      project_id=project_id,
      database_config_file=DATABASE_CONFIG_FILE
    )
    new_metadata_dir = get_temp_dir()
    _find_and_move_metadata_for_current_project(
      metadata_dir=metadata_dir,
      project_name=project_name,
      new_metadata_dir=new_metadata_dir
    )
    return {metadata_dir_key: new_metadata_dir}
  except Exception as e:
    message = f"Failed to select current project metadata, error: {e}"
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=message
    )
    raise ValueError(e)
