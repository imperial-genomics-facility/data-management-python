import os
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
  get_temp_dir
)
import os, json
from igf_data.illumina.runinfo_xml import RunInfo_xml

log = logging.getLogger(__name__)

MS_TEAMS_CONF = Variable.get(
    'analysis_ms_teams_conf',
    default_var=None
)
DATABASE_CONFIG_FILE = Variable.get(
    'database_config_file',
    default_var=None
)
HPC_SEQRUN_PATH = Variable.get(
  'hpc_seqrun_path',
  default_var=None
)
IGF_PORTAL_CONF = Variable.get(
    'igf_portal_conf', default_var=None
)
PORTAL_ADD_SEQRUN_URL =  Variable.get(
  'portal_add_seqrun_url',
  default_var="/api/v1/raw_seqrun/add_new_seqrun"
)
PORTAL_ADD_INTEROP_REPORT_URL = Variable.get(
  'portal_add_interop_report_url',
  default_var="/api/v1/interop_data/add_report"
)
HPC_INTEROP_PATH = Variable.get(
  'hpc_interop_path',
  default_var=None
)
INTEROP_REPORT_TEMPLATE = Variable.get(
  'interop_report_template',
  default_var=None
)
INTEROP_REPORT_IMAGE = Variable.get(
  'interop_report_image',
  default_var=None
)
INTEROP_REPORT_BASE_PATH = Variable.get(
  'interop_report_base_path',
  default_var=None
)
INTEROP_PREDICTION_TEMPLATE = Variable.get(
  'interop_prediction_template',
  default_var=None
)

@task(
  task_id="create_qc_and_load_external_run",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_16G8t')
def create_qc_and_load_external_run(
  external_seqrun_id_key: str = "external_seqrun_id",
  dag_run_key: str = "dag_run"
) -> None:
  try:
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    external_seqrun_id = None
    if (
      dag_run is not None and
      dag_run.conf is not None and
      dag_run.conf.get(external_seqrun_id_key) is not None
    ):
      external_seqrun_id = dag_run.conf.get(
        external_seqrun_id_key
      )
    if external_seqrun_id is None:
      raise ValueError(
        'external_seqrun_id not found in dag_run.conf'
      )
    ## step 1
    (
      output_notebook_path,
      _,
      overview_csv_output,
      _,
      work_dir
    ) = _create_interop_report(
      run_id=external_seqrun_id,
      run_dir_base_path=HPC_SEQRUN_PATH,
      report_template=INTEROP_REPORT_TEMPLATE,
      report_image=INTEROP_REPORT_IMAGE
    )
    ## step 2
    _load_interop_data_to_db(
      run_id=external_seqrun_id,
      interop_output_dir=work_dir,
      interop_report_base_path=INTEROP_REPORT_BASE_PATH,
      dbconfig_file=DATABASE_CONFIG_FILE
    )
    ## step 3
    _ = register_new_seqrun_to_db(
      dbconfig_file=DATABASE_CONFIG_FILE,
      seqrun_id=external_seqrun_id,
      seqrun_base_path=HPC_SEQRUN_PATH
    )
    ## step 4
    runinfo_file_path = os.path.join(
      HPC_SEQRUN_PATH,
      external_seqrun_id,
      'RunInfo.xml'
    )
    runinfo_data = RunInfo_xml(
      xml_file=runinfo_file_path
    )
    formatted_read_stats = (
      runinfo_data
      .get_formatted_read_stats()
    )
    ## step 5
    temp_dir = get_temp_dir(
      use_ephemeral_space=True
    )
    json_data = {
      "seqrun_id_list": [external_seqrun_id,],
      "run_config_list": [formatted_read_stats,]}
    new_run_list_json = os.path.join(
      temp_dir,
      'new_run_list.json'
    )
    with open(new_run_list_json, 'w') as fp:
      json.dump(json_data, fp)
    _ = upload_files_to_portal(
      url_suffix=PORTAL_ADD_SEQRUN_URL,
      portal_config_file=IGF_PORTAL_CONF,
      file_path=new_run_list_json,
      verify=False,
      jsonify=False
    )
    ## step 6
    _load_interop_overview_data_to_seqrun_attribute(
      seqrun_igf_id=external_seqrun_id,
      dbconfig_file=DATABASE_CONFIG_FILE,
      interop_overview_file=overview_csv_output
    )
    _ = upload_files_to_portal(
      portal_config_file=IGF_PORTAL_CONF,
      file_path=output_notebook_path,
      data={"run_name": external_seqrun_id, "tag": "InterOp"},
      url_suffix=PORTAL_ADD_INTEROP_REPORT_URL
    )
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)