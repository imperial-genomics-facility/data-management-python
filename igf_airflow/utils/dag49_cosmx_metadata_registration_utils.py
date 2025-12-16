import logging
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from igf_airflow.utils.generic_airflow_utils import (
  send_airflow_failed_logs_to_channels
)
from igf_data.process.seqrun_processing.unified_metadata_registration import (
  UnifiedMetadataRegistration
)

log = logging.getLogger(__name__)

MS_TEAMS_CONF = Variable.get(
  'analysis_ms_teams_conf',
  default_var=None
)
DATABASE_CONFIG_FILE = Variable.get(
  'database_config_file',
  default_var=None
)
IGF_PORTAL_CONF = Variable.get(
  'igf_portal_conf',
  default_var=None
)
FETCH_METADATA_URL_SUFFIX = Variable.get(
  'cosmx_metadata_fetch_metadata_url',
  default_var='/api/v1/raw_cosmx_metadata/get_raw_metadata/'
)
SYNC_METADATA_URL_SUFFIX = Variable.get(
  'cosmx_metadata_sync_metadata_url',
  default_var='/api/v1/raw_cosmx_metadata/mark_ready_metadata_as_synced'
)
METADATA_VALIDATION_SCHEMA = Variable.get(
  'manual_metadata_validation_schema',
  default_var=None
)
DEFAULT_EMAIL = Variable.get(
  'default_email_user',
  default_var=None
)

## TASK - find raw metadata id in datrun.conf
@task(
  task_id="find_raw_metadata_id",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G'
)
def find_raw_metadata_id(
  raw_cosmx_metadata_id_tag: str = "raw_cosmx_metadata_id",
  dag_run_key: str = "dag_run"
) -> int:
  try:
    ### dag_run.conf should have raw_cosmx_metadata_id
    context = get_current_context()
    dag_run = context.get(dag_run_key)
    raw_cosmx_metadata_id = None
    if (
      dag_run is not None
      and dag_run.conf is not None
      and dag_run.conf.get(raw_cosmx_metadata_id_tag) is not None
    ):
      raw_cosmx_metadata_id = (
        dag_run.conf
        .get(raw_cosmx_metadata_id_tag)
      )
    if raw_cosmx_metadata_id is None:
      raise ValueError(
        'raw_analysis_id not found in dag_run.conf'
      )
    return int(raw_cosmx_metadata_id)
  except Exception as e:
    message = f"Failed to get raw_analysis_id, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message))
    raise ValueError(message)

@task(
  task_id="register_cosmx_metadata",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G'
)
def register_cosmx_metadata(
  raw_cosmx_metadata_id: int
) -> None:
  try:
    metadata_registration = UnifiedMetadataRegistration(
      raw_cosmx_metadata_id=raw_cosmx_metadata_id,
      portal_config_file=IGF_PORTAL_CONF,
      fetch_metadata_url_suffix=FETCH_METADATA_URL_SUFFIX,
      sync_metadata_url_suffix=SYNC_METADATA_URL_SUFFIX,
      metadata_validation_schema=METADATA_VALIDATION_SCHEMA,
      db_config_file=DATABASE_CONFIG_FILE,
      default_project_user_email=DEFAULT_EMAIL,
      samples_required=False
    )
    metadata_registration.execute()
  except Exception as e:
    message = f"Failed to register new cosmx metadata, error: {e}"
    log.error(message)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(message)
    )
    raise ValueError(message)