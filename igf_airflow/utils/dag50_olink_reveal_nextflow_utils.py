import os
import logging
import pandas as pd
from typing import Any
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import task
from igf_airflow.utils.generic_airflow_utils import (
  send_airflow_failed_logs_to_channels
)
from igf_data.utils.fileutils import (
  check_file_path
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
OLINK_NEXTFLOW_CONF_TEMPLATE = Variable.get(
    'olink_nextflow_conf_template',
    default_var=None
)
OLINK_NEXTFLOW_runner_TEMPLATE = Variable.get(
    "olink_nextflow_runner_template",
    default_var=None
)

@task(
  task_id="prepare_olink_nextflow_script",
  retry_delay=timedelta(minutes=5),
  retries=4,
  queue='hpc_4G')
def prepare_olink_nextflow_script(
      design_dict: dict,
      work_dir: str
    ) -> str:
  try:
    pass
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)


@task.bash(
  task_id="run_olink_nextflow_script",
  retry_delay=timedelta(minutes=5),
  queue='hpc_16G',
  retries=4)
def run_olink_nextflow_script(run_script: str):
  try:
    bash_cmd = f"""set -eo pipefail;
      bash {{ run_script }}"""
    return bash_cmd
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)