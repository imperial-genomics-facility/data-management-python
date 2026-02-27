import os
import logging
from pathlib import Path
from datetime import timedelta
from airflow.models import Variable
from airflow.decorators import task
from igf_data.utils.fileutils import (
  check_file_path
)
from igf_airflow.utils.generic_airflow_utils import (
    get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf,
    send_airflow_failed_logs_to_channels,
    parse_analysis_design_and_get_metadata,
    _create_output_from_jinja_template
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
OLINK_NEXTFLOW_RUNNER_TEMPLATE = Variable.get(
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
      work_dir: str,
      analysis_design_key: str = "analysis_design",
      seqrun_metadata_key: str = "seqrun_metadata",
      run_base_dir_key: str = "run_base_dir",
      reveal_fixed_lod_csv_key: str = "reveal_fixed_lod_csv",
      sample_type_key: str = "sample_type",
      dataAnalysisRefIds_key: str = "dataAnalysisRefIds",
      panelDataArchive_key: str = "panelDataArchive",
      plate_design_csv_key: str = "plate_design_csv",
      indexPlate_key: str = "indexPlate",
      plate_design_csv_file: str = "plate_design.csv"
    ) -> str:
  try:
    ## read design and get sample metadata
    design_file = \
      design_dict.get(analysis_design_key)
    check_file_path(design_file)
    with open(design_file, 'r') as fp:
      input_design_yaml = fp.read()
    seqrun_list, metadata = parse_analysis_design_and_get_metadata(
      sample_metadata_key=seqrun_metadata_key,
      input_design_yaml=input_design_yaml
    )
    if (
      seqrun_list is None or
      len(seqrun_list) == 0
    ):
      raise KeyError(
        f"No entry for {seqrun_metadata_key} found in {design_file}."
      )
    if metadata is None:
      raise KeyError(
        f"No analysis metadata found in file {design_file}"
      )
    ## check required metadata
    run_base_dir = metadata.get(run_base_dir_key)
    reveal_fixed_lod_csv = metadata.get(reveal_fixed_lod_csv_key)
    sample_type = metadata.get(sample_type_key)
    dataAnalysisRefIds = metadata.get(dataAnalysisRefIds_key)
    panelDataArchive = metadata.get(panelDataArchive_key)
    plate_design_csv = metadata.get(plate_design_csv_key)
    indexPlate = metadata.get(indexPlate_key)
    if (
      run_base_dir is None or
      reveal_fixed_lod_csv is None or
      sample_type is None or
      dataAnalysisRefIds is None or
      panelDataArchive is None or
      plate_design_csv is None or
      indexPlate is None
    ):
      raise KeyError(
        f"Required metadata is not set correctly. Check {design_file}"
      )
    ## prepare plate_design_csv
    if not isinstance(plate_design_csv, list):
      raise ValueError(
        "Plate design csv data should be in the list format, got " +
        type(plate_design_csv)
      )
    plate_design_file = Path(work_dir) / plate_design_csv_file
    with open(plate_design_file.as_posix(), "w") as fp:
      fp.write("\n".join(plate_design_csv))
    ## get project name
    _, project_name = get_analysis_id_and_project_igf_id_from_airflow_dagrun_conf(
      DATABASE_CONFIG_FILE
    )
    ## render conf
    nf_conf_file = Path(work_dir) / os.path.basename(OLINK_NEXTFLOW_CONF_TEMPLATE)
    _create_output_from_jinja_template(
      template_file=OLINK_NEXTFLOW_CONF_TEMPLATE,
      output_file=nf_conf_file.as_posix(),
      autoescape_list=['xml', 'html'],
      data=dict(
        RUN_ID=seqrun_list[0],
        RUN_BASE_DIR=run_base_dir,
        PLATE_DESIGN_CSV=plate_design_file,
        PROJECT_NAME=project_name,
        SAMPLE_TYPE=sample_type,
        DATA_ANALYSIS_REF_IDS=dataAnalysisRefIds,
        INDEX_PLATE=indexPlate,
        WORKDIR=work_dir
      )
    )
    ## render runner script
    nf_script_file = Path(work_dir) / os.path.basename(OLINK_NEXTFLOW_RUNNER_TEMPLATE)
    _create_output_from_jinja_template(
      template_file=OLINK_NEXTFLOW_RUNNER_TEMPLATE,
      output_file=nf_script_file.as_posix(),
      autoescape_list=['xml', 'html'],
      data=dict(
        CONFIG_FILE=nf_conf_file,
        WORKDIR=work_dir
      )
    )
    return nf_script_file
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
bash { run_script }"""
    return bash_cmd
  except Exception as e:
    log.error(e)
    send_airflow_failed_logs_to_channels(
      ms_teams_conf=MS_TEAMS_CONF,
      message_prefix=str(e))
    raise ValueError(e)