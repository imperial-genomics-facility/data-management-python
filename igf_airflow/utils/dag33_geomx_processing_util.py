import os
import re
import json
import yaml
import shutil
import zipfile
import logging
import pandas as pd
from typing import (
    Tuple,
    Optional)
from datetime import timedelta
from airflow.models import Variable
from igf_data.utils.bashutils import bash_script_wrapper
from igf_data.utils.analysis_fastq_fetch_utils import get_fastq_and_run_for_samples
from jinja2 import Template
from yaml import (
    Loader,
    Dumper)
from typing import (
    Tuple,
    Union)
from igf_data.igfdb.igfTables import (
    Pipeline,
    Pipeline_seed,
    Project,
    Analysis)
from igf_data.utils.fileutils import (
    check_file_path,
    copy_local_file,
    get_temp_dir,
    read_json_data,
    get_date_stamp_for_file_name)
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.analysisadaptor import AnalysisAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_airflow.logging.upload_log_msg import send_log_to_channels
from igf_airflow.utils.dag22_bclconvert_demult_utils import _create_output_from_jinja_template
from igf_airflow.utils.dag26_snakemake_rnaseq_utils import (
    fetch_analysis_design,
    parse_analysis_design_and_get_metadata,
    get_project_igf_id_for_analysis,
    calculate_analysis_name,
    load_analysis_and_build_collection,
    copy_analysis_to_globus_dir,
    check_and_seed_analysis_pipeline)
from airflow.operators.python import get_current_context
from airflow.decorators import task

log = logging.getLogger(__name__)

SLACK_CONF = Variable.get('slack_conf',default_var=None)
MS_TEAMS_CONF = Variable.get('ms_teams_conf',default_var=None)
HPC_SSH_KEY_FILE = Variable.get('hpc_ssh_key_file', default_var=None)
DATABASE_CONFIG_FILE = Variable.get('database_config_file', default_var=None)
HPC_BASE_RAW_DATA_PATH = Variable.get('hpc_base_raw_data_path', default_var=None)
IGF_PORTAL_CONF = Variable.get('igf_portal_conf', default_var=None)
HPC_FILE_LOCATION = Variable.get("hpc_file_location", default_var="HPC_PROJECT")

## EMAIL CONFIG
EMAIL_CONFIG = Variable.get("email_config", default_var=None)
EMAIL_TEMPLATE = Variable.get("seqrun_email_template", default_var=None)
DEFAULT_EMAIL_USER = Variable.get("default_email_user", default_var=None)

## GLOBUS
GLOBUS_ROOT_DIR = Variable.get("globus_root_dir", default_var=None)

## VARIABLES
GEOMX_NGS_PIPELINE_EXE = Variable.get("geomx_ngs_pipeline_exe", default_var=None)
GEOMX_SCRIPT_TEMPLATE = Variable.get("geomx_script_template", default_var=None)
REPORT_TEMPLATE_FILE = Variable.get("geomx_report_template_file", default_var=None)

## TASK
@task.branch(
	task_id="mark_analysis_running",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def mark_analysis_running(
        next_task: str,
        last_task: str,
        seed_table: str = 'analysis',
        new_status: str = 'RUNNING',
        no_change_status: list = ['RUNNING', 'FAILED', 'FINISHED', 'UNKNOWN']) -> list:
    try:
        ## dag_run.conf should have analysis_id
        context = get_current_context()
        dag_run = context.get('dag_run')
        analysis_id = None
        if dag_run is not None and \
           dag_run.conf is not None and \
           dag_run.conf.get('analysis_id') is not None:
            analysis_id = \
                dag_run.conf.get('analysis_id')
        if analysis_id is None:
            raise ValueError('analysis_id not found in dag_run.conf')
        ## pipeline_name is context['task'].dag_id
        pipeline_name = context['task'].dag_id
        ## change seed status
        seed_status = \
            check_and_seed_analysis_pipeline(
                analysis_id=analysis_id,
                pipeline_name=pipeline_name,
                dbconf_json_path=DATABASE_CONFIG_FILE,
                new_status=new_status,
                seed_table=seed_table,
                no_change_status=no_change_status)
        ## set next tasks
        task_list = list()
        if seed_status:
            task_list.append(
                next_task)
        else:
            task_list.append(
                last_task)
            send_log_to_channels(
                slack_conf=SLACK_CONF,
                ms_teams_conf=MS_TEAMS_CONF,
                task_id=context['task'].task_id,
                dag_id=pipeline_name,
                project_id=None,
                comment=f"No task for analysis: {analysis_id}, pipeline: {pipeline_name}",
                reaction='pass')
        return task_list
    except Exception as e:
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)

## TASK
## CHANGE ME: Once EmptyOperator has a decorator then remove this
@task(
    task_id="no_work",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def no_work() -> None:
	try:
		pass
	except Exception as e:
		raise ValueError(e)


def fetch_analysis_yaml_and_dump_to_a_file(
        analysis_id: int,
        pipeline_name: str,
        dbconfig_file: str) -> str:
    try:
        ## get analysis design
        input_design_yaml = \
	        fetch_analysis_design(
		        analysis_id=analysis_id,
                pipeline_name=pipeline_name,
		        dbconfig_file=dbconfig_file)
        temp_dir = \
	        get_temp_dir(use_ephemeral_space=True)
        temp_yaml_file = \
            os.path.join(temp_dir, 'analysis_design.yaml')
        ## dump it in a text file for next task
        with open(temp_yaml_file, 'w') as fp:
            fp.write(input_design_yaml)
        return temp_yaml_file
    except Exception as e:
        message = f"Failed to get yaml, error: {e}"
        raise ValueError(message)


## TASK
@task(
	task_id="fetch_analysis_design",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def fetch_analysis_design_from_db() -> dict:
    try:
        ## dag_run.conf should have analysis_id
        context = get_current_context()
        dag_run = context.get('dag_run')
        analysis_id = None
        if dag_run is not None and \
           dag_run.conf is not None and \
           dag_run.conf.get('analysis_id') is not None:
            analysis_id = \
                dag_run.conf.get('analysis_id')
        if analysis_id is None:
            raise ValueError('analysis_id not found in dag_run.conf')
        ## pipeline_name is context['task'].dag_id
        pipeline_name = context['task'].dag_id
        ## get analysis design file
        temp_yaml_file = \
            fetch_analysis_yaml_and_dump_to_a_file(
                analysis_id=analysis_id,
                pipeline_name=pipeline_name,
                dbconfig_file=DATABASE_CONFIG_FILE)
        return {'design_file': temp_yaml_file}
    except Exception as e:
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def extract_geomx_config_files_from_zip(zip_file: str) -> Tuple[str, str]:
    try:
        check_file_path(zip_file)
        ## get temp dir and copy zip file to it
        temp_dir = get_temp_dir(use_ephemeral_space=True)
        temp_zip_file = os.path.join(temp_dir, 'geomx_config.zip')
        ## get extract dir
        extract_dir = os.path.join(temp_dir, 'extract')
        os.makedirs(extract_dir)
        ## copy zip file to temp dir
        shutil.copy2(zip_file, temp_zip_file)
        ## extract zip file
        with zipfile.ZipFile(temp_zip_file, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        config_file = list()
        labworksheet_file = list()
        config_file = [
		    os.path.join(extract_dir, f)
	            for f in os.listdir(extract_dir)
		            if f.endswith('.ini')]
        if len(config_file) == 0:
            raise ValueError(
                f"No .ini file present in {zip_file}")
        config_file = config_file[0]
        labworksheet_file = [
		    os.path.join(extract_dir, f)
	            for f in os.listdir(extract_dir)
		            if f.endswith('LabWorksheet.txt')]
        if len(labworksheet_file) == 0:
            raise ValueError(
                f"No LabWorksheet.txt file present in {zip_file}")
        labworksheet_file = labworksheet_file[0]
        return config_file, labworksheet_file
    except Exception as e:
        raise ValueError(
			f"Failed to get config file, error: {e}")

## TASK
@task(
	task_id="check_and_process_config_file",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def check_and_process_config_file(design_file: str) -> dict:
    try:
        check_file_path(design_file)
        with open(design_file, 'r') as fp:
            input_design_yaml = yaml.load(fp, yaml.Loader)
        sample_metadata, analysis_metadata = \
            parse_analysis_design_and_get_metadata(
                input_design_yaml=input_design_yaml)
        if sample_metadata is None or \
	       analysis_metadata is None:
            raise KeyError("Missing sample or analysis metadata")
        config_zip_file = \
            analysis_metadata.get('config_zip_file')
        check_file_path(config_zip_file)
        config_ini_file = None
        labworksheet_file = None
        config_ini_file, labworksheet_file = \
            extract_geomx_config_files_from_zip(
                zip_file=config_zip_file)
        if config_ini_file is None or \
           labworksheet_file is None:
            raise ValueError(f"Missing ini or worksheet in {config_zip_file}")
        config_file_dict = {
			'config_ini_file': config_ini_file,
			'labworksheet_file': labworksheet_file
		}
        return config_file_dict
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def get_fastq_for_samples_and_dump_in_json_file(
        design_file: str,
        db_config_file: str) -> str:
    try:
        check_file_path(design_file)
        with open(design_file, 'r') as fp:
            input_design_yaml=fp.read()
        sample_metadata, analysis_metadata = \
            parse_analysis_design_and_get_metadata(
                input_design_yaml=input_design_yaml)
        if sample_metadata is None or \
           analysis_metadata is None:
            raise KeyError("Missing sample or analysis metadata")
        ## get sample ids from metadata
        sample_igf_id_list = \
            list(sample_metadata.keys())
        if len(sample_igf_id_list) == 0:
            raise ValueError("No sample id found in the metadata")
        ## get fastq files for all samples
        fastq_list = \
            get_fastq_and_run_for_samples(
                dbconfig_file=db_config_file,
                sample_igf_id_list=sample_igf_id_list)
        if len(fastq_list) == 0:
            raise ValueError(
                f"No fastq file found for samples: {design_file}")
        temp_dir = get_temp_dir(use_ephemeral_space=True)
        fastq_list_json = \
            os.path.join(temp_dir, 'fastq_list.json')
        with open(fastq_list_json, 'w') as fp:
                json.dump(fastq_list, fp)
        return fastq_list_json
    except Exception as e:
        raise ValueError(
            f"Failed to create fastq list json, error: {e}")


## TASK
@task(
	task_id="fetch_fastq_file_path_from_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def fetch_fastq_file_path_from_db(design_file: str) -> str:
    try:
        fastq_list_json = \
            get_fastq_for_samples_and_dump_in_json_file(
                design_file=design_file,
                db_config_file=DATABASE_CONFIG_FILE)
        return fastq_list_json
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(
            fastq_list_json: str,
            required_columns: tuple = (
                'sample_igf_id',
                'run_igf_id',
                'flowcell_id',
                'lane_number',
                'file_path') ) -> str:
      try:
            check_file_path(fastq_list_json)
            df = pd.read_json(fastq_list_json) ## use pydantic??
            ## check for required columns
            for c in required_columns:
                if c not in df.columns:
                    raise KeyError(
                        f"Missing required column {c} in json file {fastq_list_json}")
            df['file_name'] = \
                df['file_path'].map(lambda x: os.path.basename(x))
            duplicate_rows = \
                len(df[df['file_name'].duplicated()].index)
            ## get symlink path
            symlink_dir = get_temp_dir(use_ephemeral_space=True)
            symlink_dict = dict()
            pattern = \
                re.compile(r'(\S+)_S(\d+)_(L00\d)_(R[1,2])_001.fastq.gz')
            if duplicate_rows > 0:
                ## make filenames unique
                counter = 999
                for _, g_data in df.groupby(['sample_igf_id', 'run_igf_id', 'flowcell_id', 'lane_number']):
                    counter += 1
                    for r in g_data.to_dict(orient='records'):
                        source_path = r['file_path']
                        source_name = r['file_name']
                        match = re.match(pattern, source_name)
                        if match:
                            (sample_id, s_id, l_id, r_id) = match.groups()
                            dest_name = f"{sample_id}_S{counter}_{l_id}_{r_id}_001.fastq.gz"
                            dest_path = os.path.join(symlink_dir, dest_name)
                            symlink_dict.update({source_path: dest_path})
            else:
                for r in df[['file_path', 'file_name']].to_dict(orient='records'):
                    source_path = r['file_path']
                    dest_name = r['file_name']
                    dest_path = os.path.join(symlink_dir, dest_name)
                    match = re.match(pattern, dest_name)
                    if match:
                        symlink_dict.update({source_path: dest_path})
            ## create symlink
            if len(symlink_dict) == 0:
                 raise ValueError(
                    f"No entry found for symlink creation. json: {fastq_list_json}")
            for source_path, dest_path in symlink_dict.items():
                os.symlink(source_path, dest_path)
            return symlink_dir
      except Exception as e:
            raise ValueError(
                f"Failed to create symlink dir, error: {e}")


## TASK
@task(
	task_id="create_temp_fastq_input_dir",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def create_temp_fastq_input_dir(fastq_list_json: list) -> str:
    try:
        symlink_dir = \
            read_fastq_list_json_and_create_symlink_dir_for_geomx_ngs(fastq_list_json)
        return symlink_dir
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def create_sample_translation_file_for_geomx_script(
        design_file: str,
        dsp_id_key: str = 'dsp_id') -> str:
    try:
        ## check design file
        check_file_path(design_file)
        with open(design_file, 'r') as fp:
            input_design_yaml = fp.read()
        sample_metadata, analysis_metadata = \
            parse_analysis_design_and_get_metadata(
                input_design_yaml=input_design_yaml)
        if sample_metadata is None or \
           analysis_metadata is None:
            raise KeyError("Missing sample or analysis metadata")
        translation_file_data = list()
        for sample_id, sample_data in sample_metadata.items():
            dsp_id = sample_data.get(dsp_id_key)
            if dsp_id is None:
                raise KeyError(
                    f"Missing DSP id for sample {sample_id} in {design_file}")
            translation_file_data.append({
                'sample_id': sample_id,
                'dsp_id': dsp_id})
        temp_translation_dir = \
            get_temp_dir(use_ephemeral_space=True)
        translation_file = \
            os.path.join(temp_translation_dir, "translation.csv")
        df = pd.DataFrame(translation_file_data)
        df[['dsp_id', 'sample_id']].\
            to_csv(
                translation_file,
                index=False,
                header=False)
        return translation_file
    except Exception as e:
        raise ValueError(
            f"Failed to create translation file, error: {e}")


def fetch_geomx_params_from_analysis_design(
        design_file: str,
        param_key: str = 'geomx_dcc_params') -> list:
    try:
        ## check design file
        check_file_path(design_file)
        with open(design_file, 'r') as fp:
            input_design_yaml = fp.read()
        sample_metadata, analysis_metadata = \
            parse_analysis_design_and_get_metadata(
                input_design_yaml=input_design_yaml)
        if sample_metadata is None or \
           analysis_metadata is None:
            raise KeyError("Missing sample or analysis metadata")
        params_list = list()
        if param_key in analysis_metadata:
            params_list = \
                analysis_metadata.get(param_key)
            if not isinstance(params_list, list):
                raise TypeError(f"Expection a list of params, got {params_list}")
        return params_list
    except Exception as e:
        raise ValueError(
            f"Failed to get Geomx ngs pipeline params, error: {e}")


def create_geomx_dcc_run_script(
        geomx_script_template: str,
        geomx_ngs_pipeline_exe: str,
        design_file: str,
        symlink_dir: str,
        config_file_dict: dict) -> Tuple[str, str]:
    try:
        ## check input files
        ## check template file
        check_file_path(geomx_script_template)
        ## check exe file
        check_file_path(geomx_ngs_pipeline_exe)
        ## check design file
        check_file_path(design_file)
        ## check symlink dir
        check_file_path(symlink_dir)
        ## create translation file for run
        translation_file = \
            create_sample_translation_file_for_geomx_script(
                design_file=design_file)
        check_file_path(translation_file)
        ## get ini config file
        config_ini_file = \
            config_file_dict.\
                get('config_ini_file')
        check_file_path(config_ini_file)
        ## fetch params list
        geomx_params_list = \
            fetch_geomx_params_from_analysis_design(
                design_file=design_file)
        ## generate rendered template
        work_dir = \
            get_temp_dir(use_ephemeral_space=True)
        output_dir = \
            get_temp_dir(use_ephemeral_space=True)
        script_file = \
            os.path.join(work_dir, 'geomx_ngs_script.sh')
        _create_output_from_jinja_template(
            template_file=geomx_script_template,
            output_file=script_file,
            autoescape_list=['html',],
            data=dict(
                WORK_DIR=work_dir,
                GEOMX_NGS_EXE=geomx_ngs_pipeline_exe,
                FASTQ_DIR=symlink_dir,
                OUTPUT_DIR=output_dir,
                INPUT_INI_FILE=config_ini_file,
                INPUT_TRANSLATION_FILE=translation_file,
                GEOMX_PARAMS=geomx_params_list))
        return script_file, output_dir
    except Exception as e:
        raise ValueError(
            f"Failed to create dcc run script, error: {e}")


## TASK
@task(
	task_id="prepare_geomx_dcc_run_script",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def prepare_geomx_dcc_run_script(
		design_file: str,
		symlink_dir: str,
		config_file_dict: dict) -> Tuple[str, str]:
    try:
        dcc_script_path, output_dir = \
            create_geomx_dcc_run_script(
                geomx_script_template=GEOMX_SCRIPT_TEMPLATE,
                geomx_ngs_pipeline_exe=GEOMX_NGS_PIPELINE_EXE,
                design_file=design_file,
                symlink_dir=symlink_dir,
                config_file_dict=config_file_dict)
        return {'dcc_script_path': dcc_script_path, 'output_dir': output_dir}
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


## TASK
@task(
	task_id="generate_dcc_count",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_64G16t')
def generate_geomx_dcc_count(dcc_script_dict: dict) -> str:
    script_path = dcc_script_dict.get('dcc_script_path')
    output_path = dcc_script_dict.get('output_dir')
    try:
        stdout_file, stderr_file = \
            bash_script_wrapper(
                script_path=script_path)
        return output_path
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


## TASK
@task(
	task_id="generate_qc_report",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def generate_geomx_qc_report(dcc_count_path: str, design_file: str) -> None:
    try:
        print(dcc_count_path)
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def calculate_md5sum_for_analysis_dir(dir_path: str) -> None:
    try:
        temp_dir = \
            get_temp_dir(use_ephemeral_space=True)
        bash_template = \
            Template("""set -eo pipefail;
            cd {{ TMP_PATH }};
            find {{ DIR_PATH }} -type f -exec md5sum {} \; > file_manifest.md5;
            mv file_manifest.md5 {{ DIR_PATH }}""")
        script_path = \
            os.path.join(temp_dir, 'bash_script.sh')
        rendered_template = \
            bash_template.render(
                TMP_PATH=temp_dir,
                DIR_PATH=dir_path)
        with open(script_path, 'w') as fp:
            fp.write(rendered_template)
        stdout_file, stderr_file = \
            bash_script_wrapper(
                script_path=script_path)
    except Exception as e:
        raise ValueError(
            f"Failed to get md5sum for dir {dir_path}, error: {e}")


## TASK
@task(
	task_id="calculate_md5sum_for_dcc",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_8G')
def calculate_md5sum_for_dcc(dcc_count_path: str) -> None:
    try:
        calculate_md5sum_for_analysis_dir(
            dir_path=dcc_count_path)
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


def collect_analysis_dir(
        analysis_id: int,
        dag_name: str,
        dir_path: str,
        db_config_file:str,
        hpc_base_path: str,
        collection_table: str = 'analysis',
        analysis_dir_prefix: str = 'analysis') -> Tuple[str, str, str]:
    try:
        date_tag = get_date_stamp_for_file_name()
        collection_type = dag_name.upper()
        collection_name = \
        calculate_analysis_name(
            analysis_id=analysis_id,
            date_tag=date_tag,
            dbconfig_file=db_config_file)
        target_dir_path = \
            load_analysis_and_build_collection(
                collection_name=collection_name,
                collection_type=collection_type,
                collection_table=collection_table,
                dbconfig_file=db_config_file,
                analysis_id=analysis_id,
                pipeline_name=dag_name,
                result_dir=dir_path,
                hpc_base_path=hpc_base_path,
                analysis_dir_prefix=analysis_dir_prefix,
                date_tag=date_tag)
        ## get project name
        project_igf_id = \
            get_project_igf_id_for_analysis(
                analysis_id=analysis_id,
                dbconfig_file=db_config_file)
        return target_dir_path, project_igf_id, date_tag
    except Exception as e:
        raise ValueError(
            f"Failed to collect analysis dir, error: {e}")


## TASK
@task(
	task_id="load_dcc_count_to_db",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def load_dcc_count_to_db(
        dcc_count_path: str) -> str:
    try:
        ## dag_run.conf should have analysis_id
        context = get_current_context()
        dag_run = context.get('dag_run')
        analysis_id = None
        if dag_run is not None and \
           dag_run.conf is not None and \
           dag_run.conf.get('analysis_id') is not None:
            analysis_id = \
                dag_run.conf.get('analysis_id')
        if analysis_id is None:
            raise ValueError('analysis_id not found in dag_run.conf')
        ## pipeline_name is context['task'].dag_id
        pipeline_name = context['task'].dag_id
        target_dir_path, project_igf_id, date_tag = \
            collect_analysis_dir(
                analysis_id=analysis_id,
                dag_name=pipeline_name,
                dir_path=dcc_count_path,
                db_config_file=DATABASE_CONFIG_FILE,
                hpc_base_path=HPC_BASE_RAW_DATA_PATH)
        return {'target_dir_path': target_dir_path, 'date_tag': date_tag}
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


## TASK
@task(
	task_id="copy_data_to_globus",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def copy_data_to_globus(analysis_dir_dict: dict) -> None:
    try:
        analysis_dir = analysis_dir_dict.get('target_dir_path')
        date_tag = analysis_dir_dict.get('date_tag')
		## dag_run.conf should have analysis_id
        context = get_current_context()
        dag_run = context.get('dag_run')
        analysis_id = None
        if dag_run is not None and \
           dag_run.conf is not None and \
           dag_run.conf.get('analysis_id') is not None:
            analysis_id = \
                dag_run.conf.get('analysis_id')
        if analysis_id is None:
            raise ValueError('analysis_id not found in dag_run.conf')
        ## pipeline_name is context['task'].dag_id
        pipeline_name = context['task'].dag_id
        target_dir_path = \
            copy_analysis_to_globus_dir(
                globus_root_dir=GLOBUS_ROOT_DIR,
                dbconfig_file=DATABASE_CONFIG_FILE,
                analysis_id=analysis_id,
                analysis_dir=analysis_dir,
                pipeline_name=pipeline_name,
                date_tag=date_tag)
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


## TASK
@task(task_id="send_email_to_user")
def send_email_to_user() -> None:
    try:
        print('send email')
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)


## TASK
@task(
	task_id="mark_analysis_finished",
    retry_delay=timedelta(minutes=5),
    retries=4,
    queue='hpc_4G')
def mark_analysis_finished(
        seed_table: str = 'analysis',
	    new_status: str = 'FINISHED',
        no_change_status: list = ('SEEDED', 'FAILED')) -> None:
    try:
        ## dag_run.conf should have analysis_id
        context = get_current_context()
        dag_run = context.get('dag_run')
        analysis_id = None
        if dag_run is not None and \
           dag_run.conf is not None and \
           dag_run.conf.get('analysis_id') is not None:
            analysis_id = \
                dag_run.conf.get('analysis_id')
        if analysis_id is None:
            raise ValueError('analysis_id not found in dag_run.conf')
        ## pipeline_name is context['task'].dag_id
        pipeline_name = context['task'].dag_id
        ## change seed status
        seed_status = \
            check_and_seed_analysis_pipeline(
                analysis_id=analysis_id,
                pipeline_name=pipeline_name,
                dbconf_json_path=DATABASE_CONFIG_FILE,
                new_status=new_status,
                seed_table=seed_table,
                no_change_status=no_change_status)
    except Exception as e:
        context = get_current_context()
        send_log_to_channels(
            slack_conf=SLACK_CONF,
            ms_teams_conf=MS_TEAMS_CONF,
            task_id=context['task'].task_id,
            dag_id=context['task'].dag_id,
            project_id=None,
            comment=e,
            reaction='fail')
        raise ValueError(e)