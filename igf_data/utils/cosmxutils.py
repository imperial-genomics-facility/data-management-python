from typing import Dict, List
from igf_data.igfdb.baseadaptor import BaseAdaptor
from sqlalchemy.orm.session import Session, sessionmaker
from igf_data.igfdb.igfTables import (
  Cosmx_platform,
  Cosmx_run,
  Project,
  Cosmx_slide,
  Cosmx_fov,
  Cosmx_fov_annotation,
  Cosmx_fov_rna_qc,
  Cosmx_fov_protein_qc,
  Cosmx_slide_attribute,
  Cosmx_fov_attribute)

def check_and_register_cosmx_run(
    project_igf_id: str,
    cosmx_run_igf_id: str,
    cosmx_platform_name: str,
    db_session_class: sessionmaker) \
      -> bool:
  """
  """
  try:
    status = False
    ## connect to database
    base = BaseAdaptor(**{"session_class": db_session_class})
    base.start_session()
    ## step 1: check if cosmx run exists
    run_check_query = \
      base.session.\
        query(Cosmx_run.cosmx_run_igf_id).\
        filter(Cosmx_run.cosmx_run_igf_id == cosmx_run_igf_id)
    existing_cosmx_run_id = \
      base.fetch_records(
        query=run_check_query,
        output_mode='one_or_none')
    if existing_cosmx_run_id is not None:
      ## step 1.1: check if the existing run is linked with same project
      run_project_query = \
        base.session.\
          query(Project.project_igf_id).\
          join(Cosmx_run, Cosmx_run.project_id == Project.project_id).\
          filter(Cosmx_run.cosmx_run_igf_id == cosmx_run_igf_id)
      existing_project_igf_id = \
        base.fetch_records(
          query=run_project_query,
          output_mode='one_or_none')
      if project_igf_id != existing_project_igf_id:
        base.close_session()
        raise ValueError(
          f"Cosmx run {cosmx_run_igf_id} was registed with {existing_project_igf_id} before. \
            Now its linked to {project_igf_id}!")
      base.close_session()
      return status
    ## step 2: check if the project exists
    project_query = \
      base.session.\
        query(Project.project_id).\
        filter(Project.project_igf_id == project_igf_id)
    project_id = \
      base.fetch_records(
        query=project_query,
        output_mode='one_or_none')
    if project_id is None:
      base.close_session()
      raise ValueError(
        f"Project {project_igf_id} is not registered in DB")
    ## step 3: check if the platform exists
    platform_query = \
      base.session.\
        query(Cosmx_platform.cosmx_platform_id).\
        filter(Cosmx_platform.cosmx_platform_name == cosmx_platform_name)
    platform_id = \
      base.fetch_records(
        query=platform_query,
        output_mode='one_or_none')
    if platform_id is None:
      base.close_session()
      raise ValueError(
        f"Cosmx platform {cosmx_platform_name} is not registered in DB")
    ## step 4: register new cosmx run
    try:
      cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id=cosmx_run_igf_id,
          project_id=project_id,
          cosmx_platform_id=platform_id)
      base.session.add(cosmx_run)
      base.session.flush()
      base.commit_session()
      base.close_session()
      status = True
    except Exception as e:
      base.rollback_session()
      base.close_session()
    return status
  except Exception as e:
    raise ValueError(
      f"Failed to register new cosmx run, error: {e}")



def check_and_register_cosmx_slide(
  cosmx_run_id: str,
  cosmx_slide_name: str,
  panel_info: str,
  assay_type: str,
  slide_metadata: List[Dict[str, str]]) -> bool:
  """
  """
  try:
    status = False
    return status
  except Exception as e:
    raise ValueError(e)


def create_or_update_cosmx_slide_fov(
  cosmx_slide_name: str,
  fov_range: str,
  slide_type: str) -> bool:
  """
  """
  try:
    status = False
    return status
  except Exception as e:
    raise ValueError(e)


def create_or_update_cosmx_slide_fov_annotation(
  cosmx_slide_name: str,
  fov_range: str,
  tissue_annotation: str,
  tissue_ontology: str,
  species: str) -> bool:
  """
  """
  try:
    status = False
    return status
  except Exception as e:
    raise ValueError(e)


def create_or_update_cosmx_slide_fov_count_qc(
  cosmx_slide_name: str,
  slide_count_qc_csv: str) -> str:
  """
  """
  try:
    slide_type = 'UNKNOWN'
    return slide_type
  except Exception as e:
    raise ValueError(e)