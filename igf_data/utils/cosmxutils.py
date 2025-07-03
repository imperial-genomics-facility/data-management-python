import json
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
    ## step 3: register new cosmx run
    try:
      cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id=cosmx_run_igf_id,
          project_id=project_id[0])
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
  cosmx_run_igf_id: str,
  cosmx_slide_igf_id: str,
  cosmx_platform_igf_id: str,
  panel_info: str,
  assay_type: str,
  version: str,
  db_session_class: sessionmaker,
  slide_metadata: List[Dict[str, str]]) -> bool:
  """
  """
  try:
    status = False
    ## connect to database
    base = BaseAdaptor(**{"session_class": db_session_class})
    base.start_session()
    ## step1: check if slide is registered
    slide_query = \
      base.session.\
        query(Cosmx_slide.cosmx_slide_igf_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id)
    existing_slide = \
      base.fetch_records(
        query=slide_query,
        output_mode='one_or_none')
    if existing_slide is not None:
      base.close_session()
      return status
    ## step2: check if run is registered
    run_query = \
      base.session.\
        query(Cosmx_run.cosmx_run_id).\
        filter(Cosmx_run.cosmx_run_igf_id == cosmx_run_igf_id)
    cosmx_run_id = \
      base.fetch_records(
        query=run_query,
        output_mode='one_or_none')
    if cosmx_run_id is None:
      base.close_session()
      raise ValueError(
        f"Cosmx run {cosmx_run_igf_id} is not in DB")
    ## step3: check if platform is registered
    platform_query = \
      base.session.\
        query(Cosmx_platform.cosmx_platform_id).\
        filter(Cosmx_platform.cosmx_platform_igf_id == cosmx_platform_igf_id)
    platform_id = \
      base.fetch_records(
        query=platform_query,
        output_mode='one_or_none')
    ## step4: check if the platform exists
    if platform_id is None:
      base.close_session()
      raise ValueError(
        f"Cosmx platform {cosmx_platform_igf_id} is not registered in DB")
    ## step5: registrer new slide
    try:
      cosmx_slide = \
        Cosmx_slide(
          cosmx_slide_igf_id=cosmx_slide_igf_id,
          cosmx_run_id=cosmx_run_id[0],
          cosmx_platform_id=platform_id[0],
          panel_info=panel_info,
          assay_type=assay_type,
          version=version,
          slide_metadata=json.dumps(slide_metadata))
      base.session.add(cosmx_slide)
      base.session.flush()
      base.commit_session()
      base.close_session()
      status = True
    except Exception as e:
      base.close_session()
      raise ValueError(
        f"Failed to load cosmx slide, error; {e}")
    return status
  except Exception as e:
    raise ValueError(
      f"Failed registering cosmx slide {cosmx_slide_igf_id}, error: {e}")


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