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
    cosmx_run_id: str,
    cosmx_platform_name: str,
    db_session_class: sessionmaker) \
      -> bool:
  """
  """
  try:
    status = False
    return status
  except Exception as e:
    raise ValueError(e)


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