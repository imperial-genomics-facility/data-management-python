import json, re
import pandas as pd
from enum import Enum
from typing import Dict, List
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
from jsonschema import (
  Draft4Validator,
  ValidationError)
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir,
  read_json_data,
  check_file_path)
from datetime import datetime
from igf_data.igfdb.baseadaptor import BaseAdaptor
from sqlalchemy.orm.session import Session, sessionmaker


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
      existing_project_row = \
        base.fetch_records(
          query=run_project_query,
          output_mode='one_or_none')
      existing_project_igf_id = \
        existing_project_row.project_igf_id
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
      cosmx_run1 = \
        Cosmx_run(
          cosmx_run_igf_id=cosmx_run_igf_id,
          project_id=project_id[0])
      base.session.add(cosmx_run1)
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
  cosmx_slide_name: str,
  cosmx_platform_igf_id: str,
  panel_info: str,
  assay_type: str,
  slide_run_date: datetime,
  version: str,
  db_session_class: sessionmaker,
  slide_metadata: Dict[str, str]) -> bool:
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
          cosmx_slide_name=cosmx_slide_name,
          cosmx_run_id=cosmx_run_id[0],
          cosmx_platform_id=platform_id[0],
          panel_info=panel_info,
          assay_type=assay_type,
          version=version,
          slide_run_date=slide_run_date,
          slide_metadata=slide_metadata)
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


def fov_range_to_list(fov_range: str) -> List[int]:
  """
  A function to convert a range string like "1-4" to a list [1,2,3,4]

  :param fov_range: A string or fov range like "1-100"
  :returns: A list of integers
  """
  try:
    range_list = list()
    if "," in fov_range:
      range_list = fov_range.strip().split(",")
      range_list = [int(i) for i in range_list]
      return range_list
    range_match = re.match(r'^(\d+)-(\d+)$', fov_range)
    if range_match:
      start, end = map(int, range_match.groups())
      range_list = [int(i) for i in range(start, end + 1)]
    else:
      raise ValueError(f"Incorrect range format.")
    return range_list
  except Exception as e:
    raise ValueError(f"Failed to get a list from range {fov_range}")


def create_or_update_cosmx_slide_fov(
  cosmx_slide_igf_id: str,
  fov_range: str,
  slide_type: str,
  db_session_class: sessionmaker) -> bool:
  """
  """
  try:
    status = False
    ## step1: get a list from fov range
    fov_list = \
      fov_range_to_list(
        fov_range=fov_range)
    if len(fov_list) == 0:
      raise ValueError("No fov range found for slid {cosmx_slide_igf_id}")
    ## check slide type
    if slide_type not in CosmxSlideType.__members__:
      raise KeyError(f"Unknown slide type {slide_type}")
    ## connect to database
    base = BaseAdaptor(**{"session_class": db_session_class})
    base.start_session()
    ## step2: check if slide is registered
    slide_query = \
      base.session.\
        query(Cosmx_slide.cosmx_slide_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id)
    cosmx_slide_entry = \
      base.fetch_records(
        query=slide_query,
        output_mode='one_or_none')
    if cosmx_slide_entry is None:
      base.close_session()
      raise ValueError(
        f"Cosmx slide {cosmx_slide_igf_id} is not in DB")
    cosmx_slide_id = \
      cosmx_slide_entry.cosmx_slide_id
    ## step3: check if fov exists
    fov_query = \
      base.session.\
      query(Cosmx_fov.cosmx_fov_name).\
      join(Cosmx_slide, Cosmx_slide.cosmx_slide_id == Cosmx_fov.cosmx_slide_id).\
      filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id).\
      filter(Cosmx_fov.cosmx_fov_name.in_(fov_list))
    existing_fov_records = \
      base.fetch_records(
        query=fov_query,
        output_mode="dataframe")
    if not isinstance(existing_fov_records, pd.DataFrame) or \
       "cosmx_fov_name" not in existing_fov_records.columns:
      raise KeyError("Failed to get cosmx_fov_name from db")
    existing_fov_list = \
      existing_fov_records["cosmx_fov_name"].astype(int).values.tolist()
    # [
      # fov.cosmx_fov_name for fov in existing_fov_records]
    ## step4: enter new fov records
    new_items = \
      list(
        set(fov_list).\
          difference(
            set(existing_fov_list)))
    try:
      for fov_id in new_items:
        fov_entry = \
          Cosmx_fov(
            cosmx_fov_name=fov_id,
            cosmx_slide_id=cosmx_slide_id,
            slide_type=slide_type)
        base.session.add(fov_entry)
        base.session.flush()
      # for fov_id in fov_list:
      #   if fov_id not in existing_fov_list:
      #     fov_entry = \
      #       Cosmx_fov(
      #         cosmx_fov_name=fov_id,
      #         cosmx_slide_id=cosmx_slide_id[0],
      #         slide_type=slide_type)
      #     base.session.add(fov_entry)
      #     base.session.flush()
      base.session.commit()
      base.close_session()
      status = True
    except Exception as e:
      base.session.rollback()
      base.close_session()
      raise ValueError(e)
    return status
  except Exception as e:
    raise ValueError(
      f"Failed to create cosmx fov records, error: {e}")


def create_or_update_cosmx_slide_fov_annotation(
  cosmx_slide_igf_id: str,
  fov_range: str,
  db_session_class: sessionmaker,
  tissue_annotation: str = 'unknown',
  tissue_ontology: str = 'unknown',
  tissue_condition: str = 'unknown',
  species: str = 'unknown') -> bool:
  """
  """
  try:
    status = False
    ## step1: get a list from fov range
    fov_list = \
      fov_range_to_list(
        fov_range=fov_range)
    if len(fov_list) == 0:
      raise ValueError("No fov range found for slide {cosmx_slide_igf_id}")
    ## connect to database
    base = BaseAdaptor(**{"session_class": db_session_class})
    base.start_session()
    ## step2: check if slides and fovs are registered
    slide_query = \
      base.session.\
        query(Cosmx_slide.cosmx_slide_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id)
    cosmx_slide_id = \
      base.fetch_records(
        query=slide_query,
        output_mode='one_or_none')
    if cosmx_slide_id is None:
      base.close_session()
      raise ValueError(
        f"Cosmx slide {cosmx_slide_igf_id} is not in DB")
    slide_fov_query = \
      base.session.\
        query(Cosmx_fov.cosmx_fov_id).\
        join(Cosmx_slide, Cosmx_slide.cosmx_slide_id == Cosmx_fov.cosmx_slide_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id).\
        filter(Cosmx_fov.cosmx_fov_name.in_(fov_list))
    fov_records = \
      base.fetch_records(
        query=slide_fov_query,
        output_mode="dataframe")
    if not isinstance(fov_records, pd.DataFrame) or \
       "cosmx_fov_id" not in fov_records.columns:
      raise KeyError("Missing cosmx_fov_id in db records")
    fov_id_list = fov_records["cosmx_fov_id"].values.tolist()
    # [
    # fov.cosmx_fov_id for fov in fov_records]
    ## step3: check if all fovs are present
    if len(fov_id_list) == 0:
      raise ValueError(
        f"Cosmx slide {cosmx_slide_igf_id} and fov range {fov_range} is not in DB")
    if len(fov_id_list) < len(fov_list):
      base.close_session()
      raise ValueError(
        f"Not all fovs are present in db")
    ## step4: add new annotation records
    try:
      for fov_id in fov_id_list:
        fov_annotation = \
          Cosmx_fov_annotation(
            cosmx_fov_id=fov_id,
            tissue_species=species,
            tissue_annotation=tissue_annotation,
            tissue_ontology=tissue_ontology,
            tissue_condition=tissue_condition)
        base.session.add(fov_annotation)
        base.session.flush()
      base.session.commit()
      base.close_session()
      status = True
    except Exception as e:
      base.session.rollback()
      base.close_session()
      raise ValueError(f"Failed to load data in DB, error: {e}")
    return status
  except Exception as e:
    raise ValueError(
      f"Failed to create cosmx annotation records, error: {e}")


def validate_cosmx_count_file(
  count_json_file: str,
  validation_schema_json_file: str) -> List[str]:
  """
  A function for validation checking COSMX count files

  :param count_json_file: AJSON file with COSMX count data
  :param validation_schema_json_file: A JSON schema for validation checking
  :returns: A list of validation errors
  """
  try:
    error_list = []
    schema = \
      read_json_data(validation_schema_json_file)
    if isinstance(schema, list):
      schema = schema[0]
    metadata_validator = \
      Draft4Validator(schema)
    with open(count_json_file, 'r') as fp:
      json_count_data = json.load(fp)
    validation_errors = \
      sorted(
        metadata_validator.iter_errors(json_count_data),
        key=lambda e: e.path)
    for err in validation_errors:
      if isinstance(err, str):
        error_list.append(err)
      else:
        if len(err.schema_path) > 2:
          error_list.append(
            f"{err.schema_path[2]}: {err.message}")
        else:
          error_list.append(
            f"{err.message}")
    return error_list
  except Exception as e:
    raise ValueError(
      f"Validation failed for {count_json_file}, error: {e}")

class CosmxSlideType(Enum):
  """
  An enum class for CosMx slide types

  """
  RNA = 'RNA'
  PROTEIN = 'PROTEIN'


def create_cosmx_slide_fov_count_qc(
  cosmx_slide_igf_id: str,
  fov_range: str,
  slide_type: str,
  db_session_class: sessionmaker,
  slide_count_json_file: str,
  rna_count_file_validation_schema: str,
  protein_count_file_validation_schema: str,) -> bool:
  """
  """
  try:
    status = False
    slide_type = slide_type.upper()
    ## step1: check slide type
    if slide_type not in CosmxSlideType.__members__:
      raise KeyError(f"Unknown slide type {slide_type}")
    ## step2: validate count columns
    validation_errors = list()
    if slide_type == CosmxSlideType.RNA.name:
      validation_errors = \
        validate_cosmx_count_file(
          count_json_file=slide_count_json_file,
          validation_schema_json_file=rna_count_file_validation_schema)
    elif slide_type == CosmxSlideType.PROTEIN.name:
      validation_errors = \
        validate_cosmx_count_file(
          count_json_file=slide_count_json_file,
          validation_schema_json_file=protein_count_file_validation_schema)
    if len(validation_errors) > 0:
      raise ValueError(
        f"Validation failed for {cosmx_slide_igf_id} - {slide_count_json_file}, \
          errors: {', '.join(validation_errors)}")
    ## step3: get a list from fov range
    fov_list = \
      fov_range_to_list(
        fov_range=fov_range)
    if len(fov_list) == 0:
      raise ValueError("No fov range found for slid {cosmx_slide_igf_id}")
    ## step4: open count json file and check if same fov's are in the file
    count_df = \
      pd.read_json(
        slide_count_json_file)
    count_file_fov_id_list = count_df['fov_id'].tolist()
    unknown_fov = [ f_id
      for f_id in count_file_fov_id_list
        if f_id not in fov_list]
    if len(unknown_fov) > 0:
      raise ValueError(
        f"Fov ids are not in range {fov_range}: {slide_count_json_file}")
    ## connect to database
    base = BaseAdaptor(**{"session_class": db_session_class})
    base.start_session()
    ## step5: check if slide is registered
    slide_query = \
      base.session.\
        query(Cosmx_slide.cosmx_slide_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id)
    cosmx_slide_id = \
      base.fetch_records(
        query=slide_query,
        output_mode='one_or_none')
    if cosmx_slide_id is None:
      base.close_session()
      raise ValueError(
        f"Cosmx slide {cosmx_slide_igf_id} is not in DB")
    ## step6: check if fovs are registered
    slide_fov_query = \
      base.session.\
        query(Cosmx_fov.cosmx_fov_id, Cosmx_fov.cosmx_fov_name).\
        join(Cosmx_slide, Cosmx_slide.cosmx_slide_id == Cosmx_fov.cosmx_slide_id).\
        filter(Cosmx_slide.cosmx_slide_igf_id == cosmx_slide_igf_id).\
        filter(Cosmx_fov.cosmx_fov_name.in_(fov_list))
    fov_records = \
      base.fetch_records(
        query=slide_fov_query,
        output_mode="object")
    fov_id_dict = {
      int(fov.cosmx_fov_name): fov.cosmx_fov_id \
        for fov in fov_records}
    fov_id_list = list(fov_id_dict.values())
    ## step7: check if all fovs are present
    if len(fov_id_list) == 0:
      raise ValueError(
        f"Cosmx slide {cosmx_slide_igf_id} and fov range {fov_range} is not in DB")
    if len(fov_id_list) < len(fov_list):
      base.close_session()
      raise ValueError(
        f"Not all fovs are present in db")
    ## step8: check if the fov entries are present in the count qc table
    fov_count_qc_entries = list()
    if slide_type == CosmxSlideType.RNA.name:
      fov_count_qc_entries = \
        base.session.\
          query(Cosmx_fov_rna_qc.cosmx_fov_id).\
          filter(Cosmx_fov_rna_qc.cosmx_fov_id.in_(list(fov_id_dict.values()))).\
          all()
    elif slide_type == CosmxSlideType.PROTEIN.name:
      fov_count_qc_entries = \
        base.session.\
          query(Cosmx_fov_protein_qc.cosmx_fov_id).\
          filter(Cosmx_fov_protein_qc.cosmx_fov_id.in_(list(fov_id_dict.values()))).\
          all()
    ## its not safe to assume that user loaded only part of the records.
    ## so we will not allow re-loading data if any record exists
    if len(fov_count_qc_entries) > 0:
      base.close_session()
      raise ValueError(
        f"FOV count qc records already exisis for slide {cosmx_slide_igf_id}. \
          Clean up before loading again.")
    ## step9: map fov ids
    count_df['cosmx_fov_id'] = count_df['fov_id'].map(fov_id_dict)
    del count_df['fov_id']
    ## step10: load to db
    try:
      for row in count_df.to_dict(orient='records'):
        if slide_type == CosmxSlideType.RNA.name:
          fov_entry = \
            Cosmx_fov_rna_qc(**row)
          base.session.add(fov_entry)
          base.session.flush()
        elif slide_type == CosmxSlideType.PROTEIN.name:
          fov_entry = \
            Cosmx_fov_protein_qc(**row)
          base.session.add(fov_entry)
          base.session.flush()
      base.session.commit()
      base.close_session()
      status = True
    except Exception as e:
      base.session.rollback()
      base.close_session()
      raise ValueError(f"Failed to load data in DB, error: {e}")
    return status
  except Exception as e:
    raise ValueError(e)