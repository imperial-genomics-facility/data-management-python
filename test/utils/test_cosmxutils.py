import pandas as pd
from decimal import Decimal
import unittest, os, json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.utils.dbutils import read_dbconf_json
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
from igf_data.utils.fileutils import (
  get_temp_dir,
  remove_dir)
from datetime import datetime
from igf_data.utils.cosmxutils import (
  check_and_register_cosmx_run,
  check_and_register_cosmx_slide,
  create_or_update_cosmx_slide_fov,
  create_or_update_cosmx_slide_fov_annotation,
  create_cosmx_slide_fov_count_qc,
  validate_cosmx_count_file
)

class Analysisadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    self.base = BaseAdaptor(**dbparam)
    self.engine = self.base.engine
    self.dbname = dbparam['dbname']
    self.temp_dir = get_temp_dir()
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    remove_dir(self.temp_dir)


  def test_check_and_register_cosmx_run(self):
    project_data = [{'project_igf_id':'project1'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    project = Project(project_igf_id='project1')
    pa.session.add(project)
    pa.session.flush()
    pa.session.commit()
    pa.close_session()
    status = \
      check_and_register_cosmx_run(
        project_igf_id='project1',
        cosmx_run_igf_id='cosmx_run_1',
        db_session_class=self.base.get_session_class())
    self.assertTrue(status)


  def test_check_and_register_cosmx_slide(self):
    project_data = [{'project_igf_id':'project1'}]
    pa = ProjectAdaptor(**{'session_class':self.base.get_session_class()})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    cosmx_platform = \
      Cosmx_platform(
        cosmx_platform_igf_id='cosmx_platform_1')
    pa.session.add(cosmx_platform)
    pa.session.flush()
    pa.session.commit()
    cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id='cosmx_run_1',
          project_id=1)
    pa.session.add(cosmx_run)
    pa.session.flush()
    pa.session.commit()
    pa.close_session()
    status = \
      check_and_register_cosmx_slide(
        cosmx_run_igf_id='cosmx_run_1',
        cosmx_slide_igf_id='cosmx_slide_1',
        cosmx_slide_name='cosmx_slide_1',
        slide_run_date=datetime.now(),
        panel_info='cosmx_panel_1',
        assay_type='assay1',
        version='1.0',
        cosmx_platform_igf_id='cosmx_platform_1',
        db_session_class=self.base.get_session_class(),
        slide_metadata={"a": "b"})
    self.assertTrue(status)


  def test_create_or_update_cosmx_slide_fov(self):
    base = BaseAdaptor(**{'session_class':self.base.get_session_class()})
    base.start_session()
    project = \
      Project(project_igf_id="project1")
    base.session.add(project)
    base.session.flush()
    base.session.commit()
    cosmx_platform = \
      Cosmx_platform(
        cosmx_platform_igf_id='cosmx_platform_1')
    base.session.add(cosmx_platform)
    base.session.flush()
    base.session.commit()
    cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id='cosmx_run_1',
          project_id=project.project_id)
    base.session.add(cosmx_run)
    base.session.flush()
    base.session.commit()
    cosmx_slide = \
      Cosmx_slide(
        cosmx_slide_igf_id='cosmx_slide_1',
        cosmx_slide_name='cosmx_slide_1',
        panel_info='cosmx_panel_1',
        assay_type='assay1',
        version='1.0',
        slide_metadata={"a": "b"},
        cosmx_run_id=cosmx_run.cosmx_run_id,
        cosmx_platform_id=cosmx_platform.cosmx_platform_id)
    base.session.add(cosmx_slide)
    base.session.flush()
    base.session.commit()
    base.close_session()
    status = \
      create_or_update_cosmx_slide_fov(
        cosmx_slide_igf_id='cosmx_slide_1',
        fov_range='1-100',
        slide_type='RNA',
        db_session_class=self.base.get_session_class())
    self.assertTrue(status)


  def test_create_or_update_cosmx_slide_fov_annotation(self):
    base = BaseAdaptor(**{'session_class':self.base.get_session_class()})
    base.start_session()
    project = \
      Project(project_igf_id="project1")
    base.session.add(project)
    base.session.flush()
    base.session.commit()
    cosmx_platform = \
      Cosmx_platform(
        cosmx_platform_igf_id='cosmx_platform_1')
    base.session.add(cosmx_platform)
    base.session.flush()
    base.session.commit()
    cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id='cosmx_run_1',
          project_id=project.project_id)
    base.session.add(cosmx_run)
    base.session.flush()
    base.session.commit()
    cosmx_slide = \
      Cosmx_slide(
        cosmx_slide_igf_id='cosmx_slide_1',
        cosmx_slide_name='cosmx_slide_1',
        panel_info='cosmx_panel_1',
        assay_type='assay1',
        version='1.0',
        slide_metadata=[{"a": "b"}],
        cosmx_run_id=cosmx_run.cosmx_run_id,
        cosmx_platform_id=cosmx_platform.cosmx_platform_id)
    base.session.add(cosmx_slide)
    base.session.flush()
    base.session.commit()
    for i in range(1,11):
      fov_entry = \
        Cosmx_fov(
          cosmx_slide_id=cosmx_slide.cosmx_slide_id,
          cosmx_fov_name=i,
          slide_type='RNA')
      base.session.add(fov_entry)
      base.session.flush()
    base.session.commit()
    fov_records = base.session.query(Cosmx_fov).all()
    self.assertEqual(len(fov_records), 10)
    base.close_session()
    status = \
      create_or_update_cosmx_slide_fov_annotation(
        cosmx_slide_igf_id='cosmx_slide_1',
        fov_range='1-10',
        tissue_annotation='annotation',
        tissue_ontology='ontology',
        tissue_condition='tumor',
        species='HUMAN',
        db_session_class=self.base.get_session_class())
    self.assertTrue(status)


  def test_validate_cosmx_count_file(self):
    protein_count_dict = [{
      "fov_id": 1,
      "mean_fluorescence_intensity": "28941",
      "mean_unique_genes_per_cell": "67",
      "number_non_empty_cells": 2163,
      "pct_non_empty_cells": "1.00",
      "percentile_10_transcript_per_cell": "18412.52",
      "percentile_90_transcript_per_cell": "42659.33",
      "fluorescence_intensity_mean_igg_control_intensity": "95.681"}, {
      "fov_id": 2,
      "mean_fluorescence_intensity": "28941",
      "mean_unique_genes_per_cell": "67",
      "number_non_empty_cells": 2163,
      "pct_non_empty_cells": "1.00",
      "percentile_10_transcript_per_cell": "18412.52",
      "percentile_90_transcript_per_cell": "42659.33",
      "fluorescence_intensity_mean_igg_control_intensity": "95.681"}]
    df = pd.DataFrame(protein_count_dict)
    df = \
      df.astype({
      "fov_id": int,
      "mean_fluorescence_intensity": int,
      "mean_unique_genes_per_cell": int,
      "number_non_empty_cells": int,
      "pct_non_empty_cells": float,
      "percentile_10_transcript_per_cell": float,
      "percentile_90_transcript_per_cell": float,
      "fluorescence_intensity_mean_igg_control_intensity": float})
    protein_count_dict = df.to_dict(orient='records')
    protein_count_file = \
      os.path.join(self.temp_dir, 'protein_count.json')
    with open(protein_count_file, 'w') as fp:
      json.dump(protein_count_dict, fp)
    errors = \
      validate_cosmx_count_file(
        count_json_file=protein_count_file,
        validation_schema_json_file='data/validation_schema/cosmx_protein_count_file_validation_schema.json')
    print(errors)
    self.assertEqual(len(errors), 0)


  def test_create_or_update_cosmx_slide_fov_count_qc(self):
    rna_count_dict = [{
      "fov_id": 1,
      "mean_transcript_per_cell": 102.25,
      "mean_unique_genes_per_cell": 76.85,
      "non_empty_cells": 1084,
      "pct_non_empty_cells": 1.00,
      "percentile_90_transcript_per_cell": 30.0,
      "percentile_10_transcript_per_cell": 193.0,
      "mean_negprobe_counts_per_cell": 0.293}, {
      "fov_id": 2,
      "mean_transcript_per_cell": 152.84,
      "mean_unique_genes_per_cell": 108.64,
      "non_empty_cells": 1715,
      "pct_non_empty_cells": 1.00,
      "percentile_10_transcript_per_cell": 289.0,
      "percentile_90_transcript_per_cell": 48.4,
      "mean_negprobe_counts_per_cell": 0.335}, {
      "fov_id": 3,
      "mean_transcript_per_cell": 91.13,
      "mean_unique_genes_per_cell": 57.07,
      "non_empty_cells": 2144,
      "pct_non_empty_cells": 1.00,
      "percentile_10_transcript_per_cell": 203.0,
      "percentile_90_transcript_per_cell": 18.0,
      "mean_negprobe_counts_per_cell": 0.193}, {
      "fov_id": 4,
      "mean_transcript_per_cell": 76.45,
      "mean_unique_genes_per_cell": 46.86,
      "non_empty_cells": 2512,
      "pct_non_empty_cells": 1.00,
      "percentile_10_transcript_per_cell": 151.0,
      "percentile_90_transcript_per_cell": 22.0,
      "mean_negprobe_counts_per_cell": 0.185}]
    rna_count_file = \
      os.path.join(self.temp_dir, 'rna_count.json')
    with open(rna_count_file, 'w') as fp:
      json.dump(rna_count_dict, fp)
    base = BaseAdaptor(**{'session_class':self.base.get_session_class()})
    base.start_session()
    project = \
      Project(project_igf_id="project1")
    base.session.add(project)
    base.session.flush()
    base.session.commit()
    cosmx_platform = \
      Cosmx_platform(
        cosmx_platform_igf_id='cosmx_platform_1')
    base.session.add(cosmx_platform)
    base.session.flush()
    base.session.commit()
    cosmx_run = \
        Cosmx_run(
          cosmx_run_igf_id='cosmx_run_1',
          project_id=project.project_id)
    base.session.add(cosmx_run)
    base.session.flush()
    base.session.commit()
    cosmx_slide = \
      Cosmx_slide(
        cosmx_slide_igf_id='cosmx_slide_1',
        cosmx_slide_name='cosmx_slide_1',
        panel_info='cosmx_panel_1',
        assay_type='assay1',
        version='1.0',
        slide_metadata={"a": "b"},
        cosmx_run_id=cosmx_run.cosmx_run_id,
        cosmx_platform_id=cosmx_platform.cosmx_platform_id)
    base.session.add(cosmx_slide)
    base.session.flush()
    base.session.commit()
    for i in range(1,5):
      fov_entry = \
        Cosmx_fov(
          cosmx_slide_id=cosmx_slide.cosmx_slide_id,
          cosmx_fov_name=i,
          slide_type='RNA')
      base.session.add(fov_entry)
      base.session.flush()
    base.session.commit()
    fov_records = base.session.query(Cosmx_fov).all()
    self.assertEqual(len(fov_records), 4)
    base.close_session()
    status = \
      create_cosmx_slide_fov_count_qc(
        cosmx_slide_igf_id='cosmx_slide_1',
        fov_range='1-4',
        slide_type='RNA',
        db_session_class=self.base.get_session_class(),
        slide_count_json_file=rna_count_file,
        rna_count_file_validation_schema='data/validation_schema/cosmx_rna_count_file_validation_schema.json',
        protein_count_file_validation_schema='data/validation_schema/cosmx_protein_count_file_validation_schema.json')
    self.assertTrue(status)

if __name__ == '__main__':
  unittest.main()