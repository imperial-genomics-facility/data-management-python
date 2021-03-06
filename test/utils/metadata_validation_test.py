import os, unittest
import pandas as pd
from igf_data.utils.validation_check.metadata_validation import Validate_project_and_samplesheet_metadata

class Validate_project_and_samplesheet_metadata_test1(unittest.TestCase):
  def setUp(self):
    self.metadata_file='data/metadata_validation/metadata_file.csv'
    self.samplesheet_file='data/metadata_validation/SampleSheet.csv'
    self.samplesheet_schema='data/validation_schema/samplesheet_validation.json'
    self.metadata_schema='data/validation_schema/metadata_validation.json'

  def test_get_samplesheet_validation_report(self):
    va = \
      Validate_project_and_samplesheet_metadata(
        samplesheet_file=self.samplesheet_file,
        metadata_files=self.metadata_file,
        samplesheet_schema=self.samplesheet_schema,
        metadata_schema=self.metadata_schema)
    errors = va.get_samplesheet_validation_report()
    err_lines = [
      val
        for err_line in errors
          for key,val in err_line.items()
            if key=='line']
    self.assertTrue(22 in err_lines)
    for err_line in errors:
      error = err_line.get('error')
      column_name = err_line.get('column')
      if 'line' in err_line and \
         err_line.get('line')==22:
        if column_name=='index':
          self.assertTrue(' TAAGGCGA' in error)
        elif column_name=='Sample_Name':
          self.assertTrue('KDSC_76' in error)

  def test_get_metadata_validation_report(self):
    va = \
      Validate_project_and_samplesheet_metadata(
        samplesheet_file=self.samplesheet_file,
        metadata_files=[self.metadata_file],
        samplesheet_schema=self.samplesheet_schema,
        metadata_schema=self.metadata_schema)
    errors = va.get_metadata_validation_report()
    for err_line in errors:
      error = err_line.get('error')
      column_name = err_line.get('column')
      if column_name=='sample_submitter_id' and \
         error.startswith('\'KDSC_77'):
        self.assertTrue('KDSC_77' in error)

class Validate_project_and_samplesheet_metadata_test2(unittest.TestCase):
  def setUp(self):
    self.metadata_file = 'data/metadata_validation/metadata_file2.csv'
    self.samplesheet_file = 'data/metadata_validation/SampleSheet2.csv'
    self.samplesheet_schema = 'data/validation_schema/samplesheet_validation.json'
    self.metadata_schema = 'data/validation_schema/metadata_validation.json'

  def test_get_samplesheet_validation_report2(self):
    va = \
      Validate_project_and_samplesheet_metadata(
        samplesheet_file=self.samplesheet_file,
        metadata_files=self.metadata_file,
        samplesheet_schema=self.samplesheet_schema,
        metadata_schema=self.metadata_schema)
    errors = va.get_samplesheet_validation_report()
    err_lines = [
      val
        for err_line in errors
          for key,val in err_line.items()
            if key=='line']
    self.assertTrue(22 in err_lines)
    for err_line in errors:
      error = err_line.get('error')
      column_name = err_line.get('column')
      if 'line' in err_line and \
         err_line.get('line')==22:
        if column_name=='index':
          self.assertTrue(' TAAGGCGA' in error)
        elif column_name=='Sample_Name':
          self.assertTrue('KDSC_76' in error)

  def test_get_metadata_validation_report2(self):
    va = \
      Validate_project_and_samplesheet_metadata(
        samplesheet_file=self.samplesheet_file,
        metadata_files=[self.metadata_file],
        samplesheet_schema=self.samplesheet_schema,
        metadata_schema=self.metadata_schema)
    errors = va.get_metadata_validation_report()
    for err_line in errors:
      error = err_line.get('error')
      column_name = err_line.get('column')
      if column_name=='sample_submitter_id' and \
         error.startswith('\'KDSC_77'):
        self.assertTrue('KDSC_77' in error)

  def test_check_metadata_library_by_row(self):
    data = \
      pd.Series(
        dict(
          sample_igf_id='SampleA',
          library_source='GENOMIC',
          library_strategy='WGS',
          experiment_type='WGS'))
    err = \
      Validate_project_and_samplesheet_metadata.\
        check_metadata_library_by_row(data)
    self.assertIsNone(err)
    data = \
      pd.Series(
        dict(
          library_source='GENOMIC',
          library_strategy='WGS',
          experiment_type='WGS'))
    err = \
      Validate_project_and_samplesheet_metadata.\
        check_metadata_library_by_row(data)
    self.assertEqual(err,'Sample igf id not found')
    data = \
      pd.Series(
        dict(
          sample_igf_id='SampleA',
          library_source='GENOMIC',
          library_strategy='WGSA',
          experiment_type='WGS'))
    err = \
      Validate_project_and_samplesheet_metadata.\
        check_metadata_library_by_row(data)
    self.assertTrue(err.startswith('SampleA'))
    data = \
      pd.Series(
        dict(
          sample_igf_id='SampleA',
          library_source='GENOMIC',
          library_strategy='WGS',
          experiment_type='WGSA'))
    err = \
      Validate_project_and_samplesheet_metadata.\
        check_metadata_library_by_row(data)
    self.assertTrue(err.startswith('SampleA'))
if __name__=='__main__':
  unittest.main()