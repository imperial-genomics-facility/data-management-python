import os, unittest
from igf_data.utils.validation_check.metadata_validation import Validate_project_and_samplesheet_metadata

class Validate_project_and_samplesheet_metadata_test1(unittest.TestCase):
  def setUp(self):
    self.metadata_file='data/metadata_validation/metadata_file.csv'
    self.samplesheet_file='data/metadata_validation/SampleSheet.csv'
    self.samplesheet_schema='data/validation_schema/samplesheet_validation.json'
    self.metadata_schema='data/validation_schema/metadata_validation.json'

  def test_get_samplesheet_validation_report(self):
    va=Validate_project_and_samplesheet_metadata(\
          samplesheet_file=self.samplesheet_file,
          metadata_files=self.metadata_file,
          samplesheet_schema=self.samplesheet_schema,
          metadata_schema=self.metadata_schema
        )
    errors=va.get_samplesheet_validation_report()
    err_lines=[val
               for err_line in errors
                 for key,val in err_line.items()
                   if key=='line']
    self.assertTrue(22 in err_lines)
    for err_line in errors:
      error=err_line.get('error')
      column_name=err_line.get('column')
      if 'line' in err_line and \
         err_line.get('line')==22:
        if column_name=='index':
          self.assertTrue(' TAAGGCGA' in error)
        elif column_name=='Sample_Name':
          self.assertTrue('KDSC_76' in error)

  def test_get_metadata_validation_report(self):
    va=Validate_project_and_samplesheet_metadata(\
          samplesheet_file=self.samplesheet_file,
          metadata_files=[self.metadata_file],
          samplesheet_schema=self.samplesheet_schema,
          metadata_schema=self.metadata_schema
        )
    errors=va.get_metadata_validation_report()
    for err_line in errors:
      error=err_line.get('error')
      column_name=err_line.get('column')
      if column_name=='sample_submitter_id' and \
         error.startswith('\'KDSC_77'):
        self.assertTrue('KDSC_77' in error)

if __name__=='__main__':
  unittest.main()