import unittest,re
from igf_data.illumina.samplesheet import SampleSheet
from pandas.io.stata import _data_method_doc

class Hiseq4000SampleSheet(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.file=file
    self.samplesheet_data=SampleSheet(infile=self.file)

  def test_check_sample_header(self):
    samplesheet_data=self.samplesheet_data
    existsA=samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsA)

    existsB=samplesheet_data.check_sample_header(section='Header', condition_key='Application')
    self.assertTrue(existsB)
   
  def test_get_lane_count(self):
    samplesheet_data=self.samplesheet_data
    count=samplesheet_data.get_lane_count()
    self.assertEqual(len(count), 8)

  def test_get_project_names(self):
    samplesheet_data=self.samplesheet_data
    project_names=samplesheet_data.get_project_names()
    self.assertIn('project_1',project_names)
    self.assertEqual(len(project_names), 4)

  def test_get_platform_name(self):
    samplesheet_data=self.samplesheet_data
    platform_name=samplesheet_data.get_platform_name()
    pattern=re.compile('hiseq', re.IGNORECASE)
    self.assertRegexpMatches(platform_name, pattern)

  def test_get_project_and_lane(self):
    samplesheet_data=self.samplesheet_data
    platform_list=samplesheet_data.get_project_and_lane()
    self.assertTrue('project_3:8' in platform_list)

  def test_filter_sample_data(self):
    samplesheet_data=self.samplesheet_data
    samplesheet_data.filter_sample_data(condition_key='Lane', condition_value=3)
    count=samplesheet_data.get_lane_count()
    self.assertEqual(len(count), 1)

  def test_index_format(self):
    samplesheet_data=self.samplesheet_data
    indexA=[row['index'] for row in samplesheet_data._data if row['Sample_ID']=='IGF00010'][0]
    self.assertEqual( indexA, 'CGCTCATT') 

  def test_get_reverse_complement_index(self):
    samplesheet_data=self.samplesheet_data
    indexA=[row['index2'] for row in samplesheet_data._data if row['Sample_ID']=='IGF0001'][0]
    self.assertEqual( indexA, 'AGGCTATA')

    # reverse complement the index2
    samplesheet_data.get_reverse_complement_index()

    indexB=[row['index2'] for row in samplesheet_data._data if row['Sample_ID']=='IGF0001'][0]
    self.assertEqual( indexB, 'TATAGCCT')

  def test_modify_sample_header(self):
    samplesheet_data=self.samplesheet_data
    existsA=samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsA)
 
    # adding adapter info in the samplesheet header
    samplesheet_data.modify_sample_header(section='Settings', type='add', condition_key='Adapter', condition_value='AAAAAAAAAA') 

    existsB=samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertTrue(existsB)

    existsC=samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter2')
    self.assertFalse(existsC)

    # removing the adapter info from samplesheet header
    samplesheet_data.modify_sample_header(section='Settings', type='remove', condition_key='Adapter')

    existsD=samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsD)

class TestValidateSampleSheet(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.file=file
    self.samplesheet_data=SampleSheet(infile=self.file)
  
  def test_validate_sample_id(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033 44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "CAATCAAG",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_ID')

  def test_validate_sample_name(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample_{1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "CAATCAAG",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_Name')

  def test_validate_sample_project(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001 : projectABC",
       "Sample_Well": "",
       "index": "CAATCAAG",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_Project')

  def test_validate_index1(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "CAATCAAGNNN",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'index')

  def test_validate_index1_2(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'index')

  def test_validate_index2(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "AAAAAAAA",
       "index2": ""}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index1(self):
    data=[{"Description": "10X",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "I5A1",
       "I7_Index_ID": "I7A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "ATAAA",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index2(self):
    data=[{"Description": "",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "",
       "I7_Index_ID": "SI-GA-A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "SI-GA-A1",
       "index2": ""}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index3(self):
    data=[{"Description": "10X",
       "Sample_ID":"IGF1033_44",
       "I5_Index_ID": "S111",
       "I7_Index_ID": "SI-GA-A1",
       "Sample_Name": "Sample-1000A",
       "Sample_Plate": "",
       "Sample_Project": "IGFQ0001_projectABC",
       "Sample_Well": "",
       "index": "SI-GA-A1",
       "index2": "TGTTAACT"}]
    samplesheet=self.samplesheet_data
    samplesheet._data=data
    errors=samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

class TestValidateSampleSheet1(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.file=file
    self.samplesheet_data=SampleSheet(infile=self.file)

  def test_group_data_by_index_length(self):
    data_group=self.\
               samplesheet_data.\
               group_data_by_index_length()
    self.assertTrue(16 in [i for i in data_group])
    self.assertFalse(12 in [i for i in data_group])

class TestValidateSampleSheet2(unittest.TestCase):
  def setUp(self):
    file='doc/data/SampleSheet/MiSeq/SampleSheet.csv'
    self.file=file
    self.samplesheet_data=SampleSheet(infile=self.file)

  def test_add_pseudo_lane_for_miseq(self):
    self.assertEqual(self.samplesheet_data._data[0].get('PseudoLane'),None)
    self.\
    samplesheet_data.\
    add_pseudo_lane_for_miseq()
    self.assertEqual(self.samplesheet_data._data[0].get('PseudoLane'),'1')

if __name__ == '__main__':
  unittest.main()
