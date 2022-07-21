import unittest,re
from igf_data.illumina.samplesheet import SampleSheet

class Hiseq4000SampleSheet(unittest.TestCase):
  def setUp(self):
    self.file = 'doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.samplesheet_data = SampleSheet(infile=self.file)

  def test_check_sample_header(self):
    samplesheet_data = self.samplesheet_data
    existsA = samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsA)

    existsB = samplesheet_data.check_sample_header(section='Header', condition_key='Application')
    self.assertTrue(existsB)

  def test_get_lane_count(self):
    samplesheet_data = self.samplesheet_data
    count = samplesheet_data.get_lane_count()
    self.assertEqual(len(count), 8)

  def test_get_project_names(self):
    samplesheet_data = self.samplesheet_data
    project_names = samplesheet_data.get_project_names()
    self.assertIn('project_1',project_names)
    self.assertEqual(len(project_names), 4)

  def test_get_platform_name(self):
    samplesheet_data = self.samplesheet_data
    platform_name = samplesheet_data.get_platform_name()
    pattern = re.compile('hiseq', re.IGNORECASE)
    self.assertRegex(platform_name, pattern)

  def test_get_project_and_lane(self):
    samplesheet_data = self.samplesheet_data
    platform_list = samplesheet_data.get_project_and_lane()
    self.assertTrue('project_3 : 8' in platform_list)

  def test_filter_sample_data(self):
    samplesheet_data = self.samplesheet_data
    samplesheet_data.filter_sample_data(condition_key='Lane', condition_value=3)
    count = samplesheet_data.get_lane_count()
    self.assertEqual(len(count), 1)

  def test_index_format(self):
    samplesheet_data = self.samplesheet_data
    indexA = [row['index'] for row in samplesheet_data._data if row['Sample_ID']=='IGF00010'][0]
    self.assertEqual( indexA, 'CGCTCATT') 

  def test_get_reverse_complement_index(self):
    samplesheet_data = self.samplesheet_data
    indexA = [row['index2'] for row in samplesheet_data._data if row['Sample_ID']=='IGF0001'][0]
    self.assertEqual( indexA, 'AGGCTATA')

    # reverse complement the index2
    samplesheet_data.get_reverse_complement_index()

    indexB = [row['index2'] for row in samplesheet_data._data if row['Sample_ID']=='IGF0001'][0]
    self.assertEqual( indexB, 'TATAGCCT')

  def test_modify_sample_header(self):
    samplesheet_data = self.samplesheet_data
    existsA = samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsA)

    # adding adapter info in the samplesheet header
    samplesheet_data.modify_sample_header(section='Settings', type='add', condition_key='Adapter', condition_value='AAAAAAAAAA') 

    existsB = samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertTrue(existsB)

    existsC = samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter2')
    self.assertFalse(existsC)

    # removing the adapter info from samplesheet header
    samplesheet_data.modify_sample_header(section='Settings', type='remove', condition_key='Adapter')

    existsD = samplesheet_data.check_sample_header(section='Settings', condition_key='Adapter')
    self.assertFalse(existsD)

class TestValidateSampleSheet(unittest.TestCase):
  def setUp(self):
    self.file = 'doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.samplesheet_data = SampleSheet(infile=self.file)

  def test_validate_sample_id(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033 44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "CAATCAAG",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_ID')

  def test_validate_sample_name(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample_{1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "CAATCAAG",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_Name')

  def test_validate_sample_project(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001 : projectABC",
      "Sample_Well": "",
      "index": "CAATCAAG",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'Sample_Project')

  def test_validate_index1(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "CAATCAAGNNN",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'index')

  def test_validate_index1_2(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)
    self.assertEqual(errors[0].path[1], 'index')

  def test_validate_index2(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "AAAAAAAA",
      "index2": ""}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index1(self):
    data = [{
      "Description": "10X",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "I5A1",
      "I7_Index_ID": "I7A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "ATAAA",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index2(self):
    data = [{
      "Description": "",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "",
      "I7_Index_ID": "SI-GA-A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "SI-GA-A1",
      "index2": ""}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

  def test_validate_singlecell_index3(self):
    data = [{
      "Description": "10X",
      "Sample_ID":"IGF1033_44",
      "I5_Index_ID": "S111",
      "I7_Index_ID": "SI-GA-A1",
      "Sample_Name": "Sample-1000A",
      "Sample_Plate": "",
      "Sample_Project": "IGFQ0001_projectABC",
      "Sample_Well": "",
      "index": "SI-GA-A1",
      "index2": "TGTTAACT"}]
    samplesheet = self.samplesheet_data
    samplesheet._data = data
    errors = samplesheet.validate_samplesheet_data(schema_json='data/validation_schema/samplesheet_validation.json')
    self.assertEqual(len(errors),1)

class TestValidateSampleSheet1(unittest.TestCase):
  def setUp(self):
    self.file = 'doc/data/SampleSheet/HiSeq4000/SampleSheet.csv'
    self.samplesheet_data = SampleSheet(infile=self.file)

  def test_group_data_by_index_length(self):
    data_group = \
      self.\
        samplesheet_data.\
          group_data_by_index_length()
    self.assertTrue(16 in [i for i in data_group])
    self.assertFalse(12 in [i for i in data_group])

class TestValidateSampleSheet2(unittest.TestCase):
  def setUp(self):
    self.file = 'doc/data/SampleSheet/MiSeq/SampleSheet.csv'
    self.samplesheet_data = SampleSheet(infile=self.file)

  def test_add_pseudo_lane_for_miseq(self):
    self.assertEqual(self.samplesheet_data._data[0].get('PseudoLane'),None)
    self.\
    samplesheet_data.\
    add_pseudo_lane_for_miseq()
    self.assertEqual(self.samplesheet_data._data[0].get('PseudoLane'),'1')

  def test_samplesheet_version(self):
    self.assertEqual(self.samplesheet_data.samplesheet_version, 'v1')

  def test_set_header_for_bclconvert_run(self):
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('Settings' in header_data)
    self.samplesheet_data.\
      set_header_for_bclconvert_run(bases_mask='Y101;I6N2;U28')
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('Settings' in header_data)
    self.assertTrue('OverrideCycles,Y101;I6N2;U28' in header_data['Settings'])
    del self.samplesheet_data._header_data['Settings']
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('Settings' not in header_data)
    self.samplesheet_data.\
      set_header_for_bclconvert_run(bases_mask='Y101;I6N2;U28')
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('Settings' in header_data)
    self.assertTrue('OverrideCycles,Y101;I6N2;U28' in header_data['Settings'])

class SampleSheet_format_v2_test1(unittest.TestCase):
  def setUp(self):
    self.file = 'data/samplesheet_v2/test1_SampleSheet.csv'
    self.samplesheet_data = SampleSheet(infile=self.file)

  def test_samplesheet_version(self):
    self.assertEqual(self.samplesheet_data.samplesheet_version, 'v2')

  def test_group_data_by_index_length(self):
    data_groups = \
      self.samplesheet_data.group_data_by_index_length()
    self.assertEqual(len(data_groups), 1)
    self.assertTrue(data_groups.get(8))
    self.assertEqual(len(data_groups.get(8)._data), 16)

  def test_get_project_names(self):
    projects = self.samplesheet_data.get_project_names()
    self.assertTrue(len(projects), 1)
    self.assertTrue('Project1' in projects)

  def test_get_project_and_lane(self):
    projects = self.samplesheet_data.get_project_and_lane()
    self.assertTrue('Project1' in projects)

  def test_get_index_count(self):
    indices = \
      self.samplesheet_data.get_index_count()
    self.assertTrue(indices.get('index'))

  def test_get_indexes(self):
    indices = \
      self.samplesheet_data.get_indexes()
    self.assertTrue('TCTTAAAG' in indices)

  def test_add_pseudo_lane_for_nextseq(self):
    self.samplesheet_data.add_pseudo_lane_for_nextseq(lanes=('1',))
    projects = self.samplesheet_data.get_project_and_lane(lane_tag='PseudoLane')
    self.assertEqual(len(projects), 1)
    self.assertEqual(projects[0].split(':')[0].strip(), 'Project1')
    self.assertEqual(projects[0].split(':')[1].strip(), '1')

  def test_get_reverse_complement_index(self):
    self.samplesheet_data.\
      get_reverse_complement_index(index_field='index')
    indices = \
      self.samplesheet_data.get_indexes()
    self.assertTrue('CTTTAAGA' in indices)

  def test_get_platform_name(self):
    pl_name = \
      self.samplesheet_data.\
        get_platform_name()
    self.assertEqual(pl_name, 'NextSeq2000')

  def test_get_lane_count(self):
    lanes = \
      self.samplesheet_data.\
        get_lane_count()
    self.assertEqual(len(lanes), 1)
    self.assertTrue(1 in lanes)

  def test_check_sample_header(self):
    cycle_data = \
      self.samplesheet_data.\
        check_sample_header(
          section='BCLConvert_Settings',
          condition_key='OverrideCycles',
          return_values=True)
    self.assertEqual(len(cycle_data), 1)
    self.assertTrue('U28;I8;Y91' in cycle_data[0].split(','))

  def test_modify_sample_header(self):
    cycle_data = \
      self.samplesheet_data.\
        check_sample_header(
          section='BCLConvert_Settings',
          condition_key='OverrideCycles',
          return_values=True)
    self.assertEqual(len(cycle_data), 1)
    self.assertTrue('U28;I8;Y91' in cycle_data[0].split(','))
    self.samplesheet_data.\
      modify_sample_header(
        section='BCLConvert_Settings',
        type="remove",
        condition_key='OverrideCycles')
    self.samplesheet_data.\
      modify_sample_header(
        section='BCLConvert_Settings',
        type="add",
        condition_key='OverrideCycles',
        condition_value='U28;I6N2;Y91')
    cycle_data = \
      self.samplesheet_data.\
        check_sample_header(
          section='BCLConvert_Settings',
          condition_key='OverrideCycles',
          return_values=True)
    self.assertEqual(len(cycle_data), 1)
    self.assertTrue('U28;I6N2;Y91' in cycle_data[0].split(','))

  def test_validate_samplesheet(self):
    errors = \
      self.samplesheet_data.\
        validate_samplesheet_data(
          schema_json='data/validation_schema/samplesheet_validation.json')

  def test_set_header_for_bclconvert_run(self):
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('BCLConvert_Settings' in header_data)
    self.assertTrue('OverrideCycles,U28;I8;Y91' in header_data['BCLConvert_Settings'])
    self.samplesheet_data.\
      set_header_for_bclconvert_run(bases_mask='Y101;I6N2;U28')
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('BCLConvert_Settings' in header_data)
    self.assertTrue('OverrideCycles,Y101;I6N2;U28' in header_data['BCLConvert_Settings'])
    del self.samplesheet_data._header_data['BCLConvert_Settings']
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('BCLConvert_Settings' not in header_data)
    self.samplesheet_data.\
      set_header_for_bclconvert_run(bases_mask='Y101;I6N2;U28')
    header_data = \
      self.samplesheet_data._header_data
    self.assertTrue('BCLConvert_Settings' in header_data)
    self.assertTrue('OverrideCycles,Y101;I6N2;U28' in header_data['BCLConvert_Settings'])

if __name__ == '__main__':
  unittest.main()
