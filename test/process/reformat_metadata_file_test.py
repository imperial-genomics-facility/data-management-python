import unittest,os
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.metadata_reformat.reformat_metadata_file import Reformat_metadata_file,EXPERIMENT_TYPE_LOOKUP,SPECIES_LOOKUP,METADATA_COLUMNS

class Reformat_metadata_file_testA(unittest.TestCase):
  def setUp(self):
    self.tmp_dir = get_temp_dir()

  def tearDown(self):
    remove_dir(self.tmp_dir)

  def test_sample_name_reformat(self):
    sample_name = 'IGF*0(1_1)'
    self.assertEqual(Reformat_metadata_file.sample_name_reformat(sample_name),'IGF-0-1-1')

  def test_sample_and_project_reformat(self):
    sample_id = 'IGF*0(1_1)'
    self.assertEqual(Reformat_metadata_file.sample_and_project_reformat(tag_name=sample_id),'IGF-0-1_1')
    project_name = 'IGF scRNA '
    self.assertEqual(Reformat_metadata_file.sample_and_project_reformat(tag_name=project_name),'IGF-scRNA')

  def test_get_assay_info(self):
    exp_type = 'TENX-TRANSCRIPTOME'
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv',
        experiment_type_lookup=EXPERIMENT_TYPE_LOOKUP,
        species_lookup=SPECIES_LOOKUP,
        metadata_columns=METADATA_COLUMNS)
    library_source,library_strategy = \
      re_metadata.get_assay_info(experiment_type=exp_type)
    self.assertEqual(library_source,'TRANSCRIPTOMIC_SINGLE_CELL')
    self.assertEqual(library_strategy,'RNA-SEQ')

  def test_calculate_insert_length_from_fragment(self):
    self.assertEqual(Reformat_metadata_file.calculate_insert_length_from_fragment(fragment_length=400),280)

  def test_get_species_info(self):
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv',
        experiment_type_lookup=EXPERIMENT_TYPE_LOOKUP,
        species_lookup=SPECIES_LOOKUP,
        metadata_columns=METADATA_COLUMNS)
    taxon_id, scientific_name = \
      re_metadata.get_species_info(genome_build='HG38')
    self.assertEqual(taxon_id,'9606')
    self.assertEqual(scientific_name,'Homo sapiens')

  def test_populate_metadata_values(self):
    data = pd.Series(\
      {'project_igf_id':'IGFQ1 scRNA-seq5primeFB',
       'sample_igf_id':'IGF3[',
       'experiment_type':'TOTAL-RNA',
       'species_name':'MM10'})
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv',
        experiment_type_lookup=EXPERIMENT_TYPE_LOOKUP,
        species_lookup=SPECIES_LOOKUP,
        metadata_columns=METADATA_COLUMNS)
    data = re_metadata.populate_metadata_values(row=data)
    self.assertEqual(data.project_igf_id,'IGFQ1-scRNA-seq5primeFB')
    self.assertTrue('library_source' in data.keys())
    self.assertEqual(data.library_source,'TRANSCRIPTOMIC')

  def test_reformat_raw_metadata_file(self):
    output_file = os.path.join(self.tmp_dir,'samplesheet.csv')
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv',
        experiment_type_lookup=EXPERIMENT_TYPE_LOOKUP,
        species_lookup=SPECIES_LOOKUP,
        metadata_columns=METADATA_COLUMNS)
    re_metadata.\
      reformat_raw_metadata_file(output_file=output_file)
    data = \
      pd.read_csv(output_file)
    self.assertTrue('library_source' in data.columns)
    sample_igf1_library_strategy = data[data['sample_igf_id']=='IGF1']['library_strategy'].values[0]
    self.assertEqual(sample_igf1_library_strategy,'RNA-SEQ')

if __name__ == '__main__':
  unittest.main()