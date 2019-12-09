import unittest,os
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.process.metadata_reformat.reformat_metadata_file import Reformat_metadata_file

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
    # test 1
    library_preparation_val = 'Whole Genome Sequencing Human - Sample'
    sample_description_val = 'NA'
    library_type_val = 'NA'
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv')
    library_source,library_strategy,exp_type,biomaterial_type = \
      re_metadata.get_assay_info(library_preparation_val,sample_description_val,library_type_val)
    self.assertEqual(library_source,'GENOMIC')
    self.assertEqual(library_strategy,'WGS')
    self.assertEqual(exp_type,'WGS')
    self.assertEqual(biomaterial_type,'UNKNOWN')
    ## test 2
    library_preparation_val = "Single Cell -3' RNAseq- Sample"
    sample_description_val = 'NA'
    library_type_val = 'NA'
    library_source,library_strategy,exp_type,biomaterial_type = \
      re_metadata.get_assay_info(library_preparation_val,sample_description_val,library_type_val)
    self.assertEqual(library_source,'TRANSCRIPTOMIC_SINGLE_CELL')
    self.assertEqual(library_strategy,'RNA-SEQ')
    self.assertEqual(exp_type,'TENX-TRANSCRIPTOME-3P')
    self.assertEqual(biomaterial_type,'UNKNOWN')
    ## test 3
    library_preparation_val = "Single Cell -3' RNAseq- Sample Nuclei"
    sample_description_val = 'NA'
    library_type_val = 'NA'
    library_source,library_strategy,exp_type,biomaterial_type = \
      re_metadata.get_assay_info(library_preparation_val,sample_description_val,library_type_val)
    self.assertEqual(library_source,'TRANSCRIPTOMIC_SINGLE_CELL')
    self.assertEqual(library_strategy,'RNA-SEQ')
    self.assertEqual(exp_type,'TENX-TRANSCRIPTOME-3P')
    self.assertEqual(biomaterial_type,'SINGLE_NUCLEI')
    ## test 4
    library_preparation_val = "Not Applicable"
    sample_description_val = "Pre Made Library"
    library_type_val = "SINGLE CELL-3’ RNA (NUCLEI)"
    library_source,library_strategy,exp_type,biomaterial_type = \
      re_metadata.get_assay_info(library_preparation_val,sample_description_val,library_type_val)
    self.assertEqual(library_source,'TRANSCRIPTOMIC_SINGLE_CELL')
    self.assertEqual(library_strategy,'RNA-SEQ')
    self.assertEqual(exp_type,'TENX-TRANSCRIPTOME-3P')
    self.assertEqual(biomaterial_type,'SINGLE_NUCLEI')

  def test_calculate_insert_length_from_fragment(self):
    self.assertEqual(Reformat_metadata_file.calculate_insert_length_from_fragment(fragment_length=400),280)

  def test_get_species_info(self):
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv')
    taxon_id, scientific_name, species_name = \
      re_metadata.get_species_info(species_text_val='human')
    self.assertEqual(taxon_id,'9606')
    self.assertEqual(scientific_name,'Homo sapiens')
    self.assertEqual(species_name,'HG38')

  def test_populate_metadata_values(self):
    data = pd.Series(\
      {'project_igf_id':'IGFQ1 scRNA-seq5primeFB',
       'sample_igf_id':'IGF3[',
       'library_preparation':'RNA Sequencing - Total RNA',
       'sample_description':'NA',
       'library_type':'NA',
       'species_text':'mouse'})
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv')
    data = re_metadata.populate_metadata_values(row=data)
    self.assertEqual(data.project_igf_id,'IGFQ1-scRNA-seq5primeFB')
    self.assertTrue('library_source' in data.keys())
    self.assertEqual(data.library_source,'TRANSCRIPTOMIC')

  def test_reformat_raw_metadata_file(self):
    output_file = os.path.join(self.tmp_dir,'samplesheet.csv')
    re_metadata = \
      Reformat_metadata_file(\
        infile='data/metadata_validation/metadata_reformatting/incorrect_metadata.csv')
    re_metadata.\
      reformat_raw_metadata_file(output_file=output_file)
    data = \
      pd.read_csv(output_file)
    self.assertTrue('library_source' in data.columns)
    sample_igf1_library_strategy = data[data['sample_igf_id']=='IGF1']['library_strategy'].values[0]
    self.assertEqual(sample_igf1_library_strategy,'TARGETED-CAPTURE')
    sample_igf2_experiment_type = data[data['sample_igf_id']=='IGF2']['experiment_type'].values[0]
    self.assertEqual(sample_igf2_experiment_type,'TENX-TRANSCRIPTOME-3P')
    sample_igf2_biomaterial_type = data[data['sample_igf_id']=='IGF2']['biomaterial_type'].values[0]
    self.assertEqual(sample_igf2_biomaterial_type,'SINGLE_NUCLEI')

if __name__ == '__main__':
  unittest.main()