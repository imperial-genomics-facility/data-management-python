import unittest, re
import pandas as pd
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_airflow.utils.dag17_create_transcriptome_ref_utils import filter_gtf_file_for_cellranger

class Dag17_create_transcriptome_ref_utils_test_utilstestA(unittest.TestCase):
  def setUp(self):
    self.input_gtf = 'data/igf_airflow/dag17_create_transcriptome_ref_utils/test.gtf'

  def tearDown(self):
    pass

  @staticmethod
  def assign_gene_id(x):
    gene_id_pattern = re.compile(r'.*gene_id \"(\S+)\"')
    gene_id = None
    if re.match(gene_id_pattern, x):
        gene_id = re.match(gene_id_pattern, x).group(1)
    return gene_id

  def test_filter_gtf_file_for_cellranger(self):
    filtered_gtf = \
      filter_gtf_file_for_cellranger(
        gtf_file=self.input_gtf,
        skip_gtf_rows=5)
    df = \
      pd.read_csv(
        filtered_gtf,
        sep='\t',
        skiprows=5,
        header=None,
        engine='c',
        low_memory=True)
    df['gene_id'] = \
      df[8].map(lambda x: self.assign_gene_id(x))
    gene_1_count = \
      len(df[df['gene_id']=='ENSG00000001167.15'].index)
    gene_2_count = \
      len(df[df['gene_id']=='ENSG00000068781.21'].index)
    self.assertEqual(gene_1_count, 49)
    self.assertEqual(gene_2_count, 0)

if __name__=='__main__':
  unittest.main()