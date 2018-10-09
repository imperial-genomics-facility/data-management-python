import pandas as pd
import os,tarfile,unittest
from dateutil.parser import parse
from igf_data.utils.fileutils import prepare_file_archive,get_temp_dir,remove_dir
from igf_data.utils.fileutils import create_file_manifest_for_dir,get_datestamp_label

class Fileutils_test1(unittest.TestCase):
  def setUp(self):
    file_list=[
               'web_summary.html',
               'metrics_summary.csv',
               'possorted_genome_bam.bam',
               'possorted_genome_bam.bam.bai',
               'filtered_gene_bc_matrices/GRCh38/matrix.mtx',
               'filtered_gene_bc_matrices/GRCh38/genes.tsv',
               'filtered_gene_bc_matrices/GRCh38/barcodes.tsv',
               'filtered_gene_bc_matrices_h5.h5',
               'raw_gene_bc_matrices/GRCh38/matrix.mtx',
               'raw_gene_bc_matrices/GRCh38/genes.tsv',
               'raw_gene_bc_matrices/GRCh38/barcodes.tsv',
               'raw_gene_bc_matrices_h5.h5',
               'analysis/pca/10_components/projection.csv',
               'analysis/pca/10_components/components.csv',
               'analysis/pca/10_components/variance.csv',
               'analysis/pca/10_components/dispersion.csv',
               'analysis/pca/10_components/genes_selected.csv',
               'molecule_info.h5',
               'cloupe.cloupe'
              ]
    self.results_dir=get_temp_dir()
    self.output_tar_file=os.path.join(get_temp_dir(),
                                      'test.tar')
    self.output_targz_file=os.path.join(get_temp_dir(),
                                        'test.tar.gz')
    self.manifest_file=os.path.join(get_temp_dir(),
                                    'file_manifest.csv')
    for file in file_list:
      file=os.path.join(self.results_dir,file)
      file_dir=os.path.dirname(file)

      if file_dir != '' and not os.path.exists(file_dir):
        os.makedirs(file_dir)

      if not os.path.exists(file):
        with open(file,'w') as fp:
          fp.write('A')

  def tearDown(self):
    remove_dir(dir_path=self.results_dir)
    if os.path.exists(self.output_tar_file):
      os.remove(self.output_tar_file)

    if os.path.exists(self.output_targz_file):
      os.remove(self.output_targz_file)

  def test_tar_output(self):
    prepare_file_archive(results_dirpath=self.results_dir,
                         output_file=self.output_tar_file,
                         gzip_output=False,
                         exclude_list=['*.bam','*.bam.bai']
                        )
    with tarfile.open(self.output_tar_file,'r') as tar:
      tar_file_list=tar.getnames()
    self.assertTrue('web_summary.html' in tar_file_list)
    self.assertTrue('possorted_genome_bam.bam.html' not in tar_file_list)

  def test_targz_output(self):
    prepare_file_archive(results_dirpath=self.results_dir,
                         output_file=self.output_targz_file,
                         gzip_output=True,
                         exclude_list=['*.bam','*.bam.bai']
                        )
    with tarfile.open(self.output_targz_file,'r:gz') as tar:
      tar_file_list=tar.getnames()
    self.assertTrue('web_summary.html' in tar_file_list)
    self.assertTrue('possorted_genome_bam.bam.html' not in tar_file_list)

  def test_create_file_manifest_for_dir(self):
    create_file_manifest_for_dir(results_dirpath=self.results_dir,
                                 output_file=self.manifest_file)
    manifest_data=pd.read_csv(self.manifest_file)
    html_data=manifest_data[manifest_data['file_path']=='web_summary.html']
    html_size=html_data['size'].values[0]
    self.assertEqual(len(html_data.index),1)
    self.assertEqual(html_size,1)

  def test_get_datestamp_label(self):
    date_str='2018-08-23 15:15:01'
    self.assertEqual(get_datestamp_label(date_str),'20180823')
    self.assertEqual(get_datestamp_label(parse(date_str)),'20180823')


if __name__ == '__main__':
  unittest.main()