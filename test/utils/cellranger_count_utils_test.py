import os, unittest
from igf_data.utils.fileutils import get_temp_dir,remove_dir
from igf_data.utils.tools.cellranger.cellranger_count_utils import _check_cellranger_multi_output

class Cellranger_count_utils_testA(unittest.TestCase):
  def setUp(self):
    self.work_dir = get_temp_dir()
    self.lib_csv = os.path.join(self.work_dir,'lib.csv')
    libs_data = [
      '[gene-expression]',
      'reference,/path/GRCh38',
      '[vdj]',
      'reference,/path/refdata-cellranger-vdj-GRCh38-alts-ensembl-5.0.0',
      '[libraries]',
      'fastq_id,fastqs,lanes,feature_types,subsample_rate',
      '1-GEX,/var/tmp/pbs.2669197.pbs/4,,Gene Expression,',
      '1-VDJT,/var/tmp/pbs.2669197.pbs/4,,VDJ-T,',
      '1-VDJB,/var/tmp/pbs.2669197.pbs/4,,VDJ-B,',
      '1-VDJ,/var/tmp/pbs.2669197.pbs/4,,VDJ,',
      '1-VDJB,/var/tmp/pbs.2669197.pbs/4,,Antibody Capture,',
    ]
    with open(self.lib_csv,'w') as fp:
      for l in libs_data:
        fp.write('{0}\n'.format(l))
    self.sample_id = 'sampleA'
    output_path = \
      os.path.join(self.work_dir,self.sample_id,'outs')
    os.makedirs(output_path)
    os.makedirs(os.path.join(output_path,'count'))
    os.makedirs(os.path.join(output_path,'vdj'))
    os.makedirs(os.path.join(output_path,'vdj_b'))
    os.makedirs(os.path.join(output_path,'vdj_t'))
    with open(os.path.join(output_path,'web_summary.html'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'count/filtered_feature_bc_matrix.h5'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'count/possorted_genome_bam.bam'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'count/cloupe.cloupe'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj/filtered_contig_annotations.csv'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj/vloupe.vloupe'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj_t/filtered_contig_annotations.csv'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj_t/vloupe.vloupe'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj_b/filtered_contig_annotations.csv'),'w') as fp:
      fp.write('a')
    with open(os.path.join(output_path,'vdj_b/vloupe.vloupe'),'w') as fp:
      fp.write('a')

  def tearDown(self):
    remove_dir(self.work_dir)

  def test_check_cellranger_multi_output(self):
    self.assertEqual(
      _check_cellranger_multi_output(
        cellranger_output=os.path.join(self.work_dir,self.sample_id,'outs'),
        library_csv=self.lib_csv),None)
    with open(self.lib_csv,'a') as fp:
      fp.write('1-VDJB,/var/tmp/pbs.2669197.pbs/4,,Antibody Capture BAD,\n')
    with self.assertRaises(ValueError):
       _check_cellranger_multi_output(
        cellranger_output=os.path.join(self.work_dir,self.sample_id,'outs'),
        library_csv=self.lib_csv)

if __name__ == '__main__':
  unittest.main()