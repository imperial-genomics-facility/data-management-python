import unittest,os
import pandas as pd
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.igfTables import Pipeline
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.utils.dbutils import read_json_data
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_nextflow.nextflow_utils.nextflow_design import collect_fastq_with_run_and_pair_info_for_sample
from igf_nextflow.nextflow_utils.nextflow_design import get_nextflow_atacseq_design_and_params
from igf_nextflow.nextflow_utils.nextflow_design import get_nextflow_chipseq_design_and_params
from igf_nextflow.nextflow_utils.nextflow_design import extend_nextflow_analysis_design_and_params

class Nextflow_design_testA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    platform_data = [{
      "platform_igf_id" : "K00345",
      "model_name" : "HISEQ4000",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA2"}]
    flowcell_rule_data = [{
      "platform_igf_id":"K00345",
      "flowcell_type":"HiSeq 3000/4000 SR",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE"
      },{
      "platform_igf_id":"K00345",
      "flowcell_type":"HiSeq 3000/4000 PE",
      "index_1":"NO_CHANGE",
      "index_2":"REVCOMP"}]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id':'seqrunA',
      'flowcell_id':'000000000',
      'platform_igf_id':'K00345',
      'flowcell':'HISEQ4000'}]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{'project_igf_id':'projectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'sampleA',
      'project_igf_id':'projectA',
      'species_name':'HG38'
      }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id':'projectA',
      'sample_igf_id':'sampleA',
      'experiment_igf_id':'sampleA_HISEQ4000',
      'library_name':'sampleA',
      'library_source':'GENOMIC',
      'library_strategy':'ATAC-SEQ',
      'experiment_type':'ATAC-SEQ',
      'library_layout':'PAIRED',
      'platform_name':'HISEQ4000'}]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id':'sampleA_HISEQ4000',
      'seqrun_igf_id':'seqrunA',
      'run_igf_id':'sampleA_HISEQ4000_000000000_1',
      'lane_number':'1'
      },{
      'experiment_igf_id':'sampleA_HISEQ4000',
      'seqrun_igf_id':'seqrunA',
      'run_igf_id':'sampleA_HISEQ4000_000000000_2',
      'lane_number':'2'
      },{
      'experiment_igf_id':'sampleA_HISEQ4000',
      'seqrun_igf_id':'seqrunA',
      'run_igf_id':'sampleA_HISEQ4000_000000000_3',
      'lane_number':'3'}]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    file_data = [{
      'file_path':'/path/sampleA_S1_L001_R1_001.fastq.gz',
      'location':'HPC_PROJECT'
      },{
      'file_path':'/path/sampleA_S1_L001_R2_001.fastq.gz',
      'location':'HPC_PROJECT'
      },{
      'file_path':'/path/sampleA_S1_L002_R1_001.fastq.gz',
      'location':'HPC_PROJECT'
      },{
      'file_path':'/path/sampleA_S1_L002_R2_001.fastq.gz',
      'location':'HPC_PROJECT'
      },{
      'file_path':'/path/sampleA_S1_L003_R1_001.fastq.gz',
      'location':'HPC_PROJECT'
      },{
      'file_path':'/path/sampleA_S1_L003_R2_001.fastq.gz',
      'location':'HPC_PROJECT'}]
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name':'sampleA_HISEQ4000_000000000_1',
      'type':'demultiplexed_fastq',
      'table':'run'
      },{
      'name':'sampleA_HISEQ4000_000000000_2',
      'type':'demultiplexed_fastq',
      'table':'run'
      },{
      'name':'sampleA_HISEQ4000_000000000_3',
      'type':'demultiplexed_fastq',
      'table':'run'}]
    collection_files_data = [{
      'name':'sampleA_HISEQ4000_000000000_1',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L001_R1_001.fastq.gz'
      },{
      'name':'sampleA_HISEQ4000_000000000_1',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L001_R2_001.fastq.gz'
      },{
      'name':'sampleA_HISEQ4000_000000000_2',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L002_R1_001.fastq.gz'
      },{
      'name':'sampleA_HISEQ4000_000000000_2',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L002_R2_001.fastq.gz'
      },{
      'name':'sampleA_HISEQ4000_000000000_3',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L003_R1_001.fastq.gz'
      },{
      'name':'sampleA_HISEQ4000_000000000_3',
      'type':'demultiplexed_fastq',
      'file_path':'/path/sampleA_S1_L003_R2_001.fastq.gz'}]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()

  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_collect_fastq_with_run_and_pair_info_for_sample(self):
    sample_fastq_data = \
      collect_fastq_with_run_and_pair_info_for_sample(
        sample_igf_id_list=['sampleA'],
        dbconf_file=self.dbconfig)
    self.assertEqual(len(sample_fastq_data),3)
    sample_fastq_data = pd.DataFrame(sample_fastq_data)
    run_3 = \
      sample_fastq_data[sample_fastq_data['run_igf_id']=='sampleA_HISEQ4000_000000000_3']
    self.assertEqual(len(run_3.index),1)
    self.assertEqual(run_3['r1_fastq_file'].values[0],'/path/sampleA_S1_L003_R1_001.fastq.gz')
    self.assertEqual(run_3['r2_fastq_file'].values[0],'/path/sampleA_S1_L003_R2_001.fastq.gz')


  def test_get_nextflow_atacseq_design_and_params(self):
    analysis_description = {
      'nextflow_pipeline':'atacseq',
      'nextflow_design':[{
        'group':'sampleA','replicate':1,'sample_igf_id':'sampleA'}],
      'nextflow_params':[
        '--genome HG38',
        '--seq_center test',
        '--narrow_peak']}
    extended_analysis_design,extended_analysis_params,input_dir_list = \
      get_nextflow_atacseq_design_and_params(
        analysis_description=analysis_description,
        igf_seq_center='IGF',
        dbconf_file=self.dbconfig)
    self.assertEqual(len(extended_analysis_design),3)
    self.assertTrue('fastq_1' in extended_analysis_design[0])
    self.assertTrue('nf-core/atacseq' in extended_analysis_params)
    self.assertTrue('--seq_center IGF' in extended_analysis_params)
    self.assertTrue('/path' in input_dir_list)

  def test_get_nextflow_chipseq_design_and_params(self):
    analysis_description = {
      'nextflow_pipeline':'chipseq',
      'nextflow_design':[{
        'group':'sampleA','replicate':1,'sample_igf_id':'sampleA'}],
      'nextflow_params':[
        '--genome HG38',
        '--seq_center test',
        '--narrow_peak']}
    extended_analysis_design,extended_analysis_params,input_dir_list = \
      get_nextflow_chipseq_design_and_params(
        analysis_description=analysis_description,
        igf_seq_center='IGF',
        dbconf_file=self.dbconfig)
    self.assertEqual(len(extended_analysis_design),3)
    self.assertTrue('fastq_1' in extended_analysis_design[0])
    self.assertTrue('nf-core/chipseq' in extended_analysis_params)
    self.assertTrue('--seq_center IGF' in extended_analysis_params)
    self.assertTrue('/path' in input_dir_list)

if __name__=='__main__':
  unittest.main()