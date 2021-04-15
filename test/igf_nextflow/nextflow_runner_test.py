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
from igf_nextflow.nextflow_utils.nextflow_runner import nextflow_pre_run_setup
from igf_nextflow.nextflow_utils.nextflow_config_formatter import format_nextflow_config_file


class Nextflow_pre_run_setup_testA(unittest.TestCase):
  def setUp(self):
    self.analysis_description = {
      'nextflow_pipeline':'atacseq',
      'nextflow_design':[{
        'group':'sampleA','replicate':1,'sample_igf_id':'sampleA'}],
      'nextflow_params':[
        '--genome HG38',
        '--seq_center test',
        '--narrow_peak']}
    self.template_file = 'igf_nextflow/config/nextflow.cfg_template'
    self.temp_dir = get_temp_dir()
    self.nextflow_exe = \
      os.path.join(self.temp_dir,'nextflow')
    with open(self.nextflow_exe,'w') as fp:
      fp.write('A')
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

  def test_nextflow_pre_run_setup(self):
    nextflow_params,work_dir = \
      nextflow_pre_run_setup(
        nextflow_exe=self.nextflow_exe,
        analysis_description=self.analysis_description,
        dbconf_file=self.dbconfig,
        nextflow_config_template=self.template_file,
        igenomes_base_path=None,
        hpc_queue_name='test',
        use_ephemeral_space=False)
    self.assertEqual(nextflow_params[0],self.nextflow_exe)
    self.assertTrue('-profile singularity' in nextflow_params)
    self.assertTrue('--input {0}/atacseq_input_design.csv'.format(work_dir) in nextflow_params)
    self.assertTrue(os.path.exists(work_dir))
    self.assertTrue(os.path.exists(os.path.join(work_dir,'atacseq_input_design.csv')))
    atacseq_design_df = pd.read_csv(os.path.join(work_dir,'atacseq_input_design.csv'))
    self.assertTrue('fastq_1' in atacseq_design_df.columns)
    sample_A_fastq_1_list = list(atacseq_design_df[atacseq_design_df['group']=='sampleA']['fastq_1'].values)
    self.assertTrue('/path/sampleA_S1_L001_R1_001.fastq.gz' in sample_A_fastq_1_list)
    self.assertEqual(len(sample_A_fastq_1_list),3)

if __name__=='__main__':
  unittest.main()