import os, unittest
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base,Pipeline_seed
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json

class Pipelineadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    self.platform_json = 'data/platform_db_data.json'
    self.seqrun_json = 'data/seqrun_db_data.json'
    self.pipeline_json = 'data/pipeline_data.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    # load platform data
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=read_json_data(self.platform_json))
    # load seqrun data
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=read_json_data(self.seqrun_json))
    # load platform data
    pla = PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=read_json_data(self.pipeline_json))
    pipeline_seed_data = [{
      'pipeline_name':'demultiplexing_fastq',
      'seed_id':'1',
      'seed_table':'seqrun'}]
    pla.create_pipeline_seed(data=pipeline_seed_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_pipeline_records_pipeline_name(self):
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    pl_data = \
      pl.fetch_pipeline_records_pipeline_name(
        pipeline_name='demultiplexing_fastq')
    self.assertEqual(pl_data.pipeline_id, 1)

  def test_create_pipeline_seed(self):
    pipeline_seed_data1=[{'seed_id':'1', 'seed_table':'seqrun'},]
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    with self.assertRaises(ValueError):
      pl.create_pipeline_seed(data=pipeline_seed_data1)
    pl.close_session()
   
  def test_fetch_pipeline_seed_with_table_data(self):
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    (pipe_seed,table_data) = \
      pl.fetch_pipeline_seed_with_table_data(
        pipeline_name='demultiplexing_fastq')
    pl.close_session() 
    self.assertIsInstance(table_data.to_dict(orient='records'),list)
    self.assertEqual(len(table_data.to_dict(orient='records')), len(pipe_seed.to_dict(orient='records')))
    self.assertTrue('seqrun_igf_id' in list(table_data.columns))

  def test_update_pipeline_seed(self):
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    pipeline_seed_data1 = [{
      'pipeline_name':'demultiplexing_fastq',
      'seed_id':'2',
      'seed_table':'seqrun',},]
    with self.assertRaises(ValueError):
      pl.update_pipeline_seed(data=pipeline_seed_data1)
    pipeline_seed_data2 = [{
      'pipeline_name':'demultiplexing_fastq',
      'seed_id':'2',
      'seed_table':'seqrun',
      'status':'RUNNING'
    }]
    pl.update_pipeline_seed(data=pipeline_seed_data2)
    (pipe_seed1,table_data1) = \
      pl.fetch_pipeline_seed_with_table_data(
        pipeline_name='demultiplexing_fastq')
    self.assertEqual(len(table_data1.to_dict(orient='records')), len(pipe_seed1.to_dict(orient='records')))
    pipeline_seed_data3 = [{
      'pipeline_name':'demultiplexing_fastq',
      'seed_id':'1',
      'seed_table':'seqrun',
      'status':'RUNNING'},]
    pl.update_pipeline_seed(data=pipeline_seed_data3)
    (pipe_seed2,_) = \
      pl.fetch_pipeline_seed_with_table_data(
        pipeline_name='demultiplexing_fastq',
        status='RUNNING')
    pl.close_session()
    self.assertEqual(pipe_seed2.loc[pipe_seed2.seed_id==1]['status'].values[0],'RUNNING')


class Pipelineadaptor_test2(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    project_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'project_name':'test_22-8-2017_rna',
      'description':'Its project 1',
      'project_deadline':'Before August 2017',
      'comments':'Some samples are treated with drug X',
    }]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'IGF00001',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
      'library_strategy':'RNA-SEQ',
      'experiment_type':'POLYA-RNA'
    },{
      'sample_igf_id':'IGF00003',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
      'experiment_type':'POLYA-RNA'
    },{
      'sample_igf_id':'IGF00002',
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc'
    }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'sample_igf_id':'IGF00001',
      'experiment_igf_id':'IGF00001_HISEQ4000',
      'library_name':'IGF00001'
    },{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'sample_igf_id':'IGF00003',
      'experiment_igf_id':'IGF00003_HISEQ4000',
      'library_name':'IGF00001'
    },{
      'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
      'sample_igf_id':'IGF00002',
      'experiment_igf_id':'IGF00002_HISEQ4000',
      'library_name':'IGF00002'
    }]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    pipeline_data = [{
      "pipeline_name":"alignment",
      "pipeline_db":"sqlite:////data/aln.db", 
      "pipeline_init_conf":{ "input_dir":"data/fastq_dir/","output_dir":"data"}, 
      "pipeline_run_conf":{"output_dir":"data"}
    }]
    pl = PipelineAdaptor(**{'session':base.session})
    pl.store_pipeline_data(data=pipeline_data)
    pipeline_seed_data = [{
      'pipeline_name':'alignment',
      'seed_id':'1',
      'seed_table':'experiment'
    }]
    pl.create_pipeline_seed(data=pipeline_seed_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_fetch_pipeline_seed_with_table_data(self):
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    (pipe_seed,table_data) = \
      pl.fetch_pipeline_seed_with_table_data(
        pipeline_name='alignment',
        table_name='experiment')
    pl.close_session() 
    self.assertIsInstance(table_data.to_dict(orient='records'),list)
    self.assertEqual(len(table_data.to_dict(orient='records')), len(pipe_seed.to_dict(orient='records')))
    exp_id = table_data.to_dict(orient='records')[0]['experiment_igf_id']
    project_id = table_data.to_dict(orient='records')[0]['project_igf_id']
    self.assertEqual(exp_id, 'IGF00001_HISEQ4000')
    self.assertEqual(project_id, 'IGFP0001_test_22-8-2017_rna_sc')
    self.assertTrue('experiment_igf_id' in list(table_data.columns))


class Pipelineadaptor_test3(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    platform_data = [{
      "platform_igf_id" : "M03291" ,
      "model_name" : "MISEQ" ,
      "vendor_name" : "ILLUMINA" ,
      "software_name" : "RTA" ,
      "software_version" : "RTA1.18.54"
    }]
    flowcell_rule_data = [{
      "platform_igf_id":"M03291",
      "flowcell_type":"MISEQ",
      "index_1":"NO_CHANGE",
      "index_2":"NO_CHANGE"
    }]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    project_data=[{'project_igf_id':'IGFQ000123_avik_10-4-2018_Miseq'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id':'IGF103923',
      'project_igf_id':'IGFQ000123_avik_10-4-2018_Miseq',
      'species_name':'HG38'
    }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    seqrun_data = [{
      'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
      'flowcell_id':'000000000-BRN47',
      'platform_igf_id':'M03291',
      'flowcell':'MISEQ'
    }]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    pipeline_data = [{
      "pipeline_name" : "PrimaryAnalysis",
      "pipeline_db" : "sqlite:////bcl2fastq.db"
    },{
      "pipeline_name" : "DemultiplexIlluminaFastq",
      "pipeline_db" : "sqlite:////bcl2fastq.db"
    }]
    pla = PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=pipeline_data)
    file_data = [{
      'file_path':'/path/S20180405S_S1_L001_R1_001.fastq.gz',
      'location':'HPC_PROJECT',
      'md5':'fd5a95c18ebb7145645e95ce08d729e4',
      'size':'1528121404'
    },{
      'file_path':'/path/S20180405S_S1_L001_R2_001.fastq.gz',
      'location':'HPC_PROJECT',
      'md5':'fd5a95c18ebb7145645e95ce08d729e4',
      'size':'1467047580'
    },{
      'file_path':'/path/S20180405S_S3_L001_R2_001.fastq.gz',
      'location':'HPC_PROJECT',
      'md5':'fd5a95c18ebb7145645e95ce08d729e4',
      'size':'1467047580'
    }]
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{
      'name':'IGF103923_MISEQ_000000000-BRN47_1',
      'type':'demultiplexed_fastq','table':'run'
    },{
      'name':'IGF103923_MISEQ1_000000000-BRN47_1',
      'type':'demultiplexed_fastq','table':'run'
    }]
    collection_files_data = [{
      'name':'IGF103923_MISEQ_000000000-BRN47_1',
      'type':'demultiplexed_fastq',
      'file_path':'/path/S20180405S_S1_L001_R1_001.fastq.gz'
    },{
      'name':'IGF103923_MISEQ_000000000-BRN47_1',
      'type':'demultiplexed_fastq',
      'file_path':'/path/S20180405S_S1_L001_R2_001.fastq.gz'
    },{
      'name':'IGF103923_MISEQ1_000000000-BRN47_1',
      'type':'demultiplexed_fastq',
      'file_path':'/path/S20180405S_S3_L001_R2_001.fastq.gz'
    }]
    ca = \
      CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data) 
    ca.create_collection_group(data=collection_files_data)
    experiment_data = [{
      'project_igf_id':'IGFQ000123_avik_10-4-2018_Miseq',
      'sample_igf_id':'IGF103923',
      'experiment_igf_id':'IGF103923_MISEQ',
      'library_name':'IGF103923',
      'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
      'library_strategy':'RNA-SEQ',
      'experiment_type':'TENX-TRANSCRIPTOME-3P',
      'library_layout':'PAIRED',
      'platform_name':'MISEQ'
    },{
      'project_igf_id':'IGFQ000123_avik_10-4-2018_Miseq',
      'sample_igf_id':'IGF103923',
      'experiment_igf_id':'IGF103923_MISEQ1',
      'library_name':'IGF103923_1',
      'library_source':'GENOMIC_SINGLE_CELL',
      'library_strategy':'WGS',
      'experiment_type':'UNKNOWN',
      'library_layout':'PAIRED',
      'platform_name':'MISEQ'
    }]
    ea = \
      ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id':'IGF103923_MISEQ',
      'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
      'run_igf_id':'IGF103923_MISEQ_000000000-BRN47_1',
      'lane_number':'1'
    },{
      'experiment_igf_id':'IGF103923_MISEQ1',
      'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
      'run_igf_id':'IGF103923_MISEQ1_000000000-BRN47_1',
      'lane_number':'1'
    }]
    ra = \
      RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_seed_new_experiments(self):
    pl = \
      PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    new_exps,_=\
      pl.seed_new_experiments(\
        pipeline_name='PrimaryAnalysis',
        species_name_list=['HG38'],
        fastq_type='demultiplexed_fastq')
    self.assertEqual(len(new_exps),1)
    self.assertEqual(new_exps[0],'IGFQ000123_avik_10-4-2018_Miseq')

  def test_seed_new_experiments1(self):
    pl = PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    new_exps,_ = \
      pl.seed_new_experiments(\
        pipeline_name='PrimaryAnalysis',
        species_name_list=['HG38'],
        fastq_type='demultiplexed_fastq',
        project_list=['IGFQ000123_avik_10-4-2018_Miseq'],
        library_source_list=['TRANSCRIPTOMIC_SINGLE_CELL'])
    self.assertFalse(new_exps)
    pl.close_session()
    pl = \
      PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    (_,exp_data) = \
      pl.fetch_pipeline_seed_with_table_data(
        pipeline_name='PrimaryAnalysis',
        table_name='experiment',
        status='SEEDED')
    self.assertEqual(len(list(exp_data['experiment_igf_id'].values)),1)
    self.assertEqual(exp_data['experiment_igf_id'].values[0],'IGF103923_MISEQ')


class Pipelineadaptor_test4(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    pipeline_data = [{
      "pipeline_name":"analysys_pipeline_1",
      "pipeline_db":"localhost:8080",
      "pipeline_type":"AIRFLOW"
    }]
    pla = \
      PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=pipeline_data)
    pipeseed_data = [{
      'seed_id':1,
      'seed_table':'analysis',
      'pipeline_name':'analysys_pipeline_1',
      'status':'SEEDED'}]
    pla.create_pipeline_seed(
      data=pipeseed_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_create_or_update_pipeline_seed(self):
    pl = \
      PipelineAdaptor(**{
        'session_class': self.session_class})
    pl.start_session()
    pl.create_or_update_pipeline_seed(
      seed_id=1,
      pipeline_name='analysys_pipeline_1',
      new_status='RUNNING',
      seed_table='analysis')
    query = \
      pl.session.\
        query(Pipeline_seed).\
        filter(Pipeline_seed.seed_id=='1').\
        filter(Pipeline_seed.pipeline_id=='1').\
        filter(Pipeline_seed.seed_table=='analysis')
    pipeseed_entry = \
      pl.fetch_records(
        query=query,
        output_mode='one_or_none')
    self.assertEqual(pipeseed_entry.status,'RUNNING')
    pl.create_or_update_pipeline_seed(
      seed_id=1,
      pipeline_name='analysys_pipeline_1',
      new_status='FAILED',
      seed_table='analysis',
      no_change_status='RUNNING')
    pipeseed_entry = \
      pl.fetch_records(
        query=query,
        output_mode='one_or_none')
    self.assertEqual(pipeseed_entry.status,'RUNNING')
    pl.close_session()
    


if __name__ == '__main__':
  unittest.main()
