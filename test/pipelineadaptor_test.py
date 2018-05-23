import os, unittest
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.utils.dbutils import read_json_data, read_dbconf_json

class Pipelineadaptor_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    self.platform_json='data/platform_db_data.json'
    self.seqrun_json='data/seqrun_db_data.json'
    self.pipeline_json='data/pipeline_data.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    # load platform data
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=read_json_data(self.platform_json))
    # load seqrun data
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=read_json_data(self.seqrun_json))
    # load platform data
    pla=PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=read_json_data(self.pipeline_json))
    pipeline_seed_data=[{'pipeline_name':'demultiplexing_fastq','seed_id':'1', 'seed_table':'seqrun'},]
    pla.create_pipeline_seed(data=pipeline_seed_data)
    base.close_session()

  
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)


  def test_fetch_pipeline_records_pipeline_name(self):
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    pl_data=pl.fetch_pipeline_records_pipeline_name(pipeline_name='demultiplexing_fastq')
    self.assertEqual(pl_data.pipeline_id, 1)


  def test_create_pipeline_seed(self):
    pipeline_seed_data1=[{'seed_id':'1', 'seed_table':'seqrun'},]
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    with self.assertRaises(ValueError):
      pl.create_pipeline_seed(data=pipeline_seed_data1)
    pl.close_session()

    
  def test_fetch_pipeline_seed_with_table_data(self):
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    (pipe_seed,table_data)=pl.fetch_pipeline_seed_with_table_data(pipeline_name='demultiplexing_fastq')
    pl.close_session() 
    self.assertIsInstance(table_data.to_dict(orient='records'),list)
    self.assertEqual(len(table_data.to_dict(orient='records')), len(pipe_seed.to_dict(orient='records')))
    self.assertTrue('seqrun_igf_id' in list(table_data.columns))

  def test_update_pipeline_seed(self):
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    pipeline_seed_data1=[{'pipeline_name':'demultiplexing_fastq','seed_id':'2', 'seed_table':'seqrun',},]
    with self.assertRaises(ValueError):
      pl.update_pipeline_seed(data=pipeline_seed_data1)
    pipeline_seed_data2=[{'pipeline_name':'demultiplexing_fastq','seed_id':'2', 'seed_table':'seqrun','status':'RUNNING'},]
    pl.update_pipeline_seed(data=pipeline_seed_data2)
    (pipe_seed1,table_data1)=pl.fetch_pipeline_seed_with_table_data(pipeline_name='demultiplexing_fastq')
    self.assertEqual(len(table_data1.to_dict(orient='records')), len(pipe_seed1.to_dict(orient='records')))
    pipeline_seed_data3=[{'pipeline_name':'demultiplexing_fastq','seed_id':'1', 'seed_table':'seqrun','status':'RUNNING'},]
    pl.update_pipeline_seed(data=pipeline_seed_data3)
    (pipe_seed2,table_data2)=pl.fetch_pipeline_seed_with_table_data(pipeline_name='demultiplexing_fastq',status='RUNNING')
    pl.close_session()
    self.assertEqual(pipe_seed2.loc[pipe_seed2.seed_id==1]['status'].values[0],'RUNNING')

class Pipelineadaptor_test2(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                   'project_name':'test_22-8-2017_rna',
                   'description':'Its project 1',
                   'project_deadline':'Before August 2017',
                   'comments':'Some samples are treated with drug X',
                 }]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data=[{'sample_igf_id':'IGF00001',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                  'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                  'library_strategy':'RNA-SEQ',
                  'experiment_type':'POLYA-RNA'},
                 {'sample_igf_id':'IGF00003',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                  'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                  'experiment_type':'POLYA-RNA'},
                 {'sample_igf_id':'IGF00002',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',},
                ]
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                      'sample_igf_id':'IGF00001',
                      'experiment_igf_id':'IGF00001_HISEQ4000',
                      'library_name':'IGF00001'},
                     {'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                      'sample_igf_id':'IGF00003',
                      'experiment_igf_id':'IGF00003_HISEQ4000',
                      'library_name':'IGF00001'},
                     {'project_igf_id':'IGFP0001_test_22-8-2017_rna_sc',
                      'sample_igf_id':'IGF00002',
                      'experiment_igf_id':'IGF00002_HISEQ4000',
                      'library_name':'IGF00002'},
                    ]
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    pipeline_data=[{"pipeline_name" : "alignment",
                    "pipeline_db" : "sqlite:////data/aln.db", 
                    "pipeline_init_conf" : { "input_dir":"data/fastq_dir/" , "output_dir" : "data"}, 
                    "pipeline_run_conf" : { "output_dir" : "data" }
                  }]
    pl=PipelineAdaptor(**{'session':base.session})
    pl.store_pipeline_data(data=pipeline_data)
    pipeline_seed_data=[{'pipeline_name':'alignment','seed_id':'1', 'seed_table':'experiment'},]
    pl.create_pipeline_seed(data=pipeline_seed_data)
    base.close_session()

  def test_fetch_pipeline_seed_with_table_data(self):
    pl=PipelineAdaptor(**{'session_class': self.session_class})
    pl.start_session()
    (pipe_seed,table_data)=pl.fetch_pipeline_seed_with_table_data(pipeline_name='alignment',
                                                                  table_name='experiment')
    pl.close_session() 
    self.assertIsInstance(table_data.to_dict(orient='records'),list)
    self.assertEqual(len(table_data.to_dict(orient='records')), len(pipe_seed.to_dict(orient='records')))
    exp_id=table_data.to_dict(orient='records')[0]['experiment_igf_id']
    project_id=table_data.to_dict(orient='records')[0]['project_igf_id']
    self.assertEqual(exp_id, 'IGF00001_HISEQ4000')
    self.assertEqual(project_id, 'IGFP0001_test_22-8-2017_rna_sc')
    self.assertTrue('experiment_igf_id' in list(table_data.columns))

if __name__ == '__main__':
  unittest.main()
