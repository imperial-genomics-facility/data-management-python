import os, unittest
from igf_data.igfdb.igfTables import Base, Pipeline
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
from igf_data.utils.dbutils import read_json_data, read_dbconf_json
from igf_data.utils.pipelineutils import load_new_pipeline_data
from igf_data.utils.pipelineutils import find_new_analysis_seeds
from igf_data.utils.pipelineutils import check_and_load_pipeline
from igf_data.utils.fileutils import get_temp_dir,remove_dir

class Pipelineutils_test1(unittest.TestCase):
  def setUp(self):
    self.data_file = 'data/pipeline_data.json'
    self.dbconfig = 'data/dbconfig.json'

    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)


  def test_load_new_pipeline_data(self): 
    load_new_pipeline_data(data_file=self.data_file,
                           dbconfig=self.dbconfig)
    pp = PipelineAdaptor(**{'session_class': self.session_class})
    pp.start_session()
    data = pp.fetch_pipeline_records_pipeline_name(pipeline_name='demultiplexing_fastq')
    pp.close_session()
    self.assertEqual(data.pipeline_name,'demultiplexing_fastq')

class Pipelineutils_test2(unittest.TestCase):
  def setUp(self):
    self.data_file = 'data/pipeline_data.json'
    self.dbconfig = 'data/dbconfig.json'

    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    self.temp_dir=get_temp_dir()
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    platform_data = [{"platform_igf_id" : "M03291" ,
                      "model_name" : "MISEQ" ,
                      "vendor_name" : "ILLUMINA" ,
                      "software_name" : "RTA" ,
                      "software_version" : "RTA1.18.54"
                     },
                     {"platform_igf_id" : "NB501820",
                      "model_name" : "NEXTSEQ",
                      "vendor_name" : "ILLUMINA",
                      "software_name" : "RTA",
                      "software_version" : "RTA2"
                     },
                     {"platform_igf_id" : "K00345",
                      "model_name" : "HISEQ4000",
                      "vendor_name" : "ILLUMINA",
                      "software_name" : "RTA",
                      "software_version" : "RTA2"
                     }
                    ]
    flowcell_rule_data = [{"platform_igf_id":"K00345",
                           "flowcell_type":"HiSeq 3000/4000 SR",
                           "index_1":"NO_CHANGE",
                           "index_2":"NO_CHANGE"},
                          {"platform_igf_id":"K00345",
                           "flowcell_type":"HiSeq 3000/4000 PE",
                           "index_1":"NO_CHANGE",
                           "index_2":"REVCOMP"},
                          {"platform_igf_id":"NB501820",
                           "flowcell_type":"NEXTSEQ",
                           "index_1":"NO_CHANGE",
                           "index_2":"REVCOMP"},
                          {"platform_igf_id":"M03291",
                           "flowcell_type":"MISEQ",
                           "index_1":"NO_CHANGE",
                           "index_2":"NO_CHANGE"}
                         ]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
                    'flowcell_id':'000000000-BRN47',
                    'platform_igf_id':'M03291',
                    'flowcell':'MISEQ',
                   },
                   {'seqrun_igf_id':'180416_NB03291_013_000000001-BRN47',
                    'flowcell_id':'000000001-BRN47',
                    'platform_igf_id':'NB501820',
                    'flowcell':'NEXTSEQ',
                   }
                  ]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{'project_igf_id':'projectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{'sample_igf_id':'sampleA',
                    'project_igf_id':'projectA',
                    'species_name':'HG38'},
                   {'sample_igf_id':'sampleB',
                    'project_igf_id':'projectA',
                    'species_name':'UNKNOWN'},
                  ]
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{'project_igf_id':'projectA',
                        'sample_igf_id':'sampleA',
                        'experiment_igf_id':'sampleA_MISEQ',
                        'library_name':'sampleA',
                        'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                        'library_strategy':'RNA-SEQ',
                        'experiment_type':'TENX-TRANSCRIPTOME-3P',
                        'library_layout':'PAIRED',
                        'platform_name':'MISEQ',
                       },
                       {'project_igf_id':'projectA',
                        'sample_igf_id':'sampleA',
                        'experiment_igf_id':'sampleA_NEXTSEQ',
                        'library_name':'sampleA',
                        'library_source':'UNKNOWN',
                        'library_strategy':'RNA-SEQ',
                        'experiment_type':'TENX-TRANSCRIPTOME-3P',
                        'library_layout':'PAIRED',
                        'platform_name':'NEXTSEQ',
                       },
                       {'project_igf_id':'projectA',
                        'sample_igf_id':'sampleB',
                        'experiment_igf_id':'sampleB_MISEQ',
                        'library_name':'sampleB',
                        'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                        'library_strategy':'RNA-SEQ',
                        'experiment_type':'TENX-TRANSCRIPTOME-3P',
                        'library_layout':'PAIRED',
                        'platform_name':'MISEQ',
                       },
                      ]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{'experiment_igf_id':'sampleA_MISEQ',
                 'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
                 'run_igf_id':'sampleA_MISEQ_000000000-BRN47_1',
                'lane_number':'1'
                },
                {'experiment_igf_id':'sampleA_NEXTSEQ',
                 'seqrun_igf_id':'180416_NB03291_013_000000001-BRN47',
                 'run_igf_id':'sampleA_NEXTSEQ_000000001-BRN47_2',
                'lane_number':'2'
                },
                {'experiment_igf_id':'sampleB_MISEQ',
                 'seqrun_igf_id':'180416_M03291_0139_000000000-BRN47',
                 'run_igf_id':'sampleB_MISEQ_HVWN7BBXX_1',
                 'lane_number':'1'
                }
               ]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    file_data = [{'file_path':'/path/sampleA_MISEQ_000000000-BRN47_1_R1.fastq.gz',
                  'location':'HPC_PROJECT',
                  'md5':'fd5a95c18ebb7145645e95ce08d729e4',
                  'size':'1528121404',
                 },
                 {'file_path':'/path/sampleA_NEXTSEQ_000000001-BRN47_2_R1.fastq.gz',
                  'location':'HPC_PROJECT',
                  'md5':'fd5a95c18ebb7145645e95ce08d729e4',
                  'size':'1528121404',
                 },
                 {'file_path':'/path/sampleB_MISEQ_HVWN7BBXX_1_R1.fastq.gz',
                  'location':'HPC_PROJECT',
                  'md5':'fd5a95c18ebb7145645e95ce08d729e4',
                  'size':'1528121404',
                 },
                ]
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(data=file_data)
    collection_data = [{'name':'sampleA_MISEQ_000000000-BRN47_1',
                        'type':'demultiplexed_fastq',
                        'table':'run'},
                       {'name':'sampleA_NEXTSEQ_000000001-BRN47_2',
                        'type':'demultiplexed_fastq',
                        'table':'run'},
                       {'name':'sampleB_MISEQ_HVWN7BBXX_1',
                        'type':'demultiplexed_fastq',
                        'table':'run'}
                      ]
    collection_files_data = [{'name':'sampleA_MISEQ_000000000-BRN47_1',
                              'type':'demultiplexed_fastq',
                              'file_path':'/path/sampleA_MISEQ_000000000-BRN47_1_R1.fastq.gz'
                             },
                             {'name':'sampleA_NEXTSEQ_000000001-BRN47_2',
                              'type':'demultiplexed_fastq',
                              'file_path':'/path/sampleA_NEXTSEQ_000000001-BRN47_2_R1.fastq.gz'
                             },
                             {'name':'sampleB_MISEQ_HVWN7BBXX_1',
                              'type':'demultiplexed_fastq',
                              'file_path':'/path/sampleB_MISEQ_HVWN7BBXX_1_R1.fastq.gz'
                             }
                            ]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    pipeline_data = [{"pipeline_name" : "PrimaryAnalysis",
                      "pipeline_db" : "sqlite:////bcl2fastq.db",
                     },
                     {"pipeline_name" : "DemultiplexIlluminaFastq",
                      "pipeline_db" : "sqlite:////bcl2fastq.db",
                     },
                    ]
    pla = PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=pipeline_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    if os.path.exists(self.temp_dir):
      remove_dir(dir_path=self.temp_dir)

  def test_find_new_analysis_seeds1(self):
    project_name_file=os.path.join(self.temp_dir,
                                   'project_name_list.txt')
    with open(project_name_file,'w') as fp:
      fp.write('')

    available_exps,seeded_exps = \
      find_new_analysis_seeds(\
        dbconfig_path=self.dbconfig,
        pipeline_name='PrimaryAnalysis',
        project_name_file=project_name_file,
        species_name_list=['HG38'],
        fastq_type='demultiplexed_fastq',
        library_source_list=['TRANSCRIPTOMIC_SINGLE_CELL']
      )
    self.assertTrue('projectA' in available_exps)
    self.assertTrue(seeded_exps is None)
    pla = PipelineAdaptor(**{'session_class':self.session_class})
    pla.start_session()
    seeded_data, exp_data = pla.fetch_pipeline_seed_with_table_data(\
                              pipeline_name='PrimaryAnalysis',
                              table_name='experiment',
                              status='SEEDED')
    pla.close_session()
    self.assertEqual(len(seeded_data.index),0)

  def test_find_new_analysis_seeds2(self):
    base = BaseAdaptor(**{'session_class':self.session_class})
    project_name_file=os.path.join(self.temp_dir,
                                   'project_name_list.txt')
    with open(project_name_file,'w') as fp:
      fp.write('projectA')

    available_exps,seeded_exps = \
      find_new_analysis_seeds(\
        dbconfig_path=self.dbconfig,
        pipeline_name='PrimaryAnalysis',
        project_name_file=project_name_file,
        species_name_list=['HG38'],
        fastq_type='demultiplexed_fastq',
        library_source_list=['TRANSCRIPTOMIC_SINGLE_CELL']
      )
    self.assertTrue(available_exps is None)
    self.assertTrue('projectA' in seeded_exps)
    pla = PipelineAdaptor(**{'session_class':self.session_class})
    pla.start_session()
    seeded_data, exp_data = pla.fetch_pipeline_seed_with_table_data(\
                              pipeline_name='PrimaryAnalysis',
                              table_name='experiment',
                              status='SEEDED')
    pla.close_session()
    exp_data = exp_data.to_dict(orient='records')
    self.assertTrue(len(exp_data),1)
    self.assertEqual(exp_data[0]['experiment_igf_id'],'sampleA_MISEQ')

  def test_check_and_load_pipeline(self):
    pipeline_data = [
      {"pipeline_name" : "PrimaryAnalysis"}]
    check_and_load_pipeline(
      pipeline_data=pipeline_data,
      dbconfig=self.dbconfig)
    pla = PipelineAdaptor(**{'session_class':self.session_class})
    pla.start_session()
    query = \
      pla.session.\
        query(Pipeline).\
        filter(Pipeline.pipeline_name == 'PrimaryAnalysis')
    pipeline_records = \
      pla.fetch_records(
        query=query,
        output_mode='one_or_none')
    pla.close_session()
    self.assertTrue(pipeline_records is not None)
    pipeline_data = [
      {"pipeline_name" : "PrimaryAnalysis1",
       "pipeline_db" : "postgres"}]
    check_and_load_pipeline(
      pipeline_data=pipeline_data,
      dbconfig=self.dbconfig)
    pla.start_session()
    query = \
      pla.session.\
        query(Pipeline).\
        filter(Pipeline.pipeline_name == 'PrimaryAnalysis1')
    pipeline_records = \
      pla.fetch_records(
        query=query,
        output_mode='one_or_none')
    pla.close_session()
    self.assertTrue(pipeline_records is not None)

if __name__ == '__main__':
  unittest.main()
