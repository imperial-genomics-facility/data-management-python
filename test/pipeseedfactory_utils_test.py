import os, unittest, sqlalchemy
import pandas as pd
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.igfTables import Base,Pipeline,Pipeline_seed,Experiment,Seqrun
from igf_data.utils.ehive_utils.pipeseedfactory_utils import get_pipeline_seeds
from igf_data.utils.ehive_utils.pipeseedfactory_utils import _get_date_from_seqrun

class Pipeseedfactory_utils_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    # PLATFORM
    platform_data=[{ "platform_igf_id" : "M03291" ,
                     "model_name" : "MISEQ" ,
                     "vendor_name" : "ILLUMINA",
                     "software_name" : "RTA" ,
                     "software_version" : "RTA1.18.54"
                  }]
    flowcell_rule_data=[{"platform_igf_id":"M03291",
                         "flowcell_type":"MISEQ",
                         "index_1":"NO_CHANGE",
                         "index_2":"NO_CHANGE"}]
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    # SEQRUN
    seqrun_data=[{'seqrun_igf_id':'180416_M03291_0139_000000000-TEST',
                  'flowcell_id':'000000000-TEST',
                  'platform_igf_id':'M03291',
                  'flowcell':'MISEQ',
                },
                {'seqrun_igf_id':'180416_M03291_0140_000000000-TEST',
                  'flowcell_id':'000000000-TEST',
                  'platform_igf_id':'M03291',
                  'flowcell':'MISEQ',
                }]
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    # PROJECT
    project_data=[{'project_igf_id':'IGFQ000123_test_10-4-2018_Miseq'}]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    # SAMPLE
    sample_data=[{'sample_igf_id':'IGF00123',
                  'project_igf_id':'IGFQ000123_test_10-4-2018_Miseq'},
                 {'sample_igf_id':'IGF00124',
                  'project_igf_id':'IGFQ000123_test_10-4-2018_Miseq'}]
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    # EXPERIMENT
    experiment_data=[{'project_igf_id':'IGFQ000123_test_10-4-2018_Miseq',
                      'sample_igf_id':'IGF00123',
                      'experiment_igf_id':'IGF00123_MISEQ',
                      'library_name':'IGF00123',
                      'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                      'library_strategy':'RNA-SEQ',
                      'experiment_type':'POLYA-RNA',
                      'library_layout':'PAIRED',
                      'platform_name':'MISEQ',
                      'singlecell_chemistry':'TENX'
                    },
                    {'project_igf_id':'IGFQ000123_test_10-4-2018_Miseq',
                      'sample_igf_id':'IGF00124',
                      'experiment_igf_id':'IGF00124_MISEQ',
                      'library_name':'IGF00124',
                      'library_source':'TRANSCRIPTOMIC_SINGLE_CELL',
                      'library_strategy':'RNA-SEQ',
                      'experiment_type':'POLYA-RNA',
                      'library_layout':'PAIRED',
                      'platform_name':'MISEQ',
                      'singlecell_chemistry':'TENX'
                    }]
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    # RUN
    run_data=[{'experiment_igf_id':'IGF00123_MISEQ',
               'seqrun_igf_id':'180416_M03291_0139_000000000-TEST',
               'run_igf_id':'IGF00123_MISEQ_000000000-TEST_1',
               'lane_number':'1'
             }]
    ra=RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    # PIPELINE
    pipeline_data=[{ "pipeline_name" : "PrimaryAnalysis",
                     "pipeline_db" : "sqlite:////aln.db", 
                   },
                   { "pipeline_name" : "DemultiplexingFastq",
                     "pipeline_db" : "sqlite:////fastq.db", 
                   }]
    pipeline_seed_data=[{'pipeline_name':'PrimaryAnalysis',
                         'seed_id':1,
                         'seed_table':'experiment'},
                        {'pipeline_name':'PrimaryAnalysis',
                         'seed_id':2,
                         'seed_table':'experiment'},
                        {'pipeline_name':'DemultiplexingFastq',
                         'seed_id':1,
                         'seed_table':'seqrun'},
                        {'pipeline_name':'DemultiplexingFastq',
                         'seed_id':2,
                         'seed_table':'seqrun'},
                       ]
    update_data=[{'pipeline_name':'PrimaryAnalysis',
                  'seed_id':2,
                  'seed_table':'experiment',
                  'status':'FINISHED'},
                 {'pipeline_name':'DemultiplexingFastq',
                  'seed_id':2,
                  'seed_table':'seqrun',
                  'status':'FINISHED'}]
    pla=PipelineAdaptor(**{'session':base.session})
    pla.store_pipeline_data(data=pipeline_data)
    pla.create_pipeline_seed(data=pipeline_seed_data)
    pla.update_pipeline_seed(update_data)
    base.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_demultiplexing_factory(self):
    pipeseed,seed_list=get_pipeline_seeds(pipeseed_mode='demultiplexing',
                                          pipeline_name='DemultiplexingFastq',
                                          igf_session_class=self.session_class)
    self.assertEqual(len(seed_list.index),1)
    seqrun_igf_id=seed_list['seqrun_igf_id'].values[0]
    self.assertEqual(seqrun_igf_id,'180416_M03291_0139_000000000-TEST')

  def test_analysis_factory(self):
    pipeseed,seed_list=get_pipeline_seeds(pipeseed_mode='alignment',
                                          pipeline_name='PrimaryAnalysis',
                                          igf_session_class=self.session_class)
    self.assertEqual(len(seed_list.index),1)
    experiment_igf_id=seed_list['experiment_igf_id'].values[0]
    self.assertEqual(experiment_igf_id,'IGF00123_MISEQ')


if __name__=='__main__':
  unittest.main()