import unittest,os,json
import pandas as pd
from igf_data.utils.dbutils import read_dbconf_json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.process.pipeline.modify_pipeline_seed import Modify_pipeline_seed


class Modify_pipeline_seed_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    base.start_session()
    platform_data=[{ "platform_igf_id" : "M00001" ,
                     "model_name" : "MISEQ" ,
                     "vendor_name" : "ILLUMINA" ,
                     "software_name" : "RTA" ,
                     "software_version" : "RTA1.18.54"
                   },
                   { "platform_igf_id" : "NB500000",
                     "model_name" : "NEXTSEQ",
                     "vendor_name" : "ILLUMINA",
                     "software_name" : "RTA",
                     "software_version" : "RTA2"
                   },
                   { "platform_igf_id" : "K00000",
                     "model_name" : "HISEQ4000",
                     "vendor_name" : "ILLUMINA",
                     "software_name" : "RTA",
                     "software_version" : "RTA2"
                   }]
    flowcell_rule_data=[{"platform_igf_id":"K00000",
                         "flowcell_type":"HiSeq 3000/4000 SR",
                         "index_1":"NO_CHANGE",
                         "index_2":"NO_CHANGE"},
                        {"platform_igf_id":"K00000",
                         "flowcell_type":"HiSeq 3000/4000 PE",
                         "index_1":"NO_CHANGE",
                         "index_2":"REVCOMP"},
                        {"platform_igf_id":"NB500000",
                         "flowcell_type":"NEXTSEQ",
                         "index_1":"NO_CHANGE",
                         "index_2":"REVCOMP"},
                        {"platform_igf_id":"M00001",
                         "flowcell_type":"MISEQ",
                         "index_1":"NO_CHANGE",
                         "index_2":"NO_CHANGE"}
                        ]
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data=[{'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
                  'flowcell_id':'000000000-D0YLK',
                  'platform_igf_id':'M00001',
                  'flowcell':'MISEQ',
                }]
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    seqrun=sra.fetch_seqrun_records_igf_id(seqrun_igf_id='171003_M00001_0089_000000000-TEST')
    pipeline_data=[{"pipeline_name" : "demultiplexing_fastq",
                    "pipeline_db" : "sqlite:////data/bcl2fastq.db", 
                    "pipeline_init_conf" : { "input_dir":"data/seqrun_dir/" ,
                                            "output_dir" : "data"}, 
                    "pipeline_run_conf" : { "output_dir" : "data" }
                  }]
    pipeseed_data=[{"pipeline_name" : "demultiplexing_fastq",
                    "seed_table":"seqrun",
                    "seed_id":seqrun.seqrun_id}]
    pp=PipelineAdaptor(**{'session': base.session})
    pp.store_pipeline_data(data=pipeline_data)
    pp.create_pipeline_seed(data=pipeseed_data,required_columns=['pipeline_id', 'seed_id', 'seed_table'])
    base.close_session()
    self.seqrun_input_list='data/reset_samplesheet_md5/seqrun_pipeline_reset_list.txt'
    with open(self.seqrun_input_list,'w') as fp:
      fp.write('')

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    os.remove(self.seqrun_input_list)

  def test_reset_pipeline_seed_for_rerun(self):
    base=BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    sra=SeqrunAdaptor(**{'session':base.session})
    seqrun=sra.fetch_seqrun_records_igf_id(seqrun_igf_id='171003_M00001_0089_000000000-TEST')
    pp=PipelineAdaptor(**{'session': base.session})
    pipeline=pp.fetch_pipeline_records_pipeline_name('demultiplexing_fastq')
    pipe_seed=pp.fetch_pipeline_seed(pipeline_id=pipeline.pipeline_id,
                                     seed_id=seqrun.seqrun_id,
                                     seed_table='seqrun')
    self.assertEqual(pipe_seed.status, 'SEEDED')
    pp.update_pipeline_seed(data=[{'pipeline_id':pipeline.pipeline_id,
                                   'seed_id':seqrun.seqrun_id,
                                   'seed_table':'seqrun',
                                   'status':'FINISHED',
                                   }])
    pipe_seed2=pp.fetch_pipeline_seed(pipeline_id=pipeline.pipeline_id,
                                     seed_id=seqrun.seqrun_id,
                                     seed_table='seqrun')
    self.assertEqual(pipe_seed2.status, 'FINISHED')
    base.close_session()

    with open(self.seqrun_input_list,'w') as fp:
      fp.write('171003_M00001_0089_000000000-TEST')

    mps=Modify_pipeline_seed(igf_id_list=self.seqrun_input_list,
                             table_name='seqrun',
                             pipeline_name='demultiplexing_fastq',
                             dbconfig_file=self.dbconfig,
                             log_slack=False,
                             log_asana=False,
                             clean_up=True
                             )
    mps.reset_pipeline_seed_for_rerun(seeded_label='SEEDED')

    base.start_session()
    sra=SeqrunAdaptor(**{'session':base.session})
    seqrun=sra.fetch_seqrun_records_igf_id(seqrun_igf_id='171003_M00001_0089_000000000-TEST')
    pp=PipelineAdaptor(**{'session': base.session})
    pipeline=pp.fetch_pipeline_records_pipeline_name('demultiplexing_fastq')
    pipe_seed=pp.fetch_pipeline_seed(pipeline_id=pipeline.pipeline_id,
                                     seed_id=seqrun.seqrun_id,
                                     seed_table='seqrun')
    self.assertEqual(pipe_seed.status, 'SEEDED')
    base.close_session()

if __name__ == '__main__':
  unittest.main()