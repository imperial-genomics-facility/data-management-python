import unittest, json, os, shutil
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db import Collect_seqrun_fastq_to_db

class Collect_fastq_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig = 'data/dbconfig.json'
    self.fastq_dir= 'data/collect_fastq_dir/1_16'
    self.model_name='MISEQ'
    self.flowcell_id='000000000-D0YLK'
    self.file_location='HPC_PROJECT'
    self.samplesheet_file='data/collect_fastq_dir/1_16/SampleSheet.csv'
    self.samplesheet_filename='SampleSheet.csv'
    
    dbparam = None
    with open(self.dbconfig, 'r') as json_data:
      dbparam = json.load(json_data)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
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
 
    project_data=[{'project_igf_id':'IGFP0001_test_22-8-2017_rna',
                   'project_name':'test_22-8-2017_rna',
                   'description':'Its project 1',
                   'project_deadline':'Before August 2017',
                   'comments':'Some samples are treated with drug X',
                 }]
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    
    sample_data=[{'sample_igf_id':'IGF00001',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00002',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00003',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00004',
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                 {'sample_igf_id':'IGF00005', 
                  'project_igf_id':'IGFP0001_test_22-8-2017_rna',},
                ]
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    
    seqrun_data=[{'seqrun_igf_id':'171003_M00001_0089_000000000-TEST',
                  'flowcell_id':'000000000-D0YLK',
                  'platform_igf_id':'M00001',
                  'flowcell':'MISEQ',
                }]
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    base.close_session()
    
  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)
    
  def collect_test1(self):
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  model_name=self.model_name,
                                  flowcell_id=self.flowcell_id,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    ci.find_fastq_and_build_db_collection()
    
if __name__=='__main__':
  unittest.main()