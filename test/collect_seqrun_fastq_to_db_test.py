import unittest, json, os, shutil
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db import Collect_seqrun_fastq_to_db

class Collect_fastq_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    self.fastq_dir='data/collect_fastq_dir/1_16'
    self.model_name='MISEQ'
    self.flowcell_id='000000000-D0YLK'
    self.seqrun_igf_id='171003_M00001_0089_000000000-TEST'
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
    self.session_class=base.session_class
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
    
  def test__get_fastq_and_samplesheet(self):
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  seqrun_igf_id=self.seqrun_igf_id,
                                  flowcell_id=self.flowcell_id,
                                  model_name=self.model_name,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    (r1_fastq_list, r2_fastq_list)=ci._get_fastq_and_samplesheet()
    self.assertEqual(len(r1_fastq_list), 5)
    self.assertEqual(len(r2_fastq_list), 0)
    
  def test_collect_fastq_and_sample_info(self):
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  seqrun_igf_id=self.seqrun_igf_id,
                                  flowcell_id=self.flowcell_id,
                                  model_name=self.model_name,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    fastq_files_list=ci._collect_fastq_and_sample_info()
    for fastq_file in fastq_files_list:
      if fastq_file['sample_igf_id']=='IGF00002':
        self.assertEqual(fastq_file['R1'],\
        'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00002/IGF00002-2_S1_L001_R1_001.fastq.gz')
    
  def test_find_fastq_and_build_db_collection(self):
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  seqrun_igf_id=self.seqrun_igf_id,
                                  flowcell_id=self.flowcell_id,
                                  model_name=self.model_name,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    ci.find_fastq_and_build_db_collection()
    ca=CollectionAdaptor(**{'session_class':self.session_class})
  
  def test_calculate_experiment_run_and_file_info(self):
    data={'lane_number': '1', 
           'seqrun_igf_id': '171003_M00001_0089_000000000-TEST', 
           'description': '', 
           'project_igf_id': 'IGFP0001_test_22-8-2017_rna', 
           'sample_igf_id': 'IGF00001', 
           'sample_name': 'IGF00001-1', 
           'R1': 'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00001/IGF00001-1_S1_L001_R1_001.fastq.gz',
           'platform_name': 'MISEQ',
           'flowcell_id': '000000000-D0YLK'}
    
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  seqrun_igf_id=self.seqrun_igf_id,
                                  flowcell_id=self.flowcell_id,
                                  model_name=self.model_name,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    data=ci._calculate_experiment_run_and_file_info(data=data,restricted_list=['10X'])
    data=data.to_dict()
    self.assertEqual(data['experiment_igf_id'],'IGF00001_MISEQ')
    self.assertEqual(data['run_igf_id'],'IGF00001_MISEQ_000000000-D0YLK_1')
    self.assertEqual(data['name'],'IGF00001_MISEQ_000000000-D0YLK_1')
    self.assertEqual(data['type'],'demultiplexed_fastq')

  def test_reformat_file_group_data(self):
    ci=Collect_seqrun_fastq_to_db(fastq_dir=self.fastq_dir,
                                  session_class=self.session_class,
                                  seqrun_igf_id=self.seqrun_igf_id,
                                  flowcell_id=self.flowcell_id,
                                  model_name=self.model_name,
                                  file_location=self.file_location,
                                  samplesheet_file=self.samplesheet_file,
                                 )
    data=[{'location': 'HPC_PROJECT',
          'sample_igf_id': 'IGF00001', 
          'sample_name': 'IGF00001-1',
          'lane_number': '1', 
          'run_igf_id': 'IGF00001_MISEQ_000000000-D0YLK_1', 
          'R1_md5': '767adf23d195c31b014c875a2cdd191f', 
          'experiment_igf_id': 'IGF00001_MISEQ', 
          'project_igf_id': 'IGFP0001_test_22-8-2017_rna',
          'flowcell_id': '000000000-D0YLK',
          'R1_size': 34,
          'type': 'demultiplexed_fastq',
          'description': '',
          'seqrun_igf_id': '171003_M00001_0089_000000000-TEST',
          'library_name': 'IGF00001',
          'library_layout': 'SINGLE',
          'platform_name': 'MISEQ',
          'name': 'IGF00001_MISEQ_000000000-D0YLK_1',
          'R1': 'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00001/IGF00001-1_S1_L001_R1_001.fastq.gz'}]
    (file_data,file_group_data)=ci._reformat_file_group_data(data=data)
    file_data=file_data.to_dict(orient='records')
    file_data=file_data[0]
    self.assertEqual(file_data['md5'],'767adf23d195c31b014c875a2cdd191f')
    self.assertEqual(file_data['file_path'],'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00001/IGF00001-1_S1_L001_R1_001.fastq.gz')
    file_group_data=file_group_data.to_dict(orient='records')
    file_group_data=file_group_data[0]
    self.assertEqual(file_group_data['file_path'],'data/collect_fastq_dir/1_16/IGFP0001_test_22-8-2017_rna/IGF00001/IGF00001-1_S1_L001_R1_001.fastq.gz')
    self.assertEqual(file_group_data['name'],'IGF00001_MISEQ_000000000-D0YLK_1')

if __name__=='__main__':
  unittest.main()