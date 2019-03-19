import unittest,os,json
from igf_data.utils.dbutils import read_dbconf_json
from sqlalchemy import create_engine
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.pipelineadaptor import PipelineAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.utils.fileutils import get_temp_dir, remove_dir
from igf_data.process.project_info.project_pooling_info import Project_pooling_info


class Project_pooling_info_test1(unittest.TestCase):
  def setUp(self):
    self.dbconfig='data/dbconfig.json'
    dbparam=read_dbconf_json(self.dbconfig)
    base=BaseAdaptor(**dbparam)
    self.engine=base.engine
    self.dbname=dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class=base.get_session_class()
    # load platform data
    platform_data=\
      [{"platform_igf_id" : "M03291" , 
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
       }]

    flowcell_rule_data=\
      [{"platform_igf_id":"K00345",
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
        "index_2":"NO_CHANGE"}]

    pl=PlatformAdaptor(**{'session_class':base.session_class})
    pl.start_session()
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    pl.close_session()

    # load project data

    project_data=[{'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA'}]
    pa=ProjectAdaptor(**{'session_class':base.session_class})
    pa.start_session()
    pa.store_project_and_attribute_data(data=project_data)
    pa.close_session()

    # load samples

    sample_data=[{'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109792',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109793',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109794',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109795',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109796',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109797',
                  'expected_read':40000000
                 },
                 {'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
                  'sample_igf_id': 'IGF109797_1',
                  'expected_read':40000000
                 },
                ]

    sa=SampleAdaptor(**{'session_class':base.session_class})
    sa.start_session()
    sa.store_sample_and_attribute_data(data=sample_data)
    sa.close_session()

    # load seqrun data

    seqrun_data=[{'flowcell_id': 'HV2GJBBXX',
                  'platform_igf_id': 'K00345',
                  'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX'}]

    sra=SeqrunAdaptor(**{'session_class':base.session_class})
    sra.start_session()
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    sra.close_session()

    # load experiment data

    experiment_data=\
      [{'experiment_igf_id': 'IGF109792_HISEQ4000',
        'library_name': 'IGF109792',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109792',
       },
       {'experiment_igf_id': 'IGF109793_HISEQ4000',
        'library_name': 'IGF109793',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109793',
       },
       {'experiment_igf_id': 'IGF109794_HISEQ4000',
        'library_name': 'IGF109794',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109794',
       },
       {'experiment_igf_id': 'IGF109795_HISEQ4000',
        'library_name': 'IGF109795',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109795',
       },
       {'experiment_igf_id': 'IGF109796_HISEQ4000',
        'library_name': 'IGF109796',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109796',
       },
       {'experiment_igf_id': 'IGF109797_HISEQ4000',
        'library_name': 'IGF109797',
        'platform_name': 'HISEQ4000',
        'project_igf_id': 'IGFQ000472_avik_28-3-2018_RNA',
        'sample_igf_id': 'IGF109797',
       },
      ]

    ea=ExperimentAdaptor(**{'session_class':base.session_class})
    ea.start_session()
    ea.store_project_and_attribute_data(data=experiment_data)
    ea.close_session()

    # load run data

    run_data=\
      [{'experiment_igf_id': 'IGF109792_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': 'IGF109792_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':288046541
       },
       {'experiment_igf_id': 'IGF109793_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': 'IGF109793_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':14666330
       },
       {'experiment_igf_id': 'IGF109794_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': 'IGF109794_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':5009143
       },
       {'experiment_igf_id': 'IGF109795_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': 'IGF109795_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':1391747
       },
       {'experiment_igf_id': 'IGF109796_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': '	IGF109796_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':1318008
       },
       {'experiment_igf_id': 'IGF109797_HISEQ4000',
        'lane_number': '7',
        'run_igf_id': 'IGF109797_HISEQ4000_H2N3MBBXY_7',
        'seqrun_igf_id': '180518_K00345_0047_BHV2GJBBXX',
        'R1_READ_COUNT':1216324
       },
      ]

    ra=RunAdaptor(**{'session_class':base.session_class})
    ra.start_session()
    ra.store_run_and_attribute_data(data=run_data)
    ra.close_session()

  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_fetch_project_info_from_db(self):
    pp = Project_pooling_info(dbconfig_file=self.dbconfig)
    records = pp._fetch_project_info_from_db()
    failed_sample = records[records['sample_igf_id']=='IGF109797_1']
    self.assertEqual(len(failed_sample.index),1)
    failed_sample_read_count = failed_sample['total_read'].values[0]
    self.assertEqual(failed_sample_read_count,0)

  def test_transform_db_data(self):
    pp = Project_pooling_info(dbconfig_file=self.dbconfig)
    records = pp._fetch_project_info_from_db()
    mod_records = pp._transform_db_data(data=records)
    failed_project = mod_records[mod_records['project']=='IGFQ000472_avik_28-3-2018_RNA']
    self.assertEqual(len(failed_project.index),1)
    failed_sample_count = failed_project['fail_samples'].values[0]
    self.assertEqual(failed_sample_count,5)

  def test_fetch_db_data_and_prepare_gviz_json(self):
    pp = Project_pooling_info(dbconfig_file=self.dbconfig)
    temp_dir = get_temp_dir()
    temp_file = os.path.join(temp_dir,'test.json')
    pp.fetch_db_data_and_prepare_gviz_json(output_file_path=temp_file)
    self.assertTrue(os.path.exists(temp_file))
    remove_dir(temp_dir)

if __name__ == '__main__':
  unittest.main()