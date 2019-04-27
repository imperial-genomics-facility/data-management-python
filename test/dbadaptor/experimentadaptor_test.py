import os, unittest, sqlalchemy
from sqlalchemy import create_engine
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.igfTables import Base

class ExperimentAdaptor_test1(unittest.TestCase):
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

    base = BaseAdaptor(**{'session_class':self.session_class})
    base.start_session()
    platform_data=[{ "platform_igf_id" : "M001",
                     "model_name" : "MISEQ" ,
                     "vendor_name" : "ILLUMINA" ,
                     "software_name" : "RTA",
                     "software_version" : "RTA1.18.54"}]                        # platform data
    flowcell_rule_data=[{"platform_igf_id":"M001",
                         "flowcell_type":"MISEQ",
                         "index_1":"NO_CHANGE",
                         "index_2":"NO_CHANGE"}]                                # flowcell rule data
    pl=PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)                                  # loading platform data
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)                     # loading flowcell rules data
    project_data=[{'project_igf_id':'ProjectA'}]                                # project data
    pa=ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)                      # load project data
    sample_data=[{'sample_igf_id':'SampleA',
                  'project_igf_id':'ProjectA'}]                                 # sample data
    sa=SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)                        # store sample data
    seqrun_data=[{'seqrun_igf_id':'SeqrunA', 
                  'flowcell_id':'000000000-D0YLK', 
                  'platform_igf_id':'M001',
                  'flowcell':'MISEQ'}]                                          # seqrun data
    sra=SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)                       # load seqrun data
    experiment_data=[{'experiment_igf_id':'ExperimentA',
                      'sample_igf_id':'SampleA',
                      'library_name':'SampleA',
                      'platform_name':'MISEQ',
                      'project_igf_id':'ProjectA'}]                             # experiment data
    ea=ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)                   # load experiment data
    base.commit_session()
    base.close_session()


  def tearDown(self):
    Base.metadata.drop_all(self.engine)
    os.remove(self.dbname)

  def test_check_experiment_records_id(self):
    ea = ExperimentAdaptor(**{'session_class':self.session_class})
    ea.start_session()
    self.assertTrue(ea.check_experiment_records_id(experiment_igf_id='ExperimentA'))
    self.assertFalse(ea.check_experiment_records_id(experiment_igf_id='ExperimentB'))
    ea.close_session()

  def test_fetch_project_and_sample_for_experiment(self):
    ea = ExperimentAdaptor(**{'session_class':self.session_class})
    ea.start_session()
    project_id,sample_id=ea.fetch_project_and_sample_for_experiment(experiment_igf_id='ExperimentA')
    self.assertEqual(project_id,'ProjectA')
    self.assertEqual(sample_id,'SampleA')
    ea.close_session()

if __name__=='__main__':
  unittest.main()