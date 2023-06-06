import os
import unittest
from igf_data.utils.fileutils import get_temp_dir
from igf_data.utils.fileutils import remove_dir
from igf_data.utils.dbutils import read_dbconf_json
from igf_data.igfdb.igfTables import Base
from igf_data.igfdb.baseadaptor import BaseAdaptor
from igf_data.igfdb.platformadaptor import PlatformAdaptor
from igf_data.igfdb.seqrunadaptor import SeqrunAdaptor
from igf_data.igfdb.projectadaptor import ProjectAdaptor
from igf_data.igfdb.sampleadaptor import SampleAdaptor
from igf_data.igfdb.experimentadaptor import ExperimentAdaptor
from igf_data.igfdb.runadaptor import RunAdaptor
from igf_data.igfdb.collectionadaptor import CollectionAdaptor
from igf_data.igfdb.fileadaptor import FileAdaptor
from igf_airflow.utils.dag27_cleanup_demultiplexing_output_utils import cleanup_existing_data_for_flowcell_and_project

class TestDag27_cleanup_demultiplexing_output_utilsA(unittest.TestCase):
  def setUp(self):
    self.temp_dir = get_temp_dir()
    self.dbconfig = 'data/dbconfig.json'
    dbparam = read_dbconf_json(self.dbconfig)
    base = BaseAdaptor(**dbparam)
    self.engine = base.engine
    self.dbname = dbparam['dbname']
    Base.metadata.create_all(self.engine)
    self.session_class = base.get_session_class()
    base.start_session()
    platform_data = [{
      "platform_igf_id" : "M03291",
      "model_name" : "MISEQ",
      "vendor_name" : "ILLUMINA",
      "software_name" : "RTA",
      "software_version" : "RTA1.18.54"}]
    flowcell_rule_data = [{
      "platform_igf_id": "M03291",
      "flowcell_type": "MISEQ",
      "index_1": "NO_CHANGE",
      "index_2": "NO_CHANGE"}]
    pl = PlatformAdaptor(**{'session':base.session})
    pl.store_platform_data(data=platform_data)
    pl.store_flowcell_barcode_rule(data=flowcell_rule_data)
    seqrun_data = [{
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'flowcell_id': '000000000-BRN47',
      'platform_igf_id': 'M03291',
      'flowcell': 'MISEQ'}]
    sra = SeqrunAdaptor(**{'session':base.session})
    sra.store_seqrun_and_attribute_data(data=seqrun_data)
    project_data = [{'project_igf_id':'projectA'}]
    pa = ProjectAdaptor(**{'session':base.session})
    pa.store_project_and_attribute_data(data=project_data)
    sample_data = [{
      'sample_igf_id': 'sampleA',
      'project_igf_id': 'projectA',
      'species_name': 'HG38'
    }]
    sa = SampleAdaptor(**{'session':base.session})
    sa.store_sample_and_attribute_data(data=sample_data)
    experiment_data = [{
      'project_igf_id': 'projectA',
      'sample_igf_id': 'sampleA',
      'experiment_igf_id': 'sampleA_MISEQ',
      'library_name': 'sampleA',
      'library_source': 'TRANSCRIPTOMIC',
      'library_strategy': 'RNA-SEQ',
      'experiment_type': 'POLYA-RNA',
      'library_layout': 'PAIRED',
      'platform_name': 'MISEQ',
    }]
    ea = ExperimentAdaptor(**{'session':base.session})
    ea.store_project_and_attribute_data(data=experiment_data)
    run_data = [{
      'experiment_igf_id': 'sampleA_MISEQ',
      'seqrun_igf_id': '180416_M03291_0139_000000000-BRN47',
      'run_igf_id': 'sampleA_MISEQ_000000000-BRN47_1',
      'lane_number': '1'
    }]
    ra = RunAdaptor(**{'session':base.session})
    ra.store_run_and_attribute_data(data=run_data)
    ## file list
    ## run level files, create both collection and file
    ## use run id as collection name
    ##
    ## * demultiplexed_fastq
    ## * FASTQC_HTML_REPORT
    ## * FASTQSCREEN_HTML_REPORT
    ## 
    ## FTP files, just create collection
    ## * FTP_FASTQC_HTML_REPORT
    ## * FTP_FASTQSCREEN_HTML_REPORT
    ##
    ## Flowcell - lane - index group level, create both file and collection
    ## use project_igf_id_flowcell_id as collection prefix
    ##
    ## * DEMULTIPLEXING_REPORT_HTML
    ## * MULTIQC_HTML_REPORT_KNOWN
    ## * MULTIQC_HTML_REPORT_UNDETERMINED
    ## * DEMULTIPLEXING_REPORT_DIR
    ##
    ## FTP files, just create collection
    ## * FTP_DEMULTIPLEXING_REPORT_HTML
    ## * FTP_MULTIQC_HTML_REPORT_KNOWN
    ## * FTP_MULTIQC_HTML_REPORT_UNDETERMINED
    ##
    os.makedirs(
      os.path.join(self.temp_dir, 'demult_dir'))
    file_data = [
      {'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastq.gz')},
      {'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastqc.html')},
      {'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastqscreen.html')},
      {'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.ftp.fastqc.html')},
      {'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.ftp.fastqscreen.html')},
      {'file_path': os.path.join(self.temp_dir, 'demult.html')},
      {'file_path': os.path.join(self.temp_dir, 'multiqc.html')},
      {'file_path': os.path.join(self.temp_dir, 'multiqc_unknown.html')},
      {'file_path': os.path.join(self.temp_dir, 'demult_dir')},
      {'file_path': os.path.join(self.temp_dir, 'demult.ftp.html')},
      {'file_path': os.path.join(self.temp_dir, 'multiqc.ftp.html')},
      {'file_path': os.path.join(self.temp_dir, 'multiqc_unknown.ftp.html')}]
    for entry in file_data:
      for _, filepath in entry.items():
        if filepath != os.path.join(self.temp_dir, 'demult_dir'):
          with open(filepath, 'w') as fp:
            fp.write('A')
    fa = FileAdaptor(**{'session':base.session})
    fa.store_file_and_attribute_data(
      data=file_data)
    collection_data = [{
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'demultiplexed_fastq',
      'table': 'run'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'FASTQC_HTML_REPORT',
      'table': 'run'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'FASTQSCREEN_HTML_REPORT',
      'table': 'run'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'FTP_FASTQC_HTML_REPORT',
      'table': 'run'
    }, {
      'name': 'sampleA_MISEQ_000000000-BRN47_1',
      'type': 'FTP_FASTQSCREEN_HTML_REPORT',
      'table': 'run'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'DEMULTIPLEXING_REPORT_HTML',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'MULTIQC_HTML_REPORT_KNOWN',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'MULTIQC_HTML_REPORT_UNDETERMINED',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'DEMULTIPLEXING_REPORT_DIR',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'FTP_DEMULTIPLEXING_REPORT_HTML',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'FTP_MULTIQC_HTML_REPORT_KNOWN',
      'table': 'file'
    }, {
      'name': 'projectA_000000000-BRN47_1',
      'type': 'FTP_MULTIQC_HTML_REPORT_UNDETERMINED',
      'table': 'file'
    }]
    collection_files_data = [
      {'name': 'sampleA_MISEQ_000000000-BRN47_1',
       'type': 'demultiplexed_fastq',
       'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastq.gz')},
      {'name': 'sampleA_MISEQ_000000000-BRN47_1',
       'type': 'FASTQC_HTML_REPORT',
       'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastqc.html')},
      {'name': 'sampleA_MISEQ_000000000-BRN47_1',
       'type': 'FASTQSCREEN_HTML_REPORT',
       'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.fastqscreen.html')},
      {'name': 'sampleA_MISEQ_000000000-BRN47_1',
       'type': 'FTP_FASTQC_HTML_REPORT',
       'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.ftp.fastqc.html')},
      {'name': 'sampleA_MISEQ_000000000-BRN47_1',
       'type': 'FTP_FASTQSCREEN_HTML_REPORT',
       'file_path': os.path.join(self.temp_dir, 'sampleA_S1_L001_R1_001.ftp.fastqscreen.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'DEMULTIPLEXING_REPORT_HTML',
       'file_path': os.path.join(self.temp_dir, 'demult.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'MULTIQC_HTML_REPORT_KNOWN',
       'file_path': os.path.join(self.temp_dir, 'multiqc.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'MULTIQC_HTML_REPORT_UNDETERMINED',
       'file_path': os.path.join(self.temp_dir, 'multiqc_unknown.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'DEMULTIPLEXING_REPORT_DIR',
       'file_path': os.path.join(self.temp_dir, 'demult_dir')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'FTP_DEMULTIPLEXING_REPORT_HTML',
       'file_path': os.path.join(self.temp_dir, 'demult.ftp.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'FTP_MULTIQC_HTML_REPORT_KNOWN',
       'file_path': os.path.join(self.temp_dir, 'multiqc.ftp.html')},
      {'name': 'projectA_000000000-BRN47_1',
       'type': 'FTP_MULTIQC_HTML_REPORT_UNDETERMINED',
       'file_path': os.path.join(self.temp_dir, 'multiqc_unknown.ftp.html')}]
    ca = CollectionAdaptor(**{'session':base.session})
    ca.store_collection_and_attribute_data(data=collection_data)
    ca.create_collection_group(data=collection_files_data)
    base.close_session()


  def tearDown(self):
    remove_dir(self.temp_dir)
    Base.metadata.drop_all(self.engine)
    if os.path.exists(self.dbname):
      os.remove(self.dbname)

  def test_cleanup_existing_data_for_flowcell_and_project(self):
    self.assertEqual(len(os.listdir(self.temp_dir)), 12)
    status = \
      cleanup_existing_data_for_flowcell_and_project(
        dbconfig_file=self.dbconfig,
        seqrun_igf_id='180416_M03291_0139_000000000-BRN47',
        project_igf_id='projectA')
    self.assertEqual(len(os.listdir(self.temp_dir)), 5)

if __name__=='__main__':
  unittest.main()