import os.path, unittest

def get_tests():
  return full_suite()

def full_suite():
  from .basesmask_test import BasesMask_testA,BasesMask_testB,BasesMask_testC,BasesMask_testD
  from .checksequenceIndexbarcodes_test import CheckSequenceIndexBarcodes_test1
  from .collect_seqrun_fastq_to_db_test import Collect_fastq_test1,Collect_fastq_test_sc1
  from .collectionadaptor_test import CollectionAdaptor_test1
  from .dbutils_test import Dbutils_test1
  from .find_and_process_new_seqrun_test import Find_seqrun_test1
  from .flowcell_rules_test import Flowcell_barcode_rule_test1
  from .moveBclFilesForDemultiplexing_test import MiSeqRunInfo
  from .moveBclFilesForDemultiplexing_test import Hiseq4000RunInfo as Hiseq4000RunInfo_moveBcl
  from .pipelineadaptor_test import Pipelineadaptor_test1
  from .pipelineadaptor_test import Pipelineadaptor_test2,Pipelineadaptor_test3
  from .pipelineutils_test import Pipelineutils_test1
  from .platformutils_test import Platformutils_test1,Platformutils_test2
  from .runadaptor_test import RunAdaptor_test1
  from .runinfo_xml_test import Hiseq4000RunInfo as Hiseq4000RunInfo_runinfo_xml
  from .runparameters_xml_test import Hiseq4000RunParam
  from .samplesheet_test import Hiseq4000SampleSheet,TestValidateSampleSheet
  from .samplesheet_test import TestValidateSampleSheet1,TestValidateSampleSheet2
  from .find_and_register_new_project_data_test import Find_and_register_project_data1
  from .useradaptor_test import Useradaptor_test1
  from .sampleadaptor_test import Sampleadaptor_test1
  from .processsinglecellsamplesheet_test import ProcessSingleCellSamplesheet_testA
  from .mergesinglecellfastq_test import MergeSingleCellFastq_testA
  from .project_data_display_utils_test import Convert_project_data_gviz_data1,Add_seqrun_path_info1
  from .projectutils_test import Projectutils_test1
  from .fileadaptor_test import Fileadaptor_test1
  from .reset_samplesheet_md5_test import Reset_samplesheet_md5_test1
  from .modify_pipeline_seed_test import Modify_pipeline_seed_test1
  from .experiment_metadata_updator_test import Experiment_metadata_updator_test
  from .projectadaptor_test import Projectadaptor_test1,Projectadaptor_test2
  from .analysis_collection_utils_test import Analysis_collection_utils_test1
  from .fileutils_test import Fileutils_test1
  from .pipeseedfactory_utils_test import Pipeseedfactory_utils_test1
  from .reference_genome_utils_test import Reference_genome_utils_test1
  from .experimentadaptor_test import ExperimentAdaptor_test1
  from .picard_util_test import Picard_util_test1
  from .baseadaptor_test import Baseadaptor_test1

  return unittest.TestSuite([ \
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testA), 
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testB),
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testC),
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testD),
      unittest.TestLoader().loadTestsFromTestCase(CheckSequenceIndexBarcodes_test1),
      unittest.TestLoader().loadTestsFromTestCase(Collect_fastq_test1),
      unittest.TestLoader().loadTestsFromTestCase(Collect_fastq_test_sc1),
      unittest.TestLoader().loadTestsFromTestCase(CollectionAdaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Dbutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Find_seqrun_test1),
      unittest.TestLoader().loadTestsFromTestCase(Flowcell_barcode_rule_test1),
      unittest.TestLoader().loadTestsFromTestCase(MiSeqRunInfo),
      unittest.TestLoader().loadTestsFromTestCase(Hiseq4000RunInfo_moveBcl),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test2),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test3),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Platformutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Platformutils_test2),
      unittest.TestLoader().loadTestsFromTestCase(RunAdaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Hiseq4000RunInfo_runinfo_xml),
      unittest.TestLoader().loadTestsFromTestCase(Hiseq4000RunParam),
      unittest.TestLoader().loadTestsFromTestCase(Hiseq4000SampleSheet),
      unittest.TestLoader().loadTestsFromTestCase(TestValidateSampleSheet),
      unittest.TestLoader().loadTestsFromTestCase(TestValidateSampleSheet1),
      unittest.TestLoader().loadTestsFromTestCase(TestValidateSampleSheet2),
      unittest.TestLoader().loadTestsFromTestCase(Find_and_register_project_data1),
      unittest.TestLoader().loadTestsFromTestCase(Useradaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Sampleadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(ProcessSingleCellSamplesheet_testA),
      unittest.TestLoader().loadTestsFromTestCase(MergeSingleCellFastq_testA),
      unittest.TestLoader().loadTestsFromTestCase(Convert_project_data_gviz_data1),
      unittest.TestLoader().loadTestsFromTestCase(Add_seqrun_path_info1),
      unittest.TestLoader().loadTestsFromTestCase(Projectutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Fileadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Reset_samplesheet_md5_test1),
      unittest.TestLoader().loadTestsFromTestCase(Modify_pipeline_seed_test1),
      unittest.TestLoader().loadTestsFromTestCase(Experiment_metadata_updator_test),
      unittest.TestLoader().loadTestsFromTestCase(Projectadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Projectadaptor_test2),
      unittest.TestLoader().loadTestsFromTestCase(Analysis_collection_utils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Fileutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Pipeseedfactory_utils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Reference_genome_utils_test1),
      unittest.TestLoader().loadTestsFromTestCase(ExperimentAdaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Picard_util_test1),
      unittest.TestLoader().loadTestsFromTestCase(Baseadaptor_test1),
    ])