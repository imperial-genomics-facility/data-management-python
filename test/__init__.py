import os.path, unittest

def get_tests():
  return full_suite()

def full_suite():
  from .process.basesmask_test import BasesMask_testA,BasesMask_testB,BasesMask_testC,BasesMask_testD
  from .process.checksequenceIndexbarcodes_test import CheckSequenceIndexBarcodes_test1
  from .process.collect_seqrun_fastq_to_db_test import Collect_fastq_test1,Collect_fastq_test_sc1
  from .dbadaptor.collectionadaptor_test import CollectionAdaptor_test1,CollectionAdaptor_test2
  from .utils.dbutils_test import Dbutils_test1
  from .process.find_and_process_new_seqrun_test import Find_seqrun_test1
  from .process.flowcell_rules_test import Flowcell_barcode_rule_test1
  from .process.moveBclFilesForDemultiplexing_test import MiSeqRunInfo
  from .process.moveBclFilesForDemultiplexing_test import Hiseq4000RunInfo as Hiseq4000RunInfo_moveBcl
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test1
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test2,Pipelineadaptor_test3
  from .utils.pipelineutils_test import Pipelineutils_test1,Pipelineutils_test2
  from .utils.platformutils_test import Platformutils_test1,Platformutils_test2
  from .dbadaptor.runadaptor_test import RunAdaptor_test1
  from .process.runinfo_xml_test import Hiseq4000RunInfo as Hiseq4000RunInfo_runinfo_xml
  from .process.runparameters_xml_test import Hiseq4000RunParam
  from .process.samplesheet_test import Hiseq4000SampleSheet,TestValidateSampleSheet
  from .process.samplesheet_test import TestValidateSampleSheet1,TestValidateSampleSheet2
  from .process.find_and_register_new_project_data_test import Find_and_register_project_data1
  from .dbadaptor.useradaptor_test import Useradaptor_test1
  from .dbadaptor.sampleadaptor_test import Sampleadaptor_test1
  from .process.processsinglecellsamplesheet_test import ProcessSingleCellSamplesheet_testA
  from .process.mergesinglecellfastq_test import MergeSingleCellFastq_testA
  from .utils.project_data_display_utils_test import Convert_project_data_gviz_data1,Add_seqrun_path_info1
  from .utils.projectutils_test import Projectutils_test1
  from .dbadaptor.fileadaptor_test import Fileadaptor_test1
  from .process.reset_samplesheet_md5_test import Reset_samplesheet_md5_test1
  from .process.modify_pipeline_seed_test import Modify_pipeline_seed_test1
  from .process.experiment_metadata_updator_test import Experiment_metadata_updator_test
  from .dbadaptor.projectadaptor_test import Projectadaptor_test1,Projectadaptor_test2
  from .utils.analysis_collection_utils_test import Analysis_collection_utils_test1
  from .utils.fileutils_test import Fileutils_test1
  from .utils.pipeseedfactory_utils_test import Pipeseedfactory_utils_test1
  from .utils.reference_genome_utils_test import Reference_genome_utils_test1
  from .dbadaptor.experimentadaptor_test import ExperimentAdaptor_test1
  from .utils.picard_util_test import Picard_util_test1,Picard_util_test2
  from .utils.samtools_utils_test import Samtools_util_test1,Samtools_util_test2
  from .utils.deeptools_utils_test import Deeptools_util_test1
  from .utils.ppqt_utils_test import Ppqt_util_test1
  from .dbadaptor.baseadaptor_test import Baseadaptor_test1
  from .dbadaptor.platformadaptor_test import Platformadaptor_test1
  from .utils.metadata_validation_test import Validate_project_and_samplesheet_metadata_test1
  from .utils.metadata_validation_test import Validate_project_and_samplesheet_metadata_test2
  from .utils.project_analysis_utils_test import Project_analysis_test1,Project_analysis_test2
  from .process.project_pooling_info_test import Project_pooling_info_test1
  from .utils.analysis_fastq_fetch_utils_test import Analysis_fastq_fetch_utils_test1

  return unittest.TestSuite([ \
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testA), 
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testB),
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testC),
      unittest.TestLoader().loadTestsFromTestCase(BasesMask_testD),
      unittest.TestLoader().loadTestsFromTestCase(CheckSequenceIndexBarcodes_test1),
      unittest.TestLoader().loadTestsFromTestCase(Collect_fastq_test1),
      unittest.TestLoader().loadTestsFromTestCase(Collect_fastq_test_sc1),
      unittest.TestLoader().loadTestsFromTestCase(CollectionAdaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(CollectionAdaptor_test2),
      unittest.TestLoader().loadTestsFromTestCase(Dbutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Find_seqrun_test1),
      unittest.TestLoader().loadTestsFromTestCase(Flowcell_barcode_rule_test1),
      unittest.TestLoader().loadTestsFromTestCase(MiSeqRunInfo),
      unittest.TestLoader().loadTestsFromTestCase(Hiseq4000RunInfo_moveBcl),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test2),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineadaptor_test3),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineutils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Pipelineutils_test2),
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
      unittest.TestLoader().loadTestsFromTestCase(Picard_util_test2),
      unittest.TestLoader().loadTestsFromTestCase(Ppqt_util_test1),
      unittest.TestLoader().loadTestsFromTestCase(Samtools_util_test1),
      unittest.TestLoader().loadTestsFromTestCase(Samtools_util_test2),
      unittest.TestLoader().loadTestsFromTestCase(Deeptools_util_test1),
      unittest.TestLoader().loadTestsFromTestCase(Baseadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Platformadaptor_test1),
      unittest.TestLoader().loadTestsFromTestCase(Validate_project_and_samplesheet_metadata_test1),
      unittest.TestLoader().loadTestsFromTestCase(Validate_project_and_samplesheet_metadata_test2),
      unittest.TestLoader().loadTestsFromTestCase(Project_analysis_test1),
      unittest.TestLoader().loadTestsFromTestCase(Project_analysis_test2),
      unittest.TestLoader().loadTestsFromTestCase(Project_pooling_info_test1),
      unittest.TestLoader().loadTestsFromTestCase(Analysis_fastq_fetch_utils_test1),
      unittest.TestLoader().loadTestsFromTestCase(Analysis_fastq_fetch_utils_test1),
    ])