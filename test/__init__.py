import os.path, unittest

def get_tests():
  return full_suite()

def full_suite():
  from .process.basesmask_test import BasesMask_testA
  from .process.basesmask_test import BasesMask_testB
  from .process.basesmask_test import BasesMask_testC
  from .process.basesmask_test import BasesMask_testD
  from .process.checksequenceIndexbarcodes_test import CheckSequenceIndexBarcodes_test1
  from .process.collect_seqrun_fastq_to_db_test import Collect_fastq_test1
  from .process.collect_seqrun_fastq_to_db_test import Collect_fastq_test_sc1
  from .dbadaptor.collectionadaptor_test import CollectionAdaptor_test1
  from .dbadaptor.collectionadaptor_test import CollectionAdaptor_test2
  from .dbadaptor.collectionadaptor_test import CollectionAdaptor_test3
  from .utils.dbutils_test import Dbutils_test1
  from .process.find_and_process_new_seqrun_test import Find_seqrun_test1
  from .process.flowcell_rules_test import Flowcell_barcode_rule_test1
  from .process.moveBclFilesForDemultiplexing_test import MiSeqRunInfo
  from .process.moveBclFilesForDemultiplexing_test import Hiseq4000RunInfo as Hiseq4000RunInfo_moveBcl
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test1
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test2
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test3
  from .dbadaptor.pipelineadaptor_test import Pipelineadaptor_test4
  from .utils.pipelineutils_test import Pipelineutils_test1,Pipelineutils_test2
  from .utils.platformutils_test import Platformutils_test1,Platformutils_test2
  from .dbadaptor.runadaptor_test import RunAdaptor_test1
  from .process.runinfo_xml_test import Hiseq4000RunInfo as Hiseq4000RunInfo_runinfo_xml
  from .process.runparameters_xml_test import Hiseq4000RunParam
  from .process.samplesheet_test import Hiseq4000SampleSheet
  from .process.samplesheet_test import TestValidateSampleSheet
  from .process.samplesheet_test import SampleSheet_format_v2_test1
  from .process.samplesheet_test import TestValidateSampleSheet1
  from .process.samplesheet_test import TestValidateSampleSheet2
  from .process.find_and_register_new_project_data_test import Find_and_register_project_data1
  from .dbadaptor.useradaptor_test import Useradaptor_test1
  from .dbadaptor.sampleadaptor_test import Sampleadaptor_test1
  from .dbadaptor.sampleadaptor_test import Sampleadaptor_test2
  from .dbadaptor.sampleadaptor_test import Sampleadaptor_test3
  from .dbadaptor.sampleadaptor_test import Sampleadaptor_test4
  from .process.processsinglecellsamplesheet_test import ProcessSingleCellSamplesheet_testA
  from .process.processsinglecellsamplesheet_test import ProcessSingleCellSamplesheet_testB
  from .process.processsinglecellsamplesheet_test import ProcessSingleCellDualIndexSamplesheetA
  from .process.mergesinglecellfastq_test import MergeSingleCellFastq_testA
  from .process.mergesinglecellfastq_test import MergeSingleCellFastq_testB
  from .utils.project_data_display_utils_test import Convert_project_data_gviz_data1
  from .utils.project_data_display_utils_test import Add_seqrun_path_info1
  from .utils.projectutils_test import Projectutils_test1
  #from .utils.projectutils_test import Projectutils_test2
  from .dbadaptor.fileadaptor_test import Fileadaptor_test1
  from .process.reset_samplesheet_md5_test import Reset_samplesheet_md5_test1
  from .process.modify_pipeline_seed_test import Modify_pipeline_seed_test1
  from .process.experiment_metadata_updator_test import Experiment_metadata_updator_test
  from .dbadaptor.projectadaptor_test import Projectadaptor_test1
  from .dbadaptor.projectadaptor_test import Projectadaptor_test2
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
  from .utils.project_analysis_utils_test import Project_analysis_test1
  from .utils.project_analysis_utils_test import Project_analysis_test2
  from .utils.project_analysis_utils_test import Project_analysis_test3
  from .process.project_pooling_info_test import Project_pooling_info_test1
  from .utils.analysis_fastq_fetch_utils_test import Analysis_fastq_fetch_utils_test1
  from .utils.analysis_fastq_fetch_utils_test import Analysis_fastq_fetch_utils_test2
  from .process.reformat_metadata_file_test import Reformat_metadata_file_testA
  from .process.reformat_samplesheet_file_test import Reformat_samplesheet_file_testA
  from .utils.singularity_run_wrapper_test import Singularity_run_test1
  from .utils.jupyter_nbconvert_wrapper_test import Nbconvert_execute_test1
  from .igf_airflow.calculate_seqrun_file_size_test import Calculate_seqrun_file_list_testA
  from .igf_airflow.ongoing_seqrun_processing_test import Compare_existing_seqrun_filesA
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestA
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestB
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestC
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestD
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestE
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestF
  from .igf_airflow.dag9_tenx_single_cell_immune_profiling_utils_test import Dag9_tenx_single_cell_immune_profiling_utilstestG
  from .igf_airflow.dag10_nextflow_atacseq_pipeline_utils_test import Dag10_nextflow_atacseq_pipeline_utils_testA
  from .igf_airflow.dag10_nextflow_atacseq_pipeline_utils_test import Dag10_nextflow_atacseq_pipeline_utils_testB
  from .igf_airflow.dag17_create_transcriptome_ref_utils_test import Dag17_create_transcriptome_ref_utils_test_utilstestA
  from .igf_airflow.dag18_upload_and_trigger_analysis_utils_test import Dag18_upload_and_trigger_analysis_utils_testA
  from .igf_airflow.dag18_upload_and_trigger_analysis_utils_test import Dag18_upload_and_trigger_analysis_utils_testB
  from .igf_airflow.dag18_upload_and_trigger_analysis_utils_test import Dag18_upload_and_trigger_analysis_utils_testC
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testA
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testB
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testC
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testD
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testE
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testF
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testG
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testH
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testI
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testJ
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testK
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testL
  from .igf_airflow.dag22_bclconvert_demult_utils_test import Dag22_bclconvert_demult_utils_testM
  from .utils.cellranger_count_utils_test import Cellranger_count_utils_testA
  from .dbadaptor.analysisadaptor_test import Analysisadaptor_test1
  from .dbadaptor.seqrunadaptor_test import SeqrunAdaptor_test1
  from .utils.samplesheet_utils_test import SamplesheetUtils_testA
  from .igf_nextflow.nextflow_config_formatter_test import Nextflow_config_formatter_testA
  from .igf_nextflow.nextflow_design_test import Nextflow_design_testA
  from .igf_nextflow.nextflow_runner_test import Nextflow_pre_run_setup_testA
  from .igf_portal.test_metadata_utils import Metadata_dump_test, Metadata_load_test
  from .process.find_and_process_new_project_data_from_portal_db_test import Find_and_register_new_project_data_from_portal_db_test1
  from .igf_airflow.dag23_test_bclconvert_demult_utils_test import Dag23_test_bclconvert_demult_utils_testA
  from .igf_airflow.dag25_copy_seqruns_to_hpc_utils_test import TestDag25_copy_seqruns_to_hpc_utilsA
  from .igf_airflow.dag25_copy_seqruns_to_hpc_utils_test import TestDag25_copy_seqruns_to_hpc_utilsB
  from .igf_airflow.dag26_snakemake_rnaseq_utils_test import TestDag26_snakemake_rnaseq_utilsA
  from .igf_airflow.dag26_snakemake_rnaseq_utils_test import TestDag26_snakemake_rnaseq_utilsB
  from .igf_airflow.dag26_snakemake_rnaseq_utils_test import TestDag26_snakemake_rnaseq_utilsC
  from .igf_airflow.dag27_cleanup_demultiplexing_output_utils_test import TestDag27_cleanup_demultiplexing_output_utilsA
  from .igf_airflow.test_dag33_geomx_processing_util import (
    TestDag33_geomx_processing_util_utilsA,
    TestDag33_geomx_processing_util_utilsB,
    TestDag33_geomx_processing_util_utilsC)

  return unittest.TestSuite([
    unittest.TestLoader().loadTestsFromTestCase(t)
      for t in [
        BasesMask_testA, 
        BasesMask_testB,
        BasesMask_testC,
        BasesMask_testD,
        CheckSequenceIndexBarcodes_test1,
        Collect_fastq_test1,
        Collect_fastq_test_sc1,
        CollectionAdaptor_test1,
        CollectionAdaptor_test2,
        CollectionAdaptor_test3,
        Dbutils_test1,
        Find_seqrun_test1,
        Flowcell_barcode_rule_test1,
        MiSeqRunInfo,
        Hiseq4000RunInfo_moveBcl,
        Pipelineadaptor_test1,
        Pipelineadaptor_test2,
        Pipelineadaptor_test3,
        Pipelineadaptor_test4,
        Pipelineutils_test1,
        Pipelineutils_test2,
        Platformutils_test1,
        Platformutils_test2,
        RunAdaptor_test1,
        Hiseq4000RunInfo_runinfo_xml,
        Hiseq4000RunParam,
        Hiseq4000SampleSheet,
        SampleSheet_format_v2_test1,
        TestValidateSampleSheet,
        TestValidateSampleSheet1,
        TestValidateSampleSheet2,
        Find_and_register_project_data1,
        Useradaptor_test1,
        Sampleadaptor_test1,
        Sampleadaptor_test2,
        Sampleadaptor_test3,
        Sampleadaptor_test4,
        ProcessSingleCellSamplesheet_testA,
        ProcessSingleCellSamplesheet_testB,
        ProcessSingleCellDualIndexSamplesheetA,
        MergeSingleCellFastq_testA,
        MergeSingleCellFastq_testB,
        Convert_project_data_gviz_data1,
        Add_seqrun_path_info1,
        Projectutils_test1,
        #Projectutils_test2,
        Fileadaptor_test1,
        Reset_samplesheet_md5_test1,
        Modify_pipeline_seed_test1,
        Experiment_metadata_updator_test,
        Projectadaptor_test1,
        Projectadaptor_test2,
        Analysis_collection_utils_test1,
        Fileutils_test1,
        Pipeseedfactory_utils_test1,
        Reference_genome_utils_test1,
        ExperimentAdaptor_test1,
        Picard_util_test1,
        Picard_util_test2,
        Ppqt_util_test1,
        Samtools_util_test1,
        Samtools_util_test2,
        Deeptools_util_test1,
        Baseadaptor_test1,
        Platformadaptor_test1,
        Validate_project_and_samplesheet_metadata_test1,
        Validate_project_and_samplesheet_metadata_test2,
        Project_analysis_test1,
        Project_analysis_test2,
        Project_analysis_test3,
        Project_pooling_info_test1,
        Analysis_fastq_fetch_utils_test1,
        Analysis_fastq_fetch_utils_test2,
        Reformat_metadata_file_testA,
        Reformat_samplesheet_file_testA,
        Singularity_run_test1,
        Nbconvert_execute_test1,
        Calculate_seqrun_file_list_testA,
        Compare_existing_seqrun_filesA,
        Dag9_tenx_single_cell_immune_profiling_utilstestA,
        Dag9_tenx_single_cell_immune_profiling_utilstestB,
        Dag9_tenx_single_cell_immune_profiling_utilstestC,
        Dag9_tenx_single_cell_immune_profiling_utilstestD,
        Dag9_tenx_single_cell_immune_profiling_utilstestE,
        Dag9_tenx_single_cell_immune_profiling_utilstestF,
        Dag9_tenx_single_cell_immune_profiling_utilstestG,
        Cellranger_count_utils_testA,
        Analysisadaptor_test1,
        SeqrunAdaptor_test1,
        SamplesheetUtils_testA,
        Nextflow_config_formatter_testA,
        Nextflow_design_testA,
        Nextflow_pre_run_setup_testA,
        Dag10_nextflow_atacseq_pipeline_utils_testA,
        Dag10_nextflow_atacseq_pipeline_utils_testB,
        Dag17_create_transcriptome_ref_utils_test_utilstestA,
        Dag18_upload_and_trigger_analysis_utils_testA,
        Dag18_upload_and_trigger_analysis_utils_testB,
        Dag18_upload_and_trigger_analysis_utils_testC,
        Metadata_dump_test,
        Metadata_load_test,
        Find_and_register_new_project_data_from_portal_db_test1,
        Dag22_bclconvert_demult_utils_testA,
        Dag22_bclconvert_demult_utils_testB,
        Dag22_bclconvert_demult_utils_testC,
        Dag22_bclconvert_demult_utils_testD,
        Dag22_bclconvert_demult_utils_testE,
        Dag22_bclconvert_demult_utils_testF,
        Dag22_bclconvert_demult_utils_testG,
        Dag22_bclconvert_demult_utils_testH,
        Dag22_bclconvert_demult_utils_testI,
        Dag22_bclconvert_demult_utils_testJ,
        Dag22_bclconvert_demult_utils_testK,
        Dag22_bclconvert_demult_utils_testL,
        Dag22_bclconvert_demult_utils_testM,
        Dag23_test_bclconvert_demult_utils_testA,
        TestDag26_snakemake_rnaseq_utilsA,
        TestDag26_snakemake_rnaseq_utilsB,
        TestDag26_snakemake_rnaseq_utilsC,
        TestDag25_copy_seqruns_to_hpc_utilsA,
        TestDag25_copy_seqruns_to_hpc_utilsB,
        TestDag27_cleanup_demultiplexing_output_utilsA,
        TestDag33_geomx_processing_util_utilsA,
        TestDag33_geomx_processing_util_utilsB,
        TestDag33_geomx_processing_util_utilsC
      ]
    ])