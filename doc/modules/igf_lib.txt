Sequencing data processing
============================
Metadata processing
--------------------

Register metadata for new projects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.seqrun_processing.find_and_register_new_project_data
   :members:

Update experiment metadata from sample attributes
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.metadata.experiment_metadata_updator
   :members:

Sequencing run
----------------

Process samplesheet file
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.illumina.samplesheet
   :members:

Fetch read cycle info from RunInfo.xml file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.illumina.runinfo_xml
   :members:

Fetch flowcell info from runparameters xml file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.illumina.runparameters_xml
   :members:

Find and process new sequencing run for demultiplexing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.seqrun_processing.find_and_process_new_seqrun
   :members:


Demultiplexing
---------------

Bases mask calculation
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.illumina.basesMask
   :members:

Copy bcl files for demultiplexing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.moveBclFilesForDemultiplexing
   :members:

Collect demultiplexed fastq files to database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db
   :members:

Check demultiplexing barcode stats
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.data_qc.check_sequence_index_barcode
   :members:


Pipeline control
-------------------

Reset pipeline seeds for re-processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.pipeline.modify_pipeline_seed
   :members:

Reset samplesheet files after modification for rerunning pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.seqrun_processing.reset_samplesheet_md5
   :members:


Demultiplexing of single cell sample
-------------------------------------

Modify samplesheet for singlecell samples
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.singlecell_seqrun.processsinglecellsamplesheet
   :members:

Merge fastq files for single cell samples
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.singlecell_seqrun.mergesinglecellfastq
   :members:
