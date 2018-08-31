Sequencing data processing
==========================

Metadata processing
--------------------

Register metadata for new projects
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.process.seqrun_processing.find_and_register_new_project_data
   :members:


Sequencing run
----------------

Find and process new sequencing run for demultiplexing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.process.seqrun_processing.find_and_process_new_seqrun
   :members:


Fetch read cycle info from RunInfo.xml file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.illumina.runinfo_xml
   :members:

Fetch flowcell info from runparameters xml file
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.illumina.runparameters_xml
   :members:


Demultiplexing
---------------

Bases mask calculation
^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.illumina.basesMask
   :members:


Collect demultiplexed fastq files to database
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. automodule:: igf_data.process.seqrun_processing.collect_seqrun_fastq_to_db
   :members:

Demultiplexing pipeline control
--------------------------------

Reset samplesheet files after modification for rerunning pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.process.seqrun_processing.reset_samplesheet_md5
   :members:



