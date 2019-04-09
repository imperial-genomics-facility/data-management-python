IGF database schema and api
============================

Database schema
-----------------
.. automodule:: igf_data.igfdb.igfTables
   :members:

Database adaptor api
--------------------

Base adaptor
^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.baseadaptor
   :members:


Project adaptor
^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.projectadaptor
   :members:


User adaptor
^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.useradaptor
   :members:


Sample adaptor
^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.sampleadaptor
   :members:


Experiment adaptor
^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.experimentadaptor
   :members:


Run adaptor
^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.runadaptor
   :members:


Collection adaptor
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.collectionadaptor
   :members:


File adaptor
^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.fileadaptor
   :members:


Sequencing run adaptor
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.seqrunadaptor
   :members:


Platform adaptor
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.platformadaptor
   :members:


Pipeline adaptor
^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.igfdb.pipelineadaptor
   :members:


Utility functions for database access
-------------------------------------

Database utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.dbutils
   :members:

Project adaptor utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.projectutils
   :members:

Sequencing adaptor utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.seqrunutils
   :members:

Pipeline adaptor utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.pipelineutils
   :members:

Platform adaptor utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.platformutils
   :members:

Pipeline seed adaptor utility functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.ehive_utils.pipeseedfactory_utils
   :members:



IGF pipeline api
=================

Pipeline api
-------------------------------

Fetch fastq files for analysis
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.analysis_fastq_fetch_utils
   :members:

Load analysis result to database and file system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: igf_data.utils.analysis_collection_utils.Analysis_collection_utils
   :members:

Run metadata validation checks
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.validation_check.metadata_validation
   :members:

Generic utility functions
--------------------------

Basic fasta sequence processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.sequtils
   :members:

Advanced fastq file processing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.fastq_utils
   :members:

Process local and remote files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.fileutils
   :members:

Load files to irods server
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.igf_irods_client
   :members:

Calculate storage statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.disk_usage_utils
   :members:


Run analysis tools
---------------------

Process fastqc output file
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.fastqc_utils
   :members:

Cellranger count utils
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.cellranger.cellranger_count_utils
   :members:

BWA utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.bwa_utils
   :members:

Picard utils
^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.picard_util
   :members:

Fastp utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.fastp_utils
   :members:

GATK utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.gatk_utils
   :members:

RSEM utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.rsem_utils
   :members:

Samtools utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.samtools_utils
   :members:

STAR utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.star_utils
   :members:

Subread utils
^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.subread_utils
   :members:

Reference genome fetch utils
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.reference_genome_utils
   :members:

Samtools utils
^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.samtools_utils
   :members:

Scanpy utils
^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.scanpy_utils
   :members:

Report page building
-------------------------------

Configure Biodalliance genome browser for qc page
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.config_genome_browser
   :members:

Process Google chart json data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.gviz_utils
   :members:

Generate data for QC project page
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.project_data_display_utils
   :members:

Generate data for QC status page
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.project_status_utils
   :members:

Generate data for QC analysis page
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.project_analysis_utils
   :members:
