IGF database
=============

Database schema
-----------------
.. automodule:: igf_data.igfdb.igfTables
   :members:

IGF database api
=================


Core adaptor class
---------------------


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


Helper api class
------------------------

Base database utils
^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.dbutils
   :members:

Base file utils
^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.fileutils
   :members:

Irods client
^^^^^^^^^^^^^
.. automodule:: igf_data.utils.igf_irods_client
   :members:

Base fastq sequence utils
^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.sequtils
   :members:

Project helper adaptor
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.projectutils
   :members:

Sequencing run helper adaptor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.seqrunutils
   :members:

Pipeline helper adaptor
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.pipelineutils
   :members:

Platform helper adaptor
^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.platformutils
   :members:

New pipeline seed utils
^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.ehive_utils.pipeseedfactory_utils
   :members:

Calculate storage statistics
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.disk_usage_utils
   :members:


Helper api for report page
-------------------------------

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


Helper api for tools
---------------------

Process fastqc output file
^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.fastqc_utils
   :members:

Cellranger count utils
^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.cellranger.cellranger_count_utils
   :members:

Picard utils
^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.tools.picard_utils
   :members:

Reference genome utils
^^^^^^^^^^^^^^^^^^^^^^^^^
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


Helper api for analysis
-----------------------

Fetch fastq files for analysis
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. automodule:: igf_data.utils.analysis_fastq_fetch_utils
   :members:

Load analysis result to database and file system
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. autoclass:: igf_data.utils.analysis_collection_utils.Analysis_collection_utils
   :members:
