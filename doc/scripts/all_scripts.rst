Sequencing run processing
===========================

Metadata registration
----------------------

**Usage**

find_and_register_project_metdata.py   
  [-h]
  -p PROJET_INFO_PATH 
  -d DBCONFIG  
  -t USER_ACCOUNT_TEMPLATE 
  -n SLACK_CONFIG 
  -u HPC_USER 
  -a HPC_ADDRESS  
  -l LDAP_SERVER 
  [-h]
  [-s]
  [-c]
  [-i]
  [-m]


**Parameters**

   -h, --help                   : Show this help message and exit
   -p, --projet_info_path       : Project metdata directory path
   -d, --dbconfig               : Database configuration file path
   -t, --user_account_template  : User account information email template file path
   -s, --log_slack              : Toggle slack logging 
   -n, --slack_config           : Slack configuration file path
   -c, --check_hpc_user         : Toggle HPC user checking
   -u, --hpc_user               : HPC user name for ldap server checking
   -a, --hpc_address            : HPC address for ldap server checking
   -l, --ldap_server            : Ldap server address
   -i, --setup_irods            : Setup iRODS account for user
   -m, --notify_user            : Notify user about new account and password
  

Monitor sequencing run for demultiplexing
-------------------------------------------

**Usage**

  find_new_seqrun_and_prepare_md5.py
    [-h]
    -p SEQRUN_PATH
    -m MD5_PATH
    -d DBCONFIG_PATH
    -s SLACK_CONFIG
    -a ASANA_CONFIG
    -i ASANA_PROJECT_ID
    -n PIPELINE_NAME
    -j SAMPLESHEET_JSON_SCHEMA
    [-e EXCLUDE_PATH]

**Parameters**

  -h, --help                                             : show this help message and exit
  -p, --seqrun_path SEQRUN_PATH                          : Seqrun directory path
  -m, --md5_path MD5_PATH                                : Seqrun md5 output dir
  -d, --dbconfig_path DBCONFIG_PATH                      : Database configuration json file
  -s, --slack_config SLACK_CONFIG                        : Slack configuration json file
  -a, --asana_config ASANA_CONFIG                        : Asana configuration json file
  -i, --asana_project_id ASANA_PROJECT_ID                : Asana project id
  -n, --pipeline_name PIPELINE_NAME                      : IGF pipeline name
  -j, --samplesheet_json_schema SAMPLESHEET_JSON_SCHEMA  : JSON schema for samplesheet validation
  -e, --exclude_path EXCLUDE_PATH                        : List of sub directories excluded from the search

Switch off project barcode checking
-------------------------------------

**Usage**

  mark_project_barcode_check_off.py
    [-h]
    -p PROJET_ID_LIST
    -d DBCONFIG
    [-s]
    -n SLACK_CONFIG

**Parameters**

  -h, --help                           : show this help message and exit
  -p, --projet_id_list PROJET_ID_LIST  : A file path listing project_igf_id
  -d, --dbconfig DBCONFIG              : Database configuration file path
  -s, --log_slack                      : Toggle slack logging
  -n, --slack_config SLACK_CONFIG      : Slack configuration file path

Accept modified samplesheet for demultiplexing run
---------------------------------------------------

**Usage**

  reset_samplesheet_for_pipeline.py
    [-h]
    -p SEQRUN_PATH
    -d DBCONFIG
    -n SLACK_CONFIG
    -a ASANA_CONFIG
    -i ASANA_PROJECT_ID
    -f INPUT_LIST

**Parameters**

  -h, --help                               : show this help message and exit
  -p, --seqrun_path SEQRUN_PATH            : Sequencing run directory path
  -d, --dbconfig DBCONFIG                  : Database configuration file path
  -n, --slack_config SLACK_CONFIG          : Slack configuration file path
  -a, --asana_config ASANA_CONFIG          : Asana configuration file path
  -i, --asana_project_id ASANA_PROJECT_ID  : Asana project id
  -f, --input_list INPUT_LIST              : Sequencing run id list file


Copy files to temp directory for demultiplexing run
-------------------------------------------------------

**Usage**

  moveFilesForDemultiplexing.py
    [-h]
    -i INPUT_DIR
    -o OUTPUT_DIR
    -s SAMPLESHEET_FILE
    -r RUNINFO_FILE

**Parameters**

  -h, --help                               : show this help message and exit
  -i, --input_dir INPUT_DIR                : Input files directory
  -o, --output_dir OUTPUT_DIR              : Output files directory
  -s, --samplesheet_file SAMPLESHEET_FILE  : Illumina format samplesheet file
  -r, --runinfo_file RUNINFO_FILE          : Illumina format RunInfo.xml file
                        

Transfer metadata to experiment from sample entries
----------------------------------------------------

**Usage**

  update_experiment_metadata_from_sample_attribute.py [-h] -d DBCONFIG -n SLACK_CONFIG

**Parameters**

  -h, --help            show this help message and exit
  -d, --dbconfig DBCONFIG          : Database configuration file path
  -n, --slack_config SLACK_CONFIG  : Slack configuration file path


Pipeline control
=================

Reset pipeline for data processing
-----------------------------------

**Usage**

  batch_modify_pipeline_seed.py [-h] -t TABLE_NAME -p PIPELINE_NAME 
                                -s SEED_STATUS -d DBCONFIG -n SLACK_CONFIG
                                -a ASANA_CONFIG -i ASANA_PROJECT_ID
                                -f INPUT_LIST

**Parameters**

  -h, --help                               : show this help message and exit
  -t, --table_name TABLE_NAME              : Table name for igf id lookup
  -p, --pipeline_name PIPELINE_NAME        : Pipeline name for seed modification
  -s, --seed_status SEED_STATUS            : New seed status for pipeline_seed table
  -d, --dbconfig DBCONFIG                  : Database configuration file path
  -n, --slack_config SLACK_CONFIG          : Slack configuration file path
  -a, --asana_config ASANA_CONFIG          : Asana configuration file path
  -i, --asana_project_id ASANA_PROJECT_ID  : Asana project id
  -f, --input_list INPUT_LIST              : IGF id list file

Samplesheet processing
=======================

Divide samplesheet data
------------------------

**Usage**

  divide_samplesheet.py
    [-h]
    -i SAMPLESHEET_FILE
    -d OUTPUT_DIR [-p]

**Parameters**

  -h, --help                              : show this help message and exit
  -i, -samplesheet_file SAMPLESHEET_FILE  : Illumina format samplesheet file
  -d, --output_dir OUTPUT_DIR             : Output directory for writing samplesheet file
  -p, --print_stats                       : Print available stats for the samplesheet and exit



Reformat samplesheet for demultiplexing
----------------------------------------

**Usage**

  reformatSampleSheet.py
    [-h]
    -i SAMPLESHEET_FILE
    -f RUNINFOXML_FILE
    [-r]
    -o OUTPUT_FILE

**Parameters**

  -h, --help                               : show this help message and exit
  -i, --samplesheet_file SAMPLESHEET_FILE  : Illumina format samplesheet file
  -f, --runinfoxml_file RUNINFOXML_FILE    : Illumina RunInfo.xml file
  -r, --revcomp_index                      : Reverse complement HiSeq and NextSeq index2 column,
                                             default: True
  -o, --output_file OUTPUT_FILE            : Reformatted samplesheet file

Calculate basesmask for demultiplexing
----------------------------------------

**Usage**

  makeBasesMask.py
    [-h]
    -s SAMPLESHEET_FILE
    -r RUNINFO_FILE
    [-a READ_OFFSET]
    [-b INDEX_OFFSET]

**Parameters**

  -h, --help                               : show this help message and exit
  -s, --samplesheet_file SAMPLESHEET_FILE  : Illumina format samplesheet file
  -r, --runinfo_file RUNINFO_FILE          : Illumina format RunInfo.xml file
  -a, --read_offset READ_OFFSET            : Extra sequencing cycle for reads, default: 1
  -b, --index_offset INDEX_OFFSET          : Extra sequencing cycle for index, default: 0
 


Create or modify data to database
=====================================


Clean up data from existing database and create new tables
-----------------------------------------------------------

**Usage**
  
  clean_and_rebuild_database.py 
    [-h] 
    -d DBCONFIG_PATH 
    -s SLACK_CONFIG

**Parameters**

  -h, --help             :  Show this help message and exit
  -d, --dbconfig_path    :  Database configuration json file
  -s, --slack_config     :  Slack configuration json file


Load flowcell runs to database
--------------------------------

**Usage**

  load_flowcell_rules_data.py 
    [-h] 
    -f FLOWCELL_DATA 
    [-u] 
    -d DBCONFIG_PATH
    -s SLACK_CONFIG


**Parameters**

  -h, --help            :  Show this help message and exit
  -f, --flowcell_data   :  Flowcell rules data json file
  -u, --update          :  Update existing flowcell rules data, default: False
  -d, --dbconfig_path   :  Database configuration json file
  -s, --slack_config    :  Slack configuration json file


Load pipeline configuration to database
----------------------------------------

**Usage**

  load_pipeline_data.py 
    [-h] 
    -p PIPELINE_DATA 
    [-u] 
    -d DBCONFIG_PATH 
    -s SLACK_CONFIG

**Paramaters**

  -h, --help           :  Show this help message and exit
  -p, --pipeline_data  :  Pipeline data json file
  -u, --update         :  Update existing platform data, default: False
  -d, --dbconfig_path  :  Database configuration json file
  -s, --slack_config   :  Slack configuration json file


Load sequencing platform information to database
---------------------------------------------------

**Usage**

  load_platform_data.py [-h] -p PLATFORM_DATA [-u] -d DBCONFIG_PATH -s SLACK_CONFIG


**Parameters**

  -h, --help             :  Show this help message and exit
  -p, --platform_data    :  Platform data json file
  -u, --update           :  Update existing platform data, default: False
  -d, --dbconfig_path    :  Database configuration json file
  -s, --slack_config     :  Slack configuration json file


Load sequencing run information to database from a text input
---------------------------------------------------------------

**Usage**

  load_seqrun_data.py [-h] -p SEQRUN_DATA -d DBCONFIG_PATH -s  SLACK_CONFIG

**Parameters**

  -h, --help             :  Show this help message and exit
  -p, --seqrun_data      :  Seqrun data json file
  -d, --dbconfig_path    :  Database configuration json file
  -s, --slack_config     :  Slack configuration json file


Load file entries and build collection in database
----------------------------------------------------

**Usage**

load_files_collecion_to_db.py
    [-h]
    -f COLLECTION_FILE_DATA
    -d DBCONFIG_PATH
    [-s]

**Parameters**

  -h, --help                                       : show this help message and exit
  -f, --collection_file_data COLLECTION_FILE_DATA  : Collection file data json file
  -d, --dbconfig_path DBCONFIG_PATH                : Database configuration json file
  -s, --calculate_checksum                         : Toggle file checksum calculation


Check Storage utilisation
==========================

Calculate disk usage summary
-----------------------------

**Usage**

  calculate_disk_usage_summary.py
    [-h]
    -p DISK_PATH
    [-c]
    [-r REMOTE_SERVER]
    -o OUTPUT_PATH

**Parameters**

  -h, --help                         : show this help message and exit
  -p, --disk_path DISK_PATH          : List of disk path for summary calculation
  -c, --copy_to_remoter              : Toggle file copy to remote server
  -r, --remote_server REMOTE_SERVER  : Remote server address
  -o, --output_path OUTPUT_PATH      : Output directory path


Calculate disk usage for a top level directory
-----------------------------------------------

**Usage**

  calculate_sub_directory_usage.py
    [-h]
    -p DIRECTORY_PATH
    [-c]
    [-r REMOTE_SERVER] -o OUTPUT_FILEPATH

**Parameters**

  -h, --help                             : show this help message and exit
  -p, --directory_path DIRECTORY_PATH    : A directory path for sub directory lookup
  -c, --copy_to_remoter                  : Toggle file copy to remote server
  -r, --remote_server REMOTE_SERVER      : Remote server address
  -o, --output_filepath OUTPUT_FILEPATH  : Output gviz file path


Merge disk usage summary file and build a gviz json
-----------------------------------------------------

**Usage**

  merge_disk_usage_summary.py
    [-h]
    -f CONFIG_FILE
    [-l LABEL_FILE]
    [-c]
    [-r REMOTE_SERVER]
    -o OUTPUT_FILEPATH

**Parameters**

  -h, --help                             : show this help message and exit
  -f, --config_file CONFIG_FILE          : A configuration json file for disk usage summary
  -l, --label_file LABEL_FILE            : A json file for disk label name
  -c, --copy_to_remoter                  : Toggle file copy to remote server
  -r, --remote_server REMOTE_SERVER      : Remote server address
  -o, --output_filepath OUTPUT_FILEPATH  : Output gviz file path


Seed analysis pipeline
---------------------------

A script for finding new experiment entries for seeding analysis pipeline

**Usage**

  find_and_seed_new_analysis.py 
        [-h]
        -d DBCONFIG_PATH
        -s SLACK_CONFIG
        -p PIPELINE_NAME
        -t FASTQ_TYPE
        -f PROJECT_NAME_FILE
        [-m SPECIES_NAME]
        [-l LIBRARY_SOURCE]


**Parameters**

  -h, --help                                 : show this help message and exit
  -d , --dbconfig_path DBCONFIG_PATH         : Database configuration json file
  -s , --slack_config SLACK_CONFIG           : Slack configuration json file
  -p , --pipeline_name PIPELINE_NAME         : IGF pipeline name
  -t , --fastq_type FASTQ_TYPE               : Fastq collection type
  -f , --project_name_file PROJECT_NAME_FILE : File containing project names for seeding analysis pipeline
  -m , --species_name SPECIES_NAME           : Species name to filter analysis
  -l , --library_source LIBRARY_SOURCE       : Library source to filter analysis


