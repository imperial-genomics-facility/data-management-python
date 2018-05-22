Find new finished sequencing runs and process them for pipeline
===============================================================

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

    -h, --help                     : Show this help message and exit
    -p, --seqrun_path              : Seqrun directory path
    -m, --md5_path                 : Seqrun md5 output dir
    -d, --dbconfig_path            : Database configuration json file
    -s, --slack_config             : Slack configuration json file
    -a, --asana_config             : Asana configuration json file
    -i, --asana_project_id         : Asana project id
    -n, --pipeline_name            : IGF pipeline name
    -j, --samplesheet_json_schema  : JSON schema for samplesheet validation 
    -e, --exclude_path             : List of sub directories excluded from the search

