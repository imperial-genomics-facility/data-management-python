Load flowcell runs to database
==============================

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

