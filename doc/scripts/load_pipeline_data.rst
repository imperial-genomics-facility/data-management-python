Load pipeline configuration to database
=======================================

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

