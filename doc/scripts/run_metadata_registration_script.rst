Run metadata registration script
================================

**Usage**


find_and_register_project_metdata.py   

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
  
