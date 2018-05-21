import argparse
from igf_data.process.pipeline.modify_pipeline_seed import Modify_pipeline_seed

parser=argparse.ArgumentParser()
parser.add_argument('-t','--table_name', required=True, help='Table name for igf id lookup')
parser.add_argument('-p','--pipeline_name', required=True, help='Pipeline name for seed modification')
parser.add_argument('-s','--seed_status', required=True, help='New seed status for pipeline_seed table')
parser.add_argument('-d','--dbconfig', required=True, help='Database configuration file path')
parser.add_argument('-n','--slack_config', required=True, help='Slack configuration file path')
parser.add_argument('-a','--asana_config', required=True, help='Asana configuration file path')
parser.add_argument('-i','--asana_project_id', required=True, help='Asana project id')
parser.add_argument('-f','--input_list', required=True, help='IGF id list file')
args=parser.parse_args()

table_name=args.table_name
pipeline_name=args.pipeline_name
dbconfig=args.dbconfig
slack_config=args.slack_config
asana_config=args.asana_config
asana_project_id=args.asana_project_id
input_list=args.input_list
seed_status=args.seed_status

try:
  mps=Modify_pipeline_seed(igf_id_list=self.seqrun_input_list,
                             table_name='seqrun',
                             pipeline_name='demultiplexing_fastq',
                             dbconfig_file=dbconfig,
                             log_slack=False,
                             log_asana=False,
                             clean_up=True
                             )
  mps.reset_pipeline_seed_for_rerun(seeded_label=seed_status)
except Exception as e:
  print('Error: {0}'.format(e))