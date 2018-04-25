import argparse, shutil, json, os
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from igf_data.utils.disk_usage_utils import merge_storage_stats_json
from igf_data.utils.fileutils import copy_remote_file, get_temp_dir, remove_dir

parser=argparse.ArgumentParser()
parser.add_argument('-f','--config_file', required=True, help='A configuration json file for disk usage summary')
parser.add_argument('-l','--label_file', default=None, help='A json file for disk label name')
parser.add_argument('-c','--copy_to_remoter', default=False, action='store_true', help='Toggle file copy to remote server')
parser.add_argument('-r','--remote_server', required=False, help='Remote server address')
parser.add_argument('-o','--output_filepath', required=True, help='Output gviz file path')
args=parser.parse_args()

config_file=args.config_file
label_file=args.label_file
copy_to_remoter=args.copy_to_remoter
remote_server=args.remote_server
output_filepath=args.output_filepath

try:
  if copy_to_remoter and not remote_server:
    parser.print_help()
    raise ValueError('Remote server address is required for copying files.')

  temp_dir=get_temp_dir()
  temp_file=os.path.join(temp_dir,'merged_summary_usage.json')                  # get temp file path
  data,description,column_order=merge_storage_stats_json(config_file,label_file) # get merged summary
  convert_to_gviz_json_for_display(description=description,
                                   data=data,
                                   columns_order=column_order,
                                   output_file=temp_file)                       # write temp gviz json file
  if copy_to_remoter:
    copy_remote_file(source_path=temp_file,
                     destinationa_path=output_filepath,
                     destination_address=remote_server)                         # copy json file to remote server
  else:
    shutil.copy2(temp_file, output_filepath)                                    # copy json file to local server

  remove_dir(temp_dir)                                                          # remove temp dir
except Exception as e:
  print(e)