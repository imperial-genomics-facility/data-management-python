import argparse, shutil, json, os
from igf_data.utils.gviz_utils import convert_to_gviz_json_for_display
from igf_data.utils.disk_usage_utils import get_sub_directory_size_in_gb
from igf_data.utils.fileutils import copy_remote_file, get_temp_dir, remove_dir

parser=argparse.ArgumentParser()
parser.add_argument('-p','--directory_path', required=True, help='A directory path for sub directory lookup')
parser.add_argument('-c','--copy_to_remoter', default=False, action='store_true', help='Toggle file copy to remote server')
parser.add_argument('-r','--remote_server', required=False, help='Remote server address')
parser.add_argument('-o','--output_filepath', required=True, help='Output gviz file path')
args=parser.parse_args()

directory_path=args.directory_path
copy_to_remoter=args.copy_to_remoter
remote_server=args.remote_server
output_filepath=args.output_filepath

try:
  if copy_to_remoter and not remote_server:
    parser.print_help()
    raise ValueError('Remote server address is required for copying files.')

  temp_dir=get_temp_dir()
  temp_file=os.path.join(temp_dir,'subdirectory_usage.json')                    # get temp file path
  storage_stats, description, column_order=\
              get_sub_directory_size_in_gb(input_path=directory_path)           # calculate sub directory usage stats
  convert_to_gviz_json_for_display(description=description,
                                   data=storage_stats,
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
  print('Error: {0}'.format(e))