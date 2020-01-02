#!/usr/bin/env python

import argparse
from igf_data.utils.fileutils import copy_remote_file

parser=argparse.ArgumentParser()
parser.add_argument('-i','--source_path',  required=True, help='Source file path')
parser.add_argument('-o','--dest_path', required=True, help='Destination file path')
parser.add_argument('-s','--source_address', default=None, help='Source address with user name')
parser.add_argument('-d','--dest_address', default=None, help='Destination address with user name')
parser.add_argument('-f','--force_update', default=False, action='store_true', help='Force update existing file')

args = parser.parse_args()
source_path = args.source_path
dest_path = args.dest_path
source_address = args.source_address
dest_address = args.dest_address
force_update = args.force_update

if __name__=='__main__':
  try:
    copy_remote_file(
      source_path=source_path,
      destinationa_path=dest_path,
      source_address=source_address,
      destination_address=dest_address,
      copy_method='rsync',
      check_file=True,
      force_update=force_update)
  except Exception as e:
    raise ValueError("Failed to copy remote file, error:{0}".format(e))