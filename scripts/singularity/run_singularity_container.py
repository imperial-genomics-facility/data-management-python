#!/usr/bin/env python
import argparse,textwrap
from igf_data.utils.singularity_run_wrapper import singularity_run

description = textwrap.dedent(
"""
 A script for running commands within a singularity container in HPC

 USAGE:
   python run_singularity_container.py
     -i /path/SINGULARITY_IMAGE
     -b /path/CONTAINER_MOUNTED_DIR
     -a command
     -a arg1
     -a arg2

 It will run the following singularity run command:
   singularity run
     --bind /path/CONTAINER_MOUNTED_DIR:/tmp
     /path/SINGULARITY_IMAGE
       command arg1 arg2

""")

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=description)
parser.add_argument('-i','--image_path', required=True, help='Singularity image path')
parser.add_argument('-b','--path_bind', required=True, help='Path to bind to singularity /tmp dir')
parser.add_argument('-a','--run_args', action='append', default=[], help='List of args for singularity run')

args = parser.parse_args()
image_path = args.image_path
path_bind = args.path_bind
run_args = args.run_args


if __name__=='__main__':
  try:
    res,singularity_run_cmd = \
      singularity_run(
        image_path=image_path,
        path_bind=path_bind,
        args_list=run_args)
  except Exception as e:
    raise ValueError("Failed to run singularity container, error: {0}".format(e))