#!/usr/bin/env python
import argparse,textwrap,logging
from igf_airflow.seqrun.calculate_seqrun_file_size import calculate_seqrun_file_list

description = textwrap.dedent(
"""
 A script for creating file size list of ongoing seqeuncing runs
"""
)

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,description=description)
parser.add_argument('-s','--seqrun_id', required=True, help='Sequencing run id')
parser.add_argument('-d','--seqrun_base_dir', required=True, help='Sequencing run base path dir')
parser.add_argument('-o','--output_path', required=True, help='Path for storing output json files')
args = parser.parse_args()

seqrun_id = args.seqrun_id
seqrun_base_dir = args.seqrun_base_dir
output_path = args.output_path

if __name__=='__main__':
  try:
    output_file = \
      calculate_seqrun_file_list(
        seqrun_id=seqrun_id,
        seqrun_base_dir=seqrun_base_dir,
        output_path=output_path)
    print(output_file)
  except Exception as e:
    logging.error(e)
    raise
