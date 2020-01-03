#!/usr/bin/env python
#
########################################################################
#
# A script for reformatting metadata and samplesheet files
# and copy them to the archive dir with additional year and month tags
# For checking the usgae, run the script with -h flag
#
########################################################################
#
import argparse,os,re
from datetime import datetime
from igf_data.task_tracking.igf_slack import IGF_slack
from igf_data.utils.fileutils import check_file_path,get_temp_dir,remove_dir,copy_local_file
from igf_data.process.metadata_reformat.reformat_metadata_file import Reformat_metadata_file
from igf_data.process.metadata_reformat.reformat_samplesheet_file import Reformat_samplesheet_file,SampleSheet

parser = argparse.ArgumentParser()
parser.add_argument('-i','--input_path', required=True, help='Dir path containing the raw metadata files')
parser.add_argument('-p','--archive_dir', required=True, help='Metadata file archive dir')
parser.add_argument('-s','--slack_config', required=True, help='Slack configuration file path')
args = parser.parse_args()
input_path = args.input_path
archive_dir = args.archive_dir
slack_config = args.slack_config


if __name__=='__main__':
  try:
    temp_dir = get_temp_dir()
    check_file_path(input_path)
    check_file_path(archive_dir)
    slack_obj = IGF_slack(slack_config=slack_config)
    samplesheet_pattern = re.compile(r'\S+_SampleSheet\.csv')
    reformatted_pattern = re.compile(r'\S+_reformatted.csv')
    year_tag = datetime.now().strftime('%Y')
    month_tag = datetime.now().strftime('%m_%Y')
    for entry in os.listdir(path=input_path):
      if os.path.isfile(entry) and \
         entry.endswith('.csv') and \
         not re.match(reformatted_pattern,entry):                               # looking at csv files and skipping reformatted files
        output_file = entry.replace('.csv','_reformatted.csv')                  # name for reformatted files
        dest_path = \
          os.path.join(
            archive_dir,
            year_tag,
            month_tag)
        raw_dest_path = \
          os.path.join(dest_path,'raw_data')
        formatted_dest_path = \
          os.path.join(dest_path,'formatted_data')
        output_file = os.path.join(temp_dir,output_file)
        if re.match(samplesheet_pattern,entry):
          try:
            re_samplesheet = \
              Reformat_samplesheet_file(
                infile=entry,
                file_format='csv')
            re_samplesheet.\
              reformat_raw_samplesheet_file(
                output_file=output_file)                                        # reformat samplesheet data
            copy_local_file(
              os.path.join(input_path,entry),
              os.path.join(raw_dest_path,entry),
              force=True)                                                       # archive raw samplesheet file
            copy_local_file(
              output_file,
              os.path.join(
                formatted_dest_path,
                os.path.basename(output_file)),
              force=True)                                                       # archive formatted samplesheet file
          except Exception as e:
            message = \
              "Failed to reformat samplesheet file {0}, error: {1}".\
                format(entry,e)
            slack_obj.post_message_to_channel(
              message=message,
              reaction='fail')
        else:
          try:
            re_metadata = \
              Reformat_metadata_file(\
                infile=entry)
            re_metadata.\
              reformat_raw_metadata_file(
                output_file=output_file)                                        # reformat metadata file
            copy_local_file(
              os.path.join(input_path,entry),
              os.path.join(raw_dest_path,entry),
              force=True)                                                       # archive raw metadata file
            copy_local_file(
              output_file,
              os.path.join(
                formatted_dest_path,
                os.path.basename(output_file)),
              force=True)                                                       # archive formatted metadata file
          except Exception as e:
            message = \
              "Failed to reformat metadata file {0}, error: {1}".\
                format(entry,e)
            slack_obj.post_message_to_channel(
              message=message,
              reaction='fail')
    remove_dir(temp_dir)

  except Exception as e:
    if 'temp_dir' in vars():
      remove_dir(temp_dir)
    message = \
      "Failed to reformat metadata files, error: {0}".format(e)
    if 'slack_obj' in vars():
      slack_obj.post_message_to_channel(
        message=message,
        reaction='fail')
    raise ValueError(message)