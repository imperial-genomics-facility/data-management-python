import re, argparse, copy, os
from igf_data.illumina.samplesheet import SampleSheet


parser=argparse.ArgumentParser()
parser.add_argument('-i','--samplesheet_file', required=True, help='Illumina format samplesheet file')
parser.add_argument('-d','--output_dir', required=True, help='Output directory for writing samplesheet file')
parser.add_argument('-p','--print_stats', default=False, action='store_true', help='Print available statse for the samplesheet and exit')
args=parser.parse_args()

samplesheet_file=args.samplesheet_file
print_stats=args.print_stats
output_dir=args.output_dir
file_prefix=os.path.basename(samplesheet_file)

samplesheet_data=SampleSheet(infile=samplesheet_file)
platform_name=samplesheet_data.get_platform_name()
sample_lane=samplesheet_data.get_lane_count()

platform_pattern=re.compile('^HISEQ',re.IGNORECASE)

data_group=dict()

if len(sample_lane) > 1:
  '''
  Run this loop only for the HiSeq data
  '''

  for lane_id in sample_lane:
    samplesheet_data_tmp=copy.copy(samplesheet_data)
    samplesheet_data_tmp.filter_sample_data( condition_key='Lane', condition_value=lane_id )
    data_group[lane_id]=samplesheet_data_tmp.group_data_by_index_length()

else:
  '''
  For MiSeq and NextSeq
  '''
  data_group[1]=samplesheet_data.group_data_by_index_length()


for lane_id in data_group.keys():
  for index_length in data_group[lane_id].keys():
    output_file=os.path.join(output_dir, '{0}_{1}_{2}'.format(file_prefix,lane_id,index_length))
    data_group[lane_id][index_length].print_sampleSheet(outfile=output_file)
