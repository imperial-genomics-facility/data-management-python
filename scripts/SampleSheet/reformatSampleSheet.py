import re, argparse
from igf_data.illumina.samplesheet import SampleSheet

parser=argparse.ArgumentParser()
parser.add_argument('-i','--samplesheet_file', required=True, help='Illumina format samplesheet file')
parser.add_argument('-r','--revcomp_index', default=False, action='store_true', help='Reverse complement index2 column, default: false')
parser.add_argument('-o','--output_file', required=True, help='Reformatted samplesheet file')
args=parser.parse_args()

samplesheet_file=args.samplesheet_file
revcomp_index=args.revcomp_index
output_file=args.output_file

samplesheet_data=SampleSheet(infile=samplesheet_file)
platform_name=samplesheet_data.get_platform_name()

platform_pattern=re.compile('^HISEQ',re.IGNORECASE)

if (re.search(platform_pattern, platform_name) and revcomp_index):
  samplesheet_data.get_reverse_complement_index()

samplesheet_data.print_sampleSheet(outfile=output_file)

