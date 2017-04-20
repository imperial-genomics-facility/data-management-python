import argparse
from __future__ import print_function
from igf_data.illumina.basesMask import BasesMask

parser=argparse.ArgumentParser()
parser.add_argument('-s','--samplesheet_file', required=True, help='Illumina format samplesheet file')
parser.add_argument('-r','--runinfo_file', required=True, help='Illumina format RunInfo.xml file')
parser.add_argument('-a','--read_offset', default=1, help='Extra sequencing cycle for reads, default: 1')
parser.add_argument('-b','--index_offset', default=0, help='Extra sequencing cycle for index, default: 0')
args=parser.parse_args()

samplesheet_file = args.samplesheet_file
runinfo_file     = args.runinfo_file
read_offset      = int(args.read_offset)
index_offset     = int(args.index_offset)


bases_mask_object=BasesMask(samplesheet_file=samplesheet_file, runinfo_file=runinfo_file, read_offset=read_offset, index_offset=index_offset)
bases_mask_value=bases_mask_object.calculate_bases_mask()

print(bases_mask_value, end='')
