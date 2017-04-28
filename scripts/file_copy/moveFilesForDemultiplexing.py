import argparse
from igf_data.process.moveBclFilesForDemultiplexing import moveBclFilesForDemultiplexing

parser=argparse.ArgumentParser()
parser.add_argument('-i','--input_dir',  required=True, help='Input files  directory')
parser.add_argument('-o','--output_dir', required=True, help='Output files directory')
parser.add_argument('-s','--samplesheet_file', required=True, help='Illumina format samplesheet file')
parser.add_argument('-r','--runinfo_file', required=True, help='Illumina format RunInfo.xml file')
args=parser.parse_args()

input_dir    = args.input_dir
output_dir   = args.output_dir
samplesheet  = args.samplesheet_file
runinfo_file = args.runinfo_file

move_file=moveBclFilesForDemultiplexing(input_dir=input_dir, output_dir=output_dir, samplesheet=samplesheet, run_info_xml=runinfo_file)
move_file.copy_bcl_files()
