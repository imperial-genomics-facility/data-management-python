import re, argparse
from igf_data.illumina.samplesheet import SampleSheet
from igf_data.illumina.runinfo_xml import RunInfo_xml

parser=argparse.ArgumentParser()
parser.add_argument('-i','--samplesheet_file', required=True, help='Illumina format samplesheet file')
parser.add_argument('-f','--runinfoxml_file', required=True, help='Illumina RunInfo.xml file')
parser.add_argument('-r','--revcomp_index', default=True, action='store_true', help='Reverse complement HiSeq and NextSeq index2 column, default: True')
parser.add_argument('-o','--output_file', required=True, help='Reformatted samplesheet file')
args=parser.parse_args()

samplesheet_file=args.samplesheet_file
revcomp_index=args.revcomp_index
output_file=args.output_file
runinfoxml_file=args.runinfoxml_file

samplesheet_data=SampleSheet(infile=samplesheet_file)
platform_name=samplesheet_data.get_platform_name()

runinfo_data=RunInfo_xml(xml_file=runinfoxml_file)
platform_series=runinfo_data.get_platform_number()

hiseq_pattern=re.compile('^HISEQ',re.IGNORECASE)
nextseq_pattern=re.compile('^NEXTSEQ',re.IGNORECASE)

if revcomp_index:
  if (re.search(hiseq_pattern, platform_name) and platform_series.startswith('K')):
    # Hack for identification of HiSeq 4000, need to replace it by db check
    samplesheet_data.get_reverse_complement_index()
  elif (re.search(nextseq_pattern, platform_name) and platform_series.startswith('NB')):
    # Hack for identification of NextSeq, need to replace it by db check
    samplesheet_data.get_reverse_complement_index()

# Reformat samplesheet
samplesheet_data.print_sampleSheet(outfile=output_file)

