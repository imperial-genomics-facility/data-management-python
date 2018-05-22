Calcumate bases mask value for sequencing run
==============================================

**Usage**

  makeBasesMask.py 
  [-h] 
  -s SAMPLESHEET_FILE 
  -r RUNINFO_FILE
  [-a READ_OFFSET]
  [-b INDEX_OFFSET]


**Parameters**

  -h, --help               :  Show this help message and exit
  -s, --samplesheet_file   :  Illumina format samplesheet file
  -r, --runinfo_file       :  Illumina format RunInfo.xml file
  -a, --read_offset        :  Extra sequencing cycle for reads, default: 1
  -b, --index_offset       :  Extra sequencing cycle for index, default: 0

