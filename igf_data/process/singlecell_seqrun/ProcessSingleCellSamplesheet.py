import os, json
from igf_data.illumina.samplesheet import SampleSheet

class ProcessSingleCellSamplesheet:
  '''
  A class for processing samplesheet containing single cell (10X) index barcodes
  It requires a json format file listing all the single
  cell barcodes downloaded from this page
  https://support.10xgenomics.com/single-cell-gene-expression/sequencing/doc/
  specifications-sample-index-sets-for-single-cell-3
  
  required params:
  samplesheet_file: A samplesheet containing single cell samples
  singlecell_barcode_json: A JSON file listing single cell indexes
  singlecell_lebel: A text lebel for the single cell sample description 
  '''
  
  def __init__(self,samplesheet_file,singlecell_barcode_json,singlecell_lebel):
    self.samplesheet_file=samplesheet_file
    self.singlecell_barcode_json=singlecell_barcode_json
    self.singlecell_lebel=singlecell_lebel
    
  def replace_singlecell_barcodes(self,output_samplesheet):
    '''
    A method for replacing single cell index codes present in the samplesheet 
    with the four index sequences. This method will create 4 samplesheet entries
    for each of the single cell samples with _1 to _4 suffix and relevant indexes
    
    required params:
    output_samplesheet: A file name of the output samplesheet
    '''
    try:
      samplesheet_data=SampleSheet(infile=self.samplesheet_file)
      
    except:
      raise