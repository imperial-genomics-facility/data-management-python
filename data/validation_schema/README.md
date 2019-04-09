# Validation schema

## Samplesheet validation

Samplesheet validation json schema file [samplesheet_validation.json](samplesheet_validation.json) is for checking samplesheet metadata consistency for demultiplexing of the Illumina sequencing runs.

### Validation checklist (v1.0)

* Lane column is optional and only allowed string characters are `"1", "2", "3","4","5","6","7","8"`
* Sample_ID column is mandatory and only allowed values are alphanumeric characters (A-Z and a-z and 0-9), underscore ("\_") and hyphen ("\-")
* Sample_Name is mandatory and allowed values are alphanumeric characters (A-Z and a-z and 0-9) and hyphen ("\-")
* Sample_Project is mandatory and only allowed values are alphanumeric characters (A-Z and a-z and 0-9), underscore ("\_") and hyphen ("\-")
* I7\_Index_ID is mandatory
* index column is mandatory, allowed values are either a string of ATGC or a single cell index "SI-GA-\[A to Z\]\[one or more digit\]
* I5\_Index_ID is optional
* index2 column is optional, allowed value is a string of ATGC
* Sample_Plate column is optional
* Sample_Well column is optional
* Description column is optional, only allowed value is "10X"


## Metadata validation

Metadata validation schema file [metadata_validation.json](metadata_validation.json) is for checking consistency of the input sample metadata

### Validation checklist (v1.0.1)

Required informations:

  * project_igf_id: Unique project id
  * name: User name
  * email_id: User email id
  * sample_igf_id: Unique sample id

