import re,string
import pandas as pd

EXPERIMENT_TYPE_LOOKUP = \
[{'library_preparation': 'WHOLE GENOME SEQUENCING - SAMPLE', 'library_type': 'WHOLE GENOME',
  'library_strategy': 'WGS', 'experiment_type': 'WGS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WHOLE GENOME SEQUENCING HUMAN - SAMPLE', 'library_type': 'WHOLE GENOME',
  'library_strategy': 'WGS', 'experiment_type': 'WGS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WHOLE GENOME SEQUENCING - BACTERIA', 'library_type': 'WHOLE GENOME',
  'library_strategy': 'WGS', 'experiment_type': 'WGS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WGA', 'library_type': 'WGA',
  'library_strategy': 'WGA', 'experiment_type': 'WGA', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WHOLE EXOME CAPTURE - EXONS - SAMPLE', 'library_type': 'HYBRID CAPTURE - EXOME',
  'library_strategy': 'WXS', 'experiment_type': 'WXS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WHOLE EXOME CAPTURE - EXONS + UTR - SAMPLE', 'library_type': 'HYBRID CAPTURE - EXOME',
  'library_strategy': 'WXS', 'experiment_type': 'WXS-UTR', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - RIBOSOME PROFILING - SAMPLE', 'library_type': 'TOTAL RNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'RIBOSOME-PROFILING', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - TOTAL RNA', 'library_type': 'TOTAL RNA',
  'library_strategy': 'RNA-SEQ','experiment_type': 'TOTAL-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - MRNA', 'library_type': 'MRNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'POLYA-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - MRNA STRANDED - SAMPLE', 'library_type': 'RNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'POLYA-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - TOTAL RNA WITH RRNA DEPLETION - SAMPLE', 'library_type': 'RNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TOTAL-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - LOW INPUT WITH RIBODEPLETION', 'library_type': 'MRNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'RIBODEPLETION', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - TOTAL RNA WITH GLOBIN DEPLETION', 'library_type': 'TOTAL RNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TOTAL-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - MRNA RNA WITH GLOBIN DEPLETION', 'library_type': 'MRNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'POLYA-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': "RNA SEQUENCING - 3' END RNA-SEQ", 'library_type': 'MRNA',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'POLYA-RNA-3P', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': "SINGLE CELL -3' RNASEQ- SAMPLE", 'library_type': "SINGLE CELL-3' RNA",
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TENX-TRANSCRIPTOME-3P', 'library_source': 'TRANSCRIPTOMIC_SINGLE_CELL','biomaterial_type':'UNKNOWN'},
 {'library_preparation': "SINGLE CELL -3' RNASEQ- SAMPLE NUCLEI", 'library_type': "SINGLE CELL-3' RNA (NUCLEI)",
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TENX-TRANSCRIPTOME-3P', 'library_source': 'TRANSCRIPTOMIC_SINGLE_CELL','biomaterial_type':'SINGLE_NUCLEI'},
 {'library_preparation': "SINGLE CELL -5' RNASEQ- SAMPLE", 'library_type': "SINGLE CELL-5' RNA",
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TENX-TRANSCRIPTOME-5P', 'library_source': 'TRANSCRIPTOMIC_SINGLE_CELL','biomaterial_type':'UNKNOWN'},
 {'library_preparation': "SINGLE CELL -5' RNASEQ- SAMPLE NUCLEI", 'library_type': "SINGLE CELL-5' RNA (NUCLEI)",
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'TENX-TRANSCRIPTOME-5P', 'library_source': 'TRANSCRIPTOMIC_SINGLE_CELL','biomaterial_type':'SINGLE_NUCLEI'},
 {'library_preparation': 'METAGENOMIC PROFILING - 16S RRNA SEQUENCING - SAMPLE', 'library_type': '16S',
  'library_strategy': 'RNA-SEQ', 'experiment_type': '16S', 'library_source': 'METAGENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RNA SEQUENCING - SMALL RNA - SAMPLE', 'library_type': 'SMALL RNA',
  'library_strategy': 'MIRNA-SEQ', 'experiment_type': 'SMALL-RNA', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'NCRNA-SEQ', 'library_type': 'NCRNA-SEQ',
  'library_strategy': 'NCRNA-SEQ', 'experiment_type': 'NCRNA-SEQ', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'FL-CDNA', 'library_type': 'FL-CDNA',
  'library_strategy': 'FL-CDNA', 'experiment_type': 'FL-CDNA', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'EST', 'library_type': 'EST',
  'library_strategy': 'EST', 'experiment_type': 'EST', 'library_source': 'TRANSCRIPTOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'HI-C SEQ', 'library_type': 'HI-C SEQ',
  'library_strategy': 'HI-C', 'experiment_type': 'HI-C', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'ATAC SEQ', 'library_type': 'ATAC SEQ',
  'library_strategy': 'ATAC-SEQ', 'experiment_type': 'ATAC-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'DNASE-SEQ', 'library_type': 'DNASE-SEQ',
  'library_strategy': 'DNASE-SEQ', 'experiment_type': 'DNASE-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'WCS', 'library_type': 'WCS',
  'library_strategy': 'WCS', 'experiment_type': 'WCS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RAD-SEQ', 'library_type': 'RAD-SEQ',
  'library_strategy': 'RAD-SEQ', 'experiment_type': 'RAD-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CLONE', 'library_type': 'CLONE',
  'library_strategy': 'CLONE', 'experiment_type': 'CLONE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'POOLCLONE', 'library_type': 'POOLCLONE',
  'library_strategy': 'POOLCLONE', 'experiment_type': 'POOLCLONE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'AMPLICON SEQUENCING - ILLUMINA TRUSEQ CUSTOM AMPLICON', 'library_type': 'AMPLICON SEQ',
  'library_strategy': 'AMPLICON', 'experiment_type': 'AMPLICON', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CLONEEND', 'library_type': 'CLONEEND',
  'library_strategy': 'CLONEEND', 'experiment_type': 'CLONEEND', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'FINISHING', 'library_type': 'FINISHING',
  'library_strategy': 'FINISHING', 'experiment_type': 'FINISHING', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - SAMPLE', 'library_type': 'CHIP SEQ',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'TF', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - INPUT', 'library_type': 'CHIP SEQ - INPUT',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'CHIP-INPUT', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - TF', 'library_type': 'CHIP SEQ - TF',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'TF', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - BROAD PEAK', 'library_type': 'CHIP SEQ - BROAD PEAK',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'HISTONE-BROAD', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - NARROW PEAK', 'library_type': 'CHIP SEQ - NARROW PEAK',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'HISTONE-NARROW', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'MNASE-SEQ', 'library_type': 'MNASE-SEQ',
  'library_strategy': 'MNASE-SEQ', 'experiment_type': 'MNASE-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'DNASE-HYPERSENSITIVITY', 'library_type': 'DNASE-HYPERSENSITIVITY',
  'library_strategy': 'DNASE-HYPERSENSITIVITY', 'experiment_type': 'DNASE-HYPERSENSITIVITY', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'METHYLATION PROFILING - RRBS-SEQ - SAMPLE', 'library_type': 'RRBS-SEQ',
  'library_strategy': 'BISULFITE-SEQ', 'experiment_type': 'RRBS-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'METHYLATION PROFILING - WHOLE GENOME BISULFITE SEQUENCING - SAMPLE', 'library_type': 'BISULFITE SEQ',
  'library_strategy': 'BISULFITE-SEQ', 'experiment_type': 'WGBS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CTS', 'library_type': 'CTS',
  'library_strategy': 'CTS', 'experiment_type': 'CTS', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'MRE-SEQ', 'library_type': 'MRE-SEQ',
  'library_strategy': 'MRE-SEQ', 'experiment_type': 'MRE-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'METHYLATION PROFILING - MEDIP-SEQ - SAMPLE', 'library_type': 'MEDIP-SEQ',
  'library_strategy': 'MEDIP-SEQ', 'experiment_type': 'MEDIP-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'METHYLATION PROFILING - MBD-SEQ - SAMPLE', 'library_type': 'MBD-SEQ',
  'library_strategy': 'MBD-SEQ', 'experiment_type': 'MBD-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'TN-SEQ', 'library_type': 'TN-SEQ',
  'library_strategy': 'TN-SEQ', 'experiment_type': 'TN-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'VALIDATION', 'library_type': 'VALIDATION',
  'library_strategy': 'VALIDATION', 'experiment_type': 'VALIDATION', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'FAIRE-SEQ', 'library_type': 'FAIRE-SEQ',
  'library_strategy': 'FAIRE-SEQ', 'experiment_type': 'FAIRE-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'SELEX', 'library_type': 'SELEX',
  'library_strategy': 'SELEX', 'experiment_type': 'SELEX', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'RIP-SEQ', 'library_type': 'RIP-SEQ',
  'library_strategy': 'RIP-SEQ', 'experiment_type': 'RIP-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIA-PET', 'library_type': 'CHIA-PET',
  'library_strategy': 'CHIA-PET', 'experiment_type': 'CHIA-PET', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'SYNTHETIC-LONG-READ', 'library_type': 'SYNTHETIC-LONG-READ',
  'library_strategy': 'SYNTHETIC-LONG-READ', 'experiment_type': 'SYNTHETIC-LONG-READ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'TARGETED CAPTURE AGILENT (PROBES PROVIDED BY COLL.) - SAMPLE', 'library_type': 'HYBRID CAPTURE - PANEL',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE: 1 TO 499KB - SAMPLE', 'library_type': 'HYBRID CAPTURE - CUSTOM',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE: 0.5 TO 2.9MB - SAMPLE', 'library_type': 'HYBRID CAPTURE - CUSTOM',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE: 3 TO 5.9MB - SAMPLE', 'library_type': 'HYBRID CAPTURE - CUSTOM',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE: 6 TO 11.9MB - SAMPLE', 'library_type': 'HYBRID CAPTURE - CUSTOM',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE: 12 TO 24MB - SAMPLE', 'library_type': 'HYBRID CAPTURE - CUSTOM',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CUSTOM TARGET CAPTURE - TRUSIGHT CARDIO - SAMPLE', 'library_type': 'HYBRID CAPTURE - PANEL',
  'library_strategy': 'TARGETED-CAPTURE', 'experiment_type': 'TARGETED-CAPTURE', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'TETHERED', 'library_type': 'TETHERED',
  'library_strategy': 'TETHERED', 'experiment_type': 'TETHERED', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'NOME-SEQ', 'library_type': 'NOME-SEQ',
  'library_strategy': 'NOME-SEQ', 'experiment_type': 'NOME-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'OTHER-SPECIFY IN COMMENT BOX', 'library_type': 'OTHER',
  'library_strategy': 'UNKNOWN', 'experiment_type': 'UNKNOWN', 'library_source': 'UNKNOWN','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIRP SEQ', 'library_type': 'CHIRP SEQ',
  'library_strategy': 'CHIRP SEQ', 'experiment_type': 'CHIRP SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': '4-C SEQ', 'library_type': '4-C SEQ',
  'library_strategy': '4-C-SEQ', 'experiment_type': '4-C-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': '5-C SEQ', 'library_type': '5-C SEQ',
  'library_strategy': '5-C-SEQ', 'experiment_type': '5-C-SEQ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'METAGENOMICS - OTHER', 'library_type': 'METAGENOMICS - OTHER',
  'library_strategy': 'WGS', 'experiment_type': 'METAGENOMIC', 'library_source': 'METAGENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'DROP-SEQ-TRANSCRIPTOME', 'library_type': 'DROP-SEQ-TRANSCRIPTOME',
  'library_strategy': 'RNA-SEQ', 'experiment_type': 'DROP-SEQ-TRANSCRIPTOME', 'library_source': 'TRANSCRIPTOMIC SINGLE CELL','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K27ME3', 'library_type': 'CHIP SEQ - H3K27ME3',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K27ME3', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K27AC', 'library_type': 'CHIP SEQ - H3K27AC',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K27AC', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K9ME3', 'library_type': 'CHIP SEQ - H3K9ME3',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K9ME3', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K36ME3', 'library_type': 'CHIP SEQ - H3K36ME3',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K36ME3', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3F3A', 'library_type': 'CHIP SEQ - H3F3A',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3F3A', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K4ME1', 'library_type': 'CHIP SEQ - H3K4ME1',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K4ME1', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K79ME2', 'library_type': 'CHIP SEQ - H3K79ME2',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K79ME2', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K79ME3', 'library_type': 'CHIP SEQ - H3K79ME3',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K79ME3', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K9ME1', 'library_type': 'CHIP SEQ - H3K9ME1',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K9ME1', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K9ME2', 'library_type': 'CHIP SEQ - H3K9ME2',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K9ME2', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H4K20ME1', 'library_type': 'CHIP SEQ - H4K20ME1',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H4K20ME1', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H2AFZ', 'library_type': 'CHIP SEQ - H2AFZ',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H2AFZ', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3AC', 'library_type': 'CHIP SEQ - H3AC',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3AC', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K4ME2', 'library_type': 'CHIP SEQ - H3K4ME2',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K4ME2', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K4ME3', 'library_type': 'CHIP SEQ - H3K4ME3',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K4ME3', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'},
 {'library_preparation': 'CHIP SEQUENCING - H3K9AC', 'library_type': 'CHIP SEQ - H3K9AC',
  'library_strategy': 'CHIP-SEQ', 'experiment_type': 'H3K9AC', 'library_source': 'GENOMIC','biomaterial_type':'UNKNOWN'}]

SPECIES_LOOKUP = \
 [{'species_text':'HUMAN','species_name':'HG38','taxon_id':9606,'scientific_name':'Homo sapiens'},
  {'species_text':'HUMAN_HG37','species_name':'HG37','taxon_id':9606,'scientific_name':'Homo sapiens'},
  {'species_text':'MOUSE','species_name':'MM10','taxon_id':10090,'scientific_name':'Mus musculus'},
  {'species_text':'MOUSE_MM9','species_name':'MM9','taxon_id':10090,'scientific_name':'Mus musculus'},
]

METADATA_COLUMNS = [
  'sample_igf_id',
  'project_igf_id',
  'name',
  'email_id',
  'sample_submitter_id',
  'experiment_type',
  'library_source',
  'library_strategy',
  'biomaterial_type',
  'expected_reads',
  'expected_lanes',
  'fragment_length_distribution_mean',
  'fragment_length_distribution_sd',
  'taxon_id',
  'scientific_name',
  'species_name'
  ]


class Reformat_metadata_file:
  '''
  A class for reformatting metadata csv files

  :param infile: Input filepath
  :param experiment_type_lookup: An experiment type lookup dictionary
  :param species_lookup: A species name lookup dictionary
  :param metadata_columns: A list of metadata columns for the reformatted files
  :param default_expected_reads: Default value for the expected read column, default 2,000,000
  :param default_expected_lanes: Default value for the expected lane columns, default 1
  :param sample_igf_id: Sample igf id column name, default 'sample_igf_id'
  :param project_igf_id: Project igf id column name, default 'project_igf_id'
  :param sample_submitter_id: Sample submitter id column name, default 'sample_submitter_id'
  :param experiment_type: Experiment type column name, default 'experiment_type'
  :param library_source: Library source column name, default 'library_source'
  :param library_strategy: Library strategy column name, default 'library_strategy'
  :param expected_reads: Expected reads column name, default 'expected_reads'
  :param expected_lanes: Expected lanes column name, default 'expected_lanes'
  :param insert_length: Insert length column name, default 'insert_length'
  :param fragment_length_distribution_mean: Fragment length column name, default 'fragment_length_distribution_mean'
  :param fragment_length_distribution_sd: Fragment length sd column name, default 'fragment_length_distribution_sd'
  :param taxon_id: Species taxon id column name, default 'taxon_id'
  :param scientific_name: Species scientific name column name, default 'scientific_name'
  :param species_name: Species genome build information column name, default 'species_name'
  :param name: Main contact information column name, default 'name'
  :param email_id: Main contact email_id information column name' default 'email_id'
  :param library_preparation: Library preparation information column from access db, default 'library_preparation'
  :param library_type: Library type information column from access db, default 'library_type'
  :param sample_type: Sample type information column from access db, default 'sample_type'
  :param sample_description: Sample description column from access db, default 'sample_description'
  :param species_text: A species text information column from access db, default 'species_text'
  :param biomaterial_type: A column name for biomaterial type, default 'biomaterial_type'
  '''
  def __init__(self,infile,experiment_type_lookup=EXPERIMENT_TYPE_LOOKUP,
               species_lookup=SPECIES_LOOKUP,
               metadata_columns=METADATA_COLUMNS,
               default_expected_reads=2000000,
               default_expected_lanes = 1,
               sample_igf_id='sample_igf_id',
               project_igf_id='project_igf_id',
               sample_submitter_id='sample_submitter_id',
               experiment_type='experiment_type',
               library_source='library_source',
               library_strategy='library_strategy',
               expected_reads='expected_reads',
               expected_lanes='expected_lanes',
               insert_length='insert_length',
               fragment_length_distribution_mean='fragment_length_distribution_mean',
               fragment_length_distribution_sd='fragment_length_distribution_sd',
               taxon_id='taxon_id',
               scientific_name='scientific_name',
               species_name='species_name',
               species_text='species_text',
               name='name',
               email_id='email_id',
               library_preparation='library_preparation',
               library_type='library_type',
               sample_type='sample_type',
               sample_description='sample_description',
               biomaterial_type='biomaterial_type'
              ):
    self.infile = infile
    self.experiment_type_lookup = pd.DataFrame(experiment_type_lookup)
    self.species_lookup = pd.DataFrame(species_lookup)
    self.metadata_columns = metadata_columns
    self.default_expected_reads = default_expected_reads
    self.default_expected_lanes = default_expected_lanes
    self.sample_igf_id = sample_igf_id
    self.project_igf_id = project_igf_id
    self.sample_submitter_id = sample_submitter_id
    self.experiment_type = experiment_type
    self.library_source = library_source
    self.library_strategy = library_strategy
    self.expected_reads = expected_reads
    self.expected_lanes = expected_lanes
    self.insert_length = insert_length
    self.fragment_length_distribution_mean = fragment_length_distribution_mean
    self.fragment_length_distribution_sd = fragment_length_distribution_sd
    self.taxon_id = taxon_id
    self.scientific_name = scientific_name
    self.species_name = species_name
    self.name = name
    self.email_id = email_id
    self.library_preparation = library_preparation
    self.library_type = library_type
    self.sample_type = sample_type
    self.sample_description = sample_description
    self.species_text = species_text
    self.biomaterial_type = biomaterial_type


  @staticmethod
  def sample_name_reformat(sample_name):
    '''
    A ststic method for reformatting sample name

    :param sample_name: A sample name string
    :returns: A string
    '''
    try:
      restricted_chars = string.punctuation
      pattern0 = re.compile(r'\s+?')
      pattern1 = re.compile('[{0}]'.format(restricted_chars))
      pattern2 = re.compile('-+')
      pattern3 = re.compile('-$')
      pattern4 = re.compile('^-')
      sample_name = \
        re.sub(pattern4,'',
          re.sub(pattern3,'',
            re.sub(pattern2,'-',
              re.sub(pattern1,'-',
                re.sub(pattern0,'-',
                  sample_name)))))
      return sample_name
    except:
      raise ValueError('Failed to reformat sample name {0}'.format(sample_name))

  @staticmethod
  def sample_and_project_reformat(tag_name):
    '''
    A static method for reformatting sample id and project name string

    :param tag_name: A sample or project name string
    :returns: A string
    '''
    try:
      restricted_chars = \
        ''.join(list(filter(lambda x: x != '_',string.punctuation)))
      pattern0 = re.compile(r'\s+?')
      pattern1 = re.compile('[{0}]'.format(restricted_chars))
      pattern2 = re.compile('-+')
      pattern3 = re.compile('-$')
      pattern4 = re.compile('^-')
      tag_name = \
        re.sub(pattern4,'',
          re.sub(pattern3,'',
            re.sub(pattern2,'-',
              re.sub(pattern1,'-',
                re.sub(pattern0,'-',
                  tag_name)))))
      return tag_name
    except:
      raise ValueError('Failed to reformat tag name {0}'.format(tag_name))


  def get_assay_info(self,library_preparation_val,sample_description_val,library_type_val):
    '''
    A method for populating library information for sample

    :param library_preparation_val: A library_preparation tag from row data
    :param sample_description_val: A sample_description tag from row data
    :param library_type_val:  A library_type tag from row data
    :returns: Three strings containing library_source,library_strategy, experiment_type and biomaterial_type information
    '''
    try:
      library_source = 'UNKNOWN'
      library_strategy = 'UNKNOWN'
      experiment_type = 'UNKNOWN'
      biomaterial_type = 'UNKNOWN'
      key = self.experiment_type
      val = 'UNKNOWN'
      if isinstance(library_preparation_val,str):
        library_preparation_val = library_preparation_val.strip().upper()
      if isinstance(sample_description_val,str):
        sample_description_val = sample_description_val.strip().upper()
      if isinstance(library_type_val,str):
        library_type_val = library_type_val.strip().upper()
      if library_preparation_val == 'NOT APPLICABLE' and \
         sample_description_val == 'PRE MADE LIBRARY':
        key=self.library_type
        val=library_type_val
      elif library_preparation_val != 'NOT APPLICABLE' and \
           library_preparation_val != '':
        key=self.library_preparation
        val=library_preparation_val

      filtered_data = self.experiment_type_lookup[self.experiment_type_lookup[key]==val]
      if len(filtered_data.index) > 1:
        filtered_data = pd.DataFrame([filtered_data.iloc[0]])

      records = filtered_data.to_dict(orient='records')
      if len(records) > 0:
        library_source = records[0].get(self.library_source) or 'UNKNOWN'
        library_strategy = records[0].get(self.library_strategy) or 'UNKNOWN'
        experiment_type = records[0].get(self.experiment_type) or 'UNKNOWN'
        biomaterial_type = records[0].get(self.biomaterial_type) or 'UNKNOWN'

      return library_source,library_strategy,experiment_type,biomaterial_type
    except Exception as e:
      raise ValueError('Failed to return assay information for type: {0},{1},{2} error: {3}'.\
              format(library_preparation_val,sample_description_val,library_type_val,e))

  @staticmethod
  def calculate_insert_length_from_fragment(fragment_length,adapter_length=120):
    '''
    A static method for calculating insert length from fragment length information

    :param fragment_length: A int value for average fragment size
    :param adapter_length: Adapter length, default 120
    '''
    try:
      insert_length = 0
      if fragment_length == '':
        insert_length = 0
      else:
        fragment_length = float(str(fragment_length).strip().replace(',',''))
        if fragment_length != '' or \
           fragment_length >= 0:
          insert_length = int(fragment_length - adapter_length)
        if insert_length < 0:
          insert_length = 0
      return insert_length
    except Exception as e:
      raise ValueError('Failed to calculate insert length: {0}'.format(e))

  def get_species_info(self,species_text_val):
    '''
    A method for fetching species taxon infor and scientific name from a lookup dictionary

    :param species_text_val: Species information as string
    :returns: Two strings, one for taxon_id, scientific name and for species_name
    '''
    try:
      taxon_id = 'UNKNOWN'
      scientific_name = 'UNKNOWN'
      species_name = 'UNKNOWN'

      records = self.species_lookup[self.species_lookup[self.species_text]==species_text_val.upper()]
      records = records.to_dict(orient='records')
      if len(records) > 0:
        taxon_id = records[0].get(self.taxon_id) or 'UNKNOWN'
        scientific_name = records[0].get(self.scientific_name) or 'UNKNOWN'
        species_name = records[0].get(self.species_name) or 'UNKNOWN'

      return str(taxon_id),scientific_name,species_name
    except Exception as e:
      raise ValueError('Failed to get species information: {0}'.format(e))

  def populate_metadata_values(self,row):
    '''
    A method for populating metadata row

    :param row: A Pandas Series
    :returns: A Pandas Series
    '''
    try:
      if not isinstance(row,pd.Series):
        raise TypeError('Expecting a pandas series and got {0}'.format(type(row)))

      if self.sample_igf_id in row.keys():
        row[self.sample_igf_id] = \
        self.sample_and_project_reformat(\
          tag_name=row[self.sample_igf_id])

      if self.project_igf_id in row.keys():
        row[self.project_igf_id] = \
          self.sample_and_project_reformat(\
            tag_name=row[self.project_igf_id])

      if self.sample_submitter_id in row.keys():
        row[self.sample_submitter_id] = \
          self.sample_name_reformat(\
            sample_name=row[self.sample_submitter_id])

      if self.library_preparation in row.keys() and \
         self.library_type in row.keys() and \
         self.sample_description in row.keys():
        row[self.library_source],row[self.library_strategy],row[self.experiment_type],biomaterial_type = \
          self.get_assay_info(\
            library_preparation_val=row.get(self.library_preparation),
            sample_description_val=row.get(self.sample_description),
            library_type_val=row.get(self.library_type))
        if self.biomaterial_type in row.keys() and \
           (row[self.biomaterial_type] == '' or \
            row[self.biomaterial_type].upper() == 'UNKNOWN') and \
           biomaterial_type != 'UNKNOWN':
          row[self.biomaterial_type] = biomaterial_type

      if self.species_name in row.keys():
        row[self.taxon_id],row[self.scientific_name],row[self.species_name] = \
          self.get_species_info(\
            species_text_val=row[self.species_text])

      #if self.fragment_length_distribution_mean in row.keys():
      #  if (row[self.insert_length] == 0 or row[self.insert_length] == '' ) and \
      #     (row[self.fragment_length_distribution_mean] != '' or \
      #      row[self.fragment_length_distribution_mean] != 0):
      #    row[self.insert_length] = \
      #      self.calculate_insert_length_from_fragment(\
      #        fragment_length=row[self.fragment_length_distribution_mean])

      if self.species_text in row.keys():
        row[self.taxon_id],row[self.scientific_name],row[self.species_name] = \
          self.get_species_info(\
            species_text_val=row[self.species_text])

      if self.expected_reads in row.keys() and \
        (row[self.expected_reads] == '' or row[self.expected_reads] == 0):
        row[self.expected_reads] = self.default_expected_reads

      if self.expected_lanes in row.keys() and \
        (row[self.expected_lanes] == '' or row[self.expected_lanes] == 0):
        row[self.expected_lanes] = self.default_expected_lanes


      return row
    except Exception as e:
      raise ValueError('Failed to reformat row: {0}, error: {1}'.format(row,e))

  def reformat_raw_metadata_file(self,output_file):
    '''
    A method for reformatting raw metadata file and print a corrected output

    :param output_file: An output filepath
    :returns: None
    '''
    try:
      try:
        data = pd.read_csv(self.infile,dtype=object,header=0)
      except Exception as e:
        raise ValueError('Failed to parse input file {0}, error {1}'.format(self.infile,e))

      for field in self.metadata_columns:
        if field not in data.columns:
          if field in ('expected_reads',
                       'expected_lanes',
                       'insert_length',
                       'fragment_length_distribution_mean',
                       'fragment_length_distribution_sd'):
            data[field] = 0
          else:
            data[field] = ''

      data = \
        data.\
        fillna('').\
        apply(lambda x: \
          self.populate_metadata_values(row=x),
          axis=1,
          result_type='reduce')                                                 # update metadata info

      if self.expected_lanes in data.columns:
        data[self.expected_lanes] = \
          data[self.expected_lanes].\
            astype(float).\
              astype(int)                                                       # expected_lanes should be int

      for field in self.metadata_columns:
        total_row_count = data[field].count()
        empty_keys = 0
        counts = data[field].value_counts().to_dict()
        for key,val in counts.items():
          if key in ('unknown','UNKNOWN',''):
            empty_keys += val

        if total_row_count == empty_keys:
          data.drop(field,axis=1,inplace=True)                                  # clean up empty columns

      column_names = \
        [column_name \
          for column_name in data.columns \
            if column_name in self.metadata_columns]                            # filter output columns
      if len(column_names) == 0:
        raise ValueError('No target metadata column found on the reformatted data')

      data[column_names].to_csv(output_file,index=False)                        # filter columns and print new metadata file
    except Exception as e:
      raise ValueError('Failed to remormat file {0}, error {1}'.format(self.infile,e))