import re,string
import pandas as pd

EXPERIMENT_TYPE_LOOKUP = \
  {'POLYA-RNA':{'library_source':'TRANSCRIPTOMIC','library_strategy':'RNA-SEQ'},
   'TOTAL-RNA':{'library_source':'TRANSCRIPTOMIC','library_strategy':'RNA-SEQ'},
   'SMALL-RNA':{'library_source':'TRANSCRIPTOMIC','library_strategy':'RNA-SEQ'},
   'TENX-TRANSCRIPTOME':{'library_source':'TRANSCRIPTOMIC_SINGLE_CELL','library_strategy':'RNA-SEQ'},
   'DROP-SEQ-TRANSCRIPTOME':{'library_source':'TRANSCRIPTOMIC_SINGLE_CELL','library_strategy':'RNA-SEQ'},
   'WGS':{'library_source':'GENOMIC','library_strategy':'WGS'},
   'EXOME':{'library_source':'GENOMIC','library_strategy':'EXOME'},
   'ATAC-SEQ':{'library_source':'GENOMIC','library_strategy':'ATAC-SEQ'},
   'TF':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'HISTONE-NARROW':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'HISTONE-BROAD':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K27ME3':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K27AC':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K9ME3':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K36ME3':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3F3A':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K4ME1':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K79ME2':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K79ME3':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K9ME1':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K9ME2':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H4K20ME1':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H2AFZ':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3AC':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K4ME2':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K4ME3':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'},
   'H3K9AC':{'library_source':'GENOMIC','library_strategy':'CHIP-SEQ'}
  }

SPECIES_LOOKUP = \
 {'HG38':{'taxon_id':9606,'scientific_name':'Homo sapiens'},
  'HG37':{'taxon_id':9606,'scientific_name':'Homo sapiens'},
  'MM10':{'taxon_id':10090,'scientific_name':'Mus musculus'},
  'MM9':{'taxon_id':10090,'scientific_name':'Mus musculus'},
  }

METADATA_COLUMNS = [
  'sample_igf_id',
  'project_igf_id',
  'sample_submitter_id',
  'experiment_type',
  'library_source',
  'library_strategy',
  'expected_reads',
  'expected_lanes',
  'insert_length',
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
  :param metadata_columns: A list of metadata columns
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
  '''
  def __init__(self,infile,experiment_type_lookup,
               species_lookup,metadata_columns,
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
               species_name='species_name'
              ):
    self.infile = infile
    self.experiment_type_lookup = experiment_type_lookup
    self.species_lookup = species_lookup
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


  def get_assay_info(self,experiment_type):
    '''
    A method for populating library information for sample

    :param experiment_type: A valid experiment_type tag from lookup table
    :returns: Two strings containing library_source and library_strategy information
    '''
    try:
      library_source = 'UNKNOWN'
      library_strategy = 'UNKNOWN'
      if experiment_type in self.experiment_type_lookup:
        library_source = self.experiment_type_lookup.get(experiment_type).get(self.library_source) or 'UNKNOWN'
        library_strategy = self.experiment_type_lookup.get(experiment_type).get(self.library_strategy) or 'UNKNOWN'

      return library_source,library_strategy
    except Exception as e:
      raise ValueError('Failed to return assay information for exp type: {0}, error: {1}'.format(experiment_type,e))

  @staticmethod
  def calculate_insert_length_from_fragment(fragment_length,adapter_length=120):
    '''
    A static method for calculating insert length from fragment length information

    :param fragment_length: A int value for average fragment size
    :param adapter_length: Adapter length, default 120
    '''
    try:
      insert_length = None
      fragment_length = float(str(fragment_length).strip().replace(',',''))
      insert_length = int(fragment_length - adapter_length)
      return insert_length
    except Exception as e:
      raise ValueError('Failed to calculate insert length: {0}'.format(e))

  def get_species_info(self,genome_build):
    '''
    A method for fetching species taxon infor and scientific name from a lookup dictionary

    :param genome_build: Species genome build info string
    :returns: Two strings, one for taxon_id andother for scientific name
    '''
    try:
      taxon_id = 'UNKNOWN'
      scientific_name = 'UNKNOWN'
      if genome_build in self.species_lookup:
        taxon_id = self.species_lookup.get(genome_build).get(self.taxon_id) or 'UNKNOWN'
        scientific_name = self.species_lookup.get(genome_build).get(self.scientific_name) or 'UNKNOWN'

      return str(taxon_id),scientific_name
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

      if self.experiment_type in row.keys():
        row[self.library_source],row[self.library_strategy] = \
          self.get_assay_info(\
            experiment_type=row[self.experiment_type])

      if self.species_name in row.keys():
        row[self.taxon_id],row[self.scientific_name] = \
          self.get_species_info(\
            genome_build=row[self.species_name])

      if self.fragment_length_distribution_mean in row.keys():
        if (row[self.insert_length] == 0 or row[self.insert_length] == '' ) and \
           (row[self.fragment_length_distribution_mean] != '' or \
            row[self.fragment_length_distribution_mean] != 0):
          row[self.insert_length] = \
            self.calculate_insert_length_from_fragment(\
              fragment_length=row[self.fragment_length_distribution_mean])

      if self.species_name in row.keys():
        row[self.taxon_id],row[self.scientific_name] = \
          self.get_species_info(\
            genome_build=row[self.species_name])

      if self.expected_reads in row.keys() and \
        (row[self.expected_reads] == '' or row[self.expected_reads] == 0):
        row[self.expected_reads] = self.default_expected_reads

      if self.expected_lanes in row.keys() and \
        (row[self.expected_lanes] == '' or row[self.expected_lanes] == 0):
        row[self.expected_lanes] = self.default_expected_lanes

      if self.experiment_type in row.keys():
        row[self.library_source],row[self.library_strategy] = \
          self.get_assay_info(\
            experiment_type=row[self.experiment_type])

      return row
    except Exception as e:
      raise ValueError('Failed to remormat row: {0}, error: {1}'.format(row,e))

  def reformat_raw_metadata_file(self,output_file):
    '''
    A method for reformatting raw metadata file and print a corrected output

    :param output_file: An output filepath
    :returns: None
    '''
    try:
      try:
        data = pd.read_csv(self.infile)
      except Exception as e:
        raise ValueError('Failed to parse input file {0}, error {1}'.format(self.infile,e))

      for field in self.metadata_columns:
        if field not in data.columns:
          data[field] = ''

      data = \
        data.\
        fillna('').\
        apply(lambda x: \
          self.populate_metadata_values(row=x),
          axis=1,
          result_type='reduce')                                                 # update metadata info

      for field in self.metadata_columns:
        total_row_count = data[field].count()
        empty_keys = 0
        counts = data[field].value_counts().to_dict()
        for key,val in counts.items():
          if key in ('UNKNOWN',''):
            empty_keys += val

        if total_row_count == empty_keys:
          data.drop(field,axis=1,inplace=True)                                  # clean up empty columns

      data.to_csv(output_file,index=False)                                      # print new metadata file
    except Exception as e:
      raise ValueError('Failed to remormat file {0}, error {1}'.format(self.infile,e))