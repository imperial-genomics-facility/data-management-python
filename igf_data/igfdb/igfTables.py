import datetime
from igf_data.igfdb.datatype import JSONType
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy import Column, String, Enum, TIMESTAMP, TEXT, ForeignKey, UniqueConstraint


Base = declarative_base()

class Project(Base):

  '''
  A table for loading project information

  :param project_id: An integer id for project table
  :param project_igf_id: A required string as project id specific to IGF team, allowed length 50
  :param project_name: An optional string as project name
  :param start_timestamp: An optional timestamp for project creation, default current timestamp
  :param description: An optional text column to document project description
  :param deliverable: An enum list to document project deliverable, default FASTQ,allowed entries are

    * FASTQ
    * ALIGNMENT
    * ANALYSIS

  :param status: An enum list for project status, default ACTIVE, allowed entries are

    * ACTIVE
    * FINISHED
    * WITHDRAWN
  '''
  __tablename__ = 'project'
  __table_args__ = (
     UniqueConstraint('project_igf_id'),
     { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_igf_id = Column(String(50), nullable=False)
  project_name = Column(String(40))
  start_timestamp = Column(TIMESTAMP(), nullable=True, server_default=current_timestamp())
  description = Column(TEXT())
  status = Column(Enum('ACTIVE', 'FINISHED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  deliverable = Column(Enum('FASTQ', 'ALIGNMENT', 'ANALYSIS'), server_default='FASTQ')
  projectuser = relationship('ProjectUser', backref="project")
  sample = relationship('Sample', backref="project")
  experiment = relationship('Experiment', backref="project")
  project_attribute = relationship('Project_attribute', backref="project")

  def __repr__(self):
    '''
    Display Project entry
    '''
    return \
      "Project(project_id = '{self.project_id}', " \
      "project_igf_id = '{self.project_igf_id}'," \
      "project_name = '{self.project_name}'," \
      "start_timestamp = '{self.start_timestamp}'," \
      "status = '{self.status}',"\
      "description = '{self.description}'," \
      "deliverable = '{self.deliverable}')".format(self=self)


class User(Base):

  '''
  A table for loading user information

  :param user_id: An integer id for user table
  :param user_igf_id: An optional string as user id specific to IGF team, allowed length 10
  :param name: A required string as user name, allowed length 30
  :param email_id: A required string as email id, allowed length 40
  :param username: A required string as IGF username, allowed length 20
  :param hpc_username: An optional string as Imperial College's HPC login name, allowed length 20
  :param twitter_user: An optional string as twitter user name, allowed length 20
  :param category: An optional enum list as user category, default NON_HPC_USER, allowed values are

    * HPC_USER
    * NON_HPC_USER
    * EXTERNAL

  :param status: An optional enum list as user status, default is ACTIVE, allowed values are

    * ACTIVE
    * BLOCKED
    * WITHDRAWN

  :param date_created: An optional timestamp, default current timestamp
  :param password: An optional string field to store encrypted password
  :param encryption_salt: An optional string field to store encryption salt
  :param ht_password: An optional field to store password for htaccess
  '''
  __tablename__ = 'user'
  __table_args__ = (
    UniqueConstraint('name'),
    UniqueConstraint('username'),
    UniqueConstraint('email_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  user_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  user_igf_id = Column(String(10))
  name = Column(String(30), nullable=False)
  email_id = Column(String(40), nullable=False)
  username = Column(String(20))
  hpc_username = Column(String(20))
  twitter_user = Column(String(20))
  orcid_id = Column(String(50))
  category = Column(Enum('HPC_USER','NON_HPC_USER','EXTERNAL'), nullable=False, server_default='NON_HPC_USER')
  status = Column(Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  password = Column(String(129))
  encryption_salt = Column(String(129))
  ht_password = Column(String(40))
  projectuser = relationship('ProjectUser', backref="user")

  def __repr__(self):
    '''
    Display User entry
    '''
    return \
      "User(user_id = '{self.user_id}'," \
      "user_igf_id = '{self.user_igf_id}'," \
      "name = '{self.name}', " \
      "username = '{self.username}'," \
      "hpc_username = '{self.hpc_username}'," \
      "twitter_user = '{self.twitter_user}, " \
      "orcid_id = '{self.orcid_id}, " \
      "category = '{self.category}'," \
      "status = '{self.status}'," \
      "email_id = '{self.email_id}'," \
      "date_created = '{self.date_created}'," \
      "password = '{self.password}'," \
      "ht_password = '{self.ht_password}'," \
      "encryption_salt = '{self.encryption_salt}')".format(self=self)


class ProjectUser(Base):

  '''
  A table for linking users to the projects

  :param project_user_id: An integer id for project_user table
  :param project_id: An integer id for project table (foreign key)
  :param user_id: An integer id for user table (foreign key)
  :param data_authority: An optional enum value to denote primary user for the project,
                          allowed value T
  '''
  __tablename__ = 'project_user'
  __table_args__ = (
    UniqueConstraint('project_id','data_authority'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_user_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_id = Column(INTEGER(unsigned=True), ForeignKey("project.project_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  user_id = Column(INTEGER(unsigned=True), ForeignKey("user.user_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  data_authority = Column(Enum('T'))

  def __repr__(self):
    '''
    Display ProjectUser entry
    '''
    return \
      "ProjectUser(project_user_id = '{self.project_user_id}'," \
      "project_id = '{self.project_id}'," \
      "user_id = '{self.user_id}'," \
      "data_authority = '{self.data_authority}')".format(self=self)


class Sample(Base):

  '''
  A table for loading sample information

  :param sample_id: An integer id for sample table
  :param sample_igf_id: A required string as sample id specific to IGF team, allowed length 20
  :param sample_submitter_id: An optional string as sample name from user, allowed value 40
  :param taxon_id: An optional integer NCBI taxonomy information for sample
  :param scientific_name: An optional string as scientific name of the species
  :param species_name: An optional string as the species name (genome build code) information
  :param donor_anonymized_id: An optional string as anonymous donor name
  :param description: An optional string as sample description
  :param phenotype: An optional string as sample phenotype information
  :param sex: An optional enum list to specify sample sex, default UNKNOWN, allowed values are

    * FEMALE
    * MALE
    * MIXED
    * UNKNOWN

  :param status: An optional enum list to specify sample status, default ACTIVE, allowed values are

    * ACTIVE
    * FAILED
    * WITHDRAWS

  :param biomaterial_type: An optional enum list as sample biomaterial type, default UNKNOWN, allowed values are

    * PRIMARY_TISSUE
    * PRIMARY_CELL
    * PRIMARY_CELL_CULTURE
    * CELL_LINE
    * SINGLE_NUCLEI
    * UNKNOWN

  :param cell_type: An optional string to specify sample cell_type information, if biomaterial_type is PRIMARY_CELL or PRIMARY_CELL_CULTURE
  :param tissue_type: An optional string to specify sample tissue information, if biomaterial_type is PRIMARY_TISSUE
  :param cell_line: An optional string to specify cell line information ,if biomaterial_type is CELL_LINE
  :param date_created: An optional timestamp column to specify entry creation date, default current timestamp
  :param project_id:  An integer id for project table (foreign key)
  '''
  __tablename__ = 'sample'
  __table_args__ = (
    UniqueConstraint('sample_igf_id'),
    { 'mysql_engine':'InnoDB','mysql_charset':'utf8' })

  sample_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  sample_igf_id = Column(String(20), nullable=False)
  sample_submitter_id = Column(String(40))
  taxon_id = Column(INTEGER(unsigned=True))
  scientific_name = Column(String(50))
  species_name = Column(String(50))
  donor_anonymized_id = Column(String(10))
  description = Column(String(50))
  phenotype = Column(String(45))
  sex = Column(Enum('FEMALE', 'MALE', 'MIXED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  biomaterial_type = Column(Enum('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE', 'SINGLE_NUCLEI', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  cell_type = Column(String(50))
  tissue_type = Column(String(50))
  cell_line = Column(String(50))
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  project_id = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  experiment = relationship('Experiment', backref="sample")
  sample_attribute = relationship('Sample_attribute', backref="sample")

  def __repr__(self):
    '''
    Display Sample entry
    '''
    return \
      "Sample(sample_id = '{self.sample_id}'," \
      "sample_igf_id = '{self.sample_igf_id}'," \
      "sample_submitter_id = '{self.sample_submitter_id}', " \
      "taxon_id = '{self.taxon_id}'," \
      "scientific_name = '{self.scientific_name}'," \
      "species_name = '{self.species_name}'," \
      "donor_anonymized_id = '{self.donor_anonymized_id}'," \
      "description = '{self.description}'," \
      "phenotype = '{self.phenotype}'," \
      "sex = '{self.sex}'," \
      "status = '{self.status}'," \
      "biomaterial_type = '{self.biomaterial_type}'," \
      "cell_type = '{self.cell_type}'," \
      "tissue_type = '{self.tissue_type}'," \
      "cell_line = '{self.cell_line}'," \
      "date_created = '{self.date_created}'," \
      "project_id = '{self.project_id}')".format(self=self)


class Platform(Base):

  '''
  A table for loading sequencing platform information

  :param platform_id: An integer id for platform table
  :param platform_igf_id: A required string as platform id specific to IGF team, allowed length 10
  :param model_name: A required enum list to specify platform model, allowed values are

    * HISEQ2500
    * HISEQ4000
    * MISEQ
    * NEXTSEQ
    * NOVASEQ6000
    * NANOPORE_MINION
    * DNBSEQ-G400
    * DNBSEQ-G50
    * DNBSEQ-T7

  :param vendor_name: A required enum list to specify vendor's name, allowed values are

    * ILLUMINA
    * NANOPORE
    * MGI

  :param software_name: A required enum list for specifying platform software, allowed values are

    * RTA
    * UNKNOWN

  :param software_version: A optional software version number, default is UNKNOWN
  :param date_created: An optional timestamp column to record entry creation time, default current timestamp
  '''
  __tablename__ = 'platform'
  __table_args__ = (
    UniqueConstraint('platform_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  platform_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  platform_igf_id = Column(String(10), nullable=False)
  model_name = Column(Enum('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ', 'NEXTSEQ2000','NOVASEQ6000','NANOPORE_MINION','DNBSEQ-G400', 'DNBSEQ-G50', 'DNBSEQ-T7', 'SEQUEL2'), nullable=False)
  vendor_name = Column(Enum('ILLUMINA','NANOPORE', 'MGI', 'PACBIO'), nullable=False)
  software_name = Column(Enum('RTA','UNKNOWN'), nullable=False)
  software_version = Column(String(20), nullable=False, server_default='UNKNOWN')
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now )
  seqrun = relationship('Seqrun', backref="platform")
  flowcell_rule = relationship('Flowcell_barcode_rule', backref="platform")

  def __repr__(self):
    '''
    Display Platform entry
    '''
    return \
      "Platform(platform_id = '{self.platform_id}'," \
      "platform_igf_id = '{self.platform_igf_id}'," \
      "model_name = '{self.model_name}'," \
      "vendor_name = '{self.vendor_name}'," \
      "software_name = '{self.software_name}'," \
      "software_version = '{self.software_version}'," \
      "date_created = '{self.date_created}')".format(self=self)


class Flowcell_barcode_rule(Base):

  '''
  A table for loading flowcell specific barcode rules information

  :param flowcell_rule_id: An integer id for flowcell_barcode_rule table
  :param platform_id: An integer id for platform table (foreign key)
  :param flowcell_type: A required string as flowcell type name, allowed length 50
  :param index_1: An optional enum list as index_1 specific rule, default UNKNOWN, allowed values are

    * NO_CHANGE
    * REVCOMP
    * UNKNOWN

  :param index_2: An optional enum list as index_2 specific rule, default UNKNOWN, allowed values are

    * NO_CHANGE
    * REVCOMP
    * UNKNOWN
  '''
  __tablename__ = 'flowcell_barcode_rule'
  __table_args__ = (
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  flowcell_rule_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  platform_id = Column(INTEGER(unsigned=True), ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  flowcell_type = Column(String(50), nullable=False)
  index_1 = Column(Enum('NO_CHANGE','REVCOMP','UNKNOWN'), nullable=False, server_default='UNKNOWN')
  index_2 = Column(Enum('NO_CHANGE','REVCOMP','UNKNOWN'), nullable=False, server_default='UNKNOWN')

  def __repr__(self):
    '''
    Display Flowcell_barcode_rule entry
    '''
    return \
      "Flowcell_barcode_rule(flowcell_rule_id  = '{self.flowcell_rule_id}'," \
      "platform_id = '{self.platform_id}'," \
      "flowcell_type = '{self.flowcell_type}'," \
      "index_1 = '{self.index_1}'," \
      "index_2 = '{self.index_2}')".format(self=self)


class Seqrun(Base):

  '''
  A table for loading sequencing run information

  :param seqrun_id: An integer id for seqrun table
  :param seqrun_igf_id: A required string as seqrun id specific to IGF team, allowed length 50
  :param reject_run: An optional enum list to specify rejected run information ,default N,
                      allowed values Y and N
  :param date_created: An optional timestamp column to record entry creation time, default current timestamp
  :param flowcell_id: A required string column for storing flowcell_id information, allowed length 20
  :param platform_id: An integer platform id (foreign key)
  '''
  __tablename__ = 'seqrun'
  __table_args__ = (
    UniqueConstraint('seqrun_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  seqrun_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seqrun_igf_id = Column(String(50), nullable=False)
  reject_run = Column(Enum('Y','N'), nullable=False, server_default='N')
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  flowcell_id = Column(String(20), nullable=False)
  platform_id = Column(INTEGER(unsigned=True), ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  run = relationship('Run', backref="seqrun")
  seqrun_stats = relationship('Seqrun_stats', backref="seqrun")
  seqrun_attribute = relationship('Seqrun_attribute', backref='seqrun')

  def __repr__(self):
    '''
    Display Seqrun entry
    '''
    return \
      "Seqrun(seqrun_id = '{self.seqrun_id}'," \
      "seqrun_igf_id = '{self.seqrun_igf_id}'," \
      "reject_run = '{self.reject_run}'," \
      "flowcell_id = '{self.flowcell_id}'," \
      "date_created = '{self.date_created}'," \
      "platform_id = '{self.platform_id}')".format(self=self)


class Seqrun_stats(Base):

  '''
  A table for loading sequencing stats information

  :param seqrun_stats_id: An integer id for seqrun_stats table
  :param seqrun_id: An integer seqrun id (foreign key)
  :param lane_number: A required enum list for specifying lane information,
                       allowed values are 1, 2, 3, 4, 5, 6, 7 and 8
  :param bases_mask: An optional string field for storing bases mask information
  :param undetermined_barcodes: An optional json field to store barcode info for undetermined samples
  :param known_barcodes: An optional json field to store barcode info for known samples
  :param undetermined_fastqc: An optional json field to store qc info for undetermined samples
  '''
  __tablename__  = 'seqrun_stats'
  __table_args__ = (
    UniqueConstraint('seqrun_id', 'lane_number'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  seqrun_stats_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seqrun_id = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  lane_number = Column(Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  bases_mask = Column(String(20))
  undetermined_barcodes = Column(JSONType)
  known_barcodes = Column(JSONType)
  undetermined_fastqc = Column(JSONType)

  def __repr__(self):
    '''
    Display Seqrun_stats entry
    '''
    return \
      "Seqrun_stats(seqrun_stats_id = '{self.seqrun_stats_id}'," \
      "seqrun_id = '{self.seqrun_id}'," \
      "lane_number = '{self.lane_number}'," \
      "bases_mask = '{self.bases_mask}'," \
      "undetermined_barcodes = '{self.undetermined_barcodes}'," \
      "known_barcodes = '{self.known_barcodes}'," \
      "undetermined_fastqc = '{self.undetermined_fastqc}')".format(self=self)


class Experiment(Base):

  '''
  A table for loading experiment (unique combination of sample, library and platform) information.

  :param experiment_id: An integer id for experiment table
  :param experiment_igf_id: A required string as experiment id specific to IGF team, allowed length 40
  :param project_id: A required integer id from project table (foreign key)
  :param sample_id: A required integer id from sample table (foreign key)
  :param library_name: A required string to specify library name, allowed length 50
  :param library_source: An optional enum list to specify library source information, default is UNKNOWN, allowed values are

    * GENOMIC
    * TRANSCRIPTOMIC
    * GENOMIC_SINGLE_CELL
    * TRANSCRIPTOMIC_SINGLE_CELL
    * METAGENOMIC
    * METATRANSCRIPTOMIC
    * SYNTHETIC
    * VIRAL_RNA
    * UNKNOWN

  :param library_strategy: An optional enum list to specify library strategy information, default is UNKNOWN, allowed values are

    * WGS
    * WXS
    * WGA
    * RNA-SEQ
    * CHIP-SEQ
    * ATAC-SEQ
    * MIRNA-SEQ
    * NCRNA-SEQ
    * FL-CDNA
    * EST
    * HI-C
    * DNASE-SEQ
    * WCS
    * RAD-SEQ
    * CLONE
    * POOLCLONE
    * AMPLICON
    * CLONEEND
    * FINISHING
    * MNASE-SEQ
    * DNASE-HYPERSENSITIVITY
    * BISULFITE-SEQ
    * CTS
    * MRE-SEQ
    * MEDIP-SEQ
    * MBD-SEQ
    * TN-SEQ
    * VALIDATION
    * FAIRE-SEQ
    * SELEX
    * RIP-SEQ
    * CHIA-PET
    * SYNTHETIC-LONG-READ
    * TARGETED-CAPTURE
    * TETHERED
    * NOME-SEQ
    * CHIRP SEQ
    * 4-C-SEQ
    * 5-C-SEQ
    * UNKNOWN

  :param experiment_type: An optional enum list as experiment type information, default is UNKNOWN, allowed values are

    * POLYA-RNA
    * POLYA-RNA-3P
    * TOTAL-RNA
    * SMALL-RNA
    * WGS
    * WGA
    * WXS
    * WXS-UTR
    * RIBOSOME-PROFILING
    * RIBODEPLETION
    * 16S
    * NCRNA-SEQ
    * FL-CDNA
    * EST
    * HI-C
    * DNASE-SEQ
    * WCS
    * RAD-SEQ
    * CLONE
    * POOLCLONE
    * AMPLICON
    * CLONEEND
    * FINISHING
    * DNASE-HYPERSENSITIVITY
    * RRBS-SEQ
    * WGBS
    * CTS
    * MRE-SEQ
    * MEDIP-SEQ
    * MBD-SEQ
    * TN-SEQ
    * VALIDATION
    * FAIRE-SEQ
    * SELEX
    * RIP-SEQ
    * CHIA-PET
    * SYNTHETIC-LONG-READ
    * TARGETED-CAPTURE
    * TETHERED
    * NOME-SEQ
    * CHIRP-SEQ
    * 4-C-SEQ
    * 5-C-SEQ
    * METAGENOMIC
    * METATRANSCRIPTOMIC
    * TF
    * H3K27ME3
    * H3K27AC
    * H3K9ME3
    * H3K36ME3
    * H3F3A
    * H3K4ME1
    * H3K79ME2
    * H3K79ME3
    * H3K9ME1
    * H3K9ME2
    * H4K20ME1
    * H2AFZ
    * H3AC
    * H3K4ME2
    * H3K4ME3
    * H3K9AC
    * HISTONE-NARROW
    * HISTONE-BROAD
    * CHIP-INPUT
    * ATAC-SEQ
    * TENX-TRANSCRIPTOME-3P
    * TENX-TRANSCRIPTOME-5P
    * DROP-SEQ-TRANSCRIPTOME
    * UNKNOWN

  :param library_layout: An optional enum list to specify library layout, default is UNONWN, allowed values are

    * SINGLE
    * PAIRED
    * UNKNOWN

  :param status: An optional enum list to specify experiment status, default is ACTIVE, allowed values are

    * ACTIVE
    * FAILED
    * WITHDRAWN

  :param date_created: An optional timestamp column to record entry creation or modification time, default current timestamp
  :param platform_name: An optional enum list to specify platform model, default is UNKNOWN, allowed values are

    * HISEQ250
    * HISEQ4000
    * MISEQ
    * NEXTSEQ
    * NOVASEQ6000
    * NANOPORE_MINION
    * DNBSEQ-G400
    * DNBSEQ-G50
    * DNBSEQ-T7
    * NEXTSEQ2000
    * SEQUEL2
    * UNKNOWN
  '''
  __tablename__ = 'experiment'
  __table_args__ = (
    UniqueConstraint('sample_id', 'library_name', 'platform_name'),
    UniqueConstraint('experiment_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  experiment_igf_id = Column(String(40), nullable=False)
  project_id = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  sample_id = Column(INTEGER(unsigned=True), ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="SET NULL"))
  library_name = Column(String(50), nullable=False)
  library_source = Column(Enum('GENOMIC', 'TRANSCRIPTOMIC' ,'GENOMIC_SINGLE_CELL', 'METAGENOMIC', 'METATRANSCRIPTOMIC',
                               'TRANSCRIPTOMIC_SINGLE_CELL', 'SYNTHETIC', 'VIRAL_RNA', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_strategy = Column(Enum('WGS', 'WXS', 'WGA', 'RNA-SEQ', 'CHIP-SEQ', 'ATAC-SEQ', 'MIRNA-SEQ', 'NCRNA-SEQ',
                                 'FL-CDNA', 'EST', 'HI-C', 'DNASE-SEQ', 'WCS', 'RAD-SEQ', 'CLONE', 'POOLCLONE',
                                 'AMPLICON', 'CLONEEND', 'FINISHING', 'MNASE-SEQ', 'DNASE-HYPERSENSITIVITY', 'BISULFITE-SEQ',
                                 'CTS', 'MRE-SEQ', 'MEDIP-SEQ', 'MBD-SEQ', 'TN-SEQ', 'VALIDATION', 'FAIRE-SEQ', 'SELEX',
                                 'RIP-SEQ', 'CHIA-PET', 'SYNTHETIC-LONG-READ', 'TARGETED-CAPTURE', 'TETHERED', 'NOME-SEQ',
                                 'CHIRP SEQ', '4-C-SEQ', '5-C-SEQ', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment_type = Column(Enum('POLYA-RNA', 'POLYA-RNA-3P', 'TOTAL-RNA', 'SMALL-RNA', 'WGS', 'WGA', 'WXS', 'WXS-UTR',
                                'RIBOSOME-PROFILING', 'RIBODEPLETION', '16S', 'NCRNA-SEQ', 'FL-CDNA', 'EST', 'HI-C',
                                'DNASE-SEQ', 'WCS', 'RAD-SEQ', 'CLONE', 'POOLCLONE', 'AMPLICON', 'CLONEEND', 'FINISHING',
                                'DNASE-HYPERSENSITIVITY', 'RRBS-SEQ', 'WGBS', 'CTS', 'MRE-SEQ', 'MEDIP-SEQ', 'MBD-SEQ',
                                'TN-SEQ', 'VALIDATION', 'FAIRE-SEQ', 'SELEX', 'RIP-SEQ', 'CHIA-PET', 'SYNTHETIC-LONG-READ',
                                'TARGETED-CAPTURE', 'TETHERED', 'NOME-SEQ', 'CHIRP-SEQ', '4-C-SEQ', '5-C-SEQ',
                                'METAGENOMIC', 'METATRANSCRIPTOMIC', 'TF', 'H3K27ME3', 'H3K27AC', 'H3K9ME3',
                                'H3K36ME3', 'H3F3A', 'H3K4ME1', 'H3K79ME2', 'H3K79ME3', 'H3K9ME1', 'H3K9ME2',
                                'H4K20ME1', 'H2AFZ', 'H3AC', 'H3K4ME2', 'H3K4ME3', 'H3K9AC', 'HISTONE-NARROW',
                                'HISTONE-BROAD', 'CHIP-INPUT', 'ATAC-SEQ', 'TENX-TRANSCRIPTOME-3P', 'TENX-TRANSCRIPTOME-5P',
                                'DROP-SEQ-TRANSCRIPTOME', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_layout = Column(Enum('SINGLE', 'PAIRED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  platform_name = Column(Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ', 'NANOPORE_MINION', 'NOVASEQ6000',
                              'DNBSEQ-G400', 'DNBSEQ-G50', 'DNBSEQ-T7', 'NEXTSEQ2000', 'SEQUEL2',
                              'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment = relationship('Run', backref='experiment')
  experiment_attribute = relationship('Experiment_attribute', backref='experiment')

  def __repr__(self):
    '''
    Display Experiment entry
    '''
    return \
      "Experiment(experiment_id = '{self.experiment_id}'," \
      "experiment_igf_id = '{self.experiment_igf_id}'," \
      "project_id = '{self.project_id}'," \
      "sample_id = '{self.sample_id}'," \
      "library_name = '{self.library_name}'," \
      "library_source = '{self.library_source}'," \
      "library_strategy = '{self.library_strategy}'," \
      "experiment_type = '{self.experiment_type}'," \
      "library_layout = '{self.library_layout}'," \
      "status = '{self.status}'," \
      "date_created = '{self.date_created}'," \
      "platform_name = '{self.platform_name}')".format(self=self)


class Run(Base):

  '''
  A table for loading run (unique combination of experiment, sequencing flowcell and lane) information

  :param run_id: An integer id for run table
  :param run_igf_id: A required string as run id specific to IGF team, allowed length 70
  :param experiment_id: A required integer id from experiment table (foreign key)
  :param seqrun_id: A required integer id from seqrun table (foreign key)
  :param status: An optional enum list to specify experiment status, default is ACTIVE, allowed values are 

    * ACTIVE
    * FAILED
    * WITHDRAWN

  :param lane_number: A required enum list for specifying lane information,
                       allowed values 1, 2, 3, 4, 5, 6, 7 and 8
  :param date_created: An optional timestamp column to record entry creation time, default current timestamp
  '''
  __tablename__ = 'run'
  __table_args__ = (
    UniqueConstraint('run_igf_id'),
    UniqueConstraint('experiment_id','seqrun_id','lane_number'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  run_igf_id = Column(String(70), nullable=False)
  experiment_id = Column(INTEGER(unsigned=True), ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="SET NULL"))
  seqrun_id = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="SET NULL"))
  status = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  lane_number = Column(Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  run_attribute = relationship('Run_attribute', backref='run')

  def __repr__(self):
    '''
    Display Run entry
    '''
    return \
      "Run(run_id = '{self.run_id}'," \
      "experiment_id = '{self.experiment_id}'," \
      "run_igf_id = '{self.run_igf_id}'," \
      "seqrun_id = '{self.seqrun_id}'," \
      "status = '{self.status}'," \
      "lane_number = '{self.lane_number}'," \
      "date_created = '{self.date_created}')".format(self=self)


class Analysis(Base):

  '''
  A table for loading analysis design information

  :param analysis_id: An integer id for analysis table
  :param project_id: A required integer id from project table (foreign key)
  :param analysis_type: An optional string field of 120chrs to specify analysis type
  :param analysis_description: An optional json description for analysis
  '''
  __tablename__ = 'analysis'
  __table_args__ = (
    UniqueConstraint('analysis_name','project_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  analysis_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_id = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  analysis_name = Column(String(120), nullable=False)
  analysis_type = Column(String(120), nullable=False)
  analysis_description = Column(JSONType)

  def __repr__(self):
    '''
    Display Analysis entry
    '''
    return \
      "Analysis(analysis_id = '{self.analysis_id}'," \
      "project_id = '{self.project_id}'," \
      "analysis_name = '{self.analysis_name}'," \
      "analysis_type = '{self.analysis_type}'," \
      "analysis_description = '{self.analysis_description}')".format(self=self)


class Collection(Base):

  '''
  A table for loading collection information

  :param collection_id: An integer id for collection table
  :param name: A required string to specify collection name, allowed length 70
  :param type: A required string to specify collection type, allowed length 50
  :param table: An optional enum list to specify collection table information, default unknown,
                 allowed values are sample, experiment, run, file, project, seqrun and unknown
  :param date_stamp: An optional timestamp column to record entry creation or modification time, default current timestamp
  '''
  __tablename__ = 'collection'
  __table_args__ = (
    UniqueConstraint('name','type'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  name = Column(String(70), nullable=False)
  type = Column(String(50), nullable=False)
  table = Column(Enum('sample', 'experiment', 'run', 'file', 'project', 'seqrun', 'analysis', 'unknown'), nullable=False, server_default='unknown')
  date_stamp = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  collection_group = relationship('Collection_group', backref='collection')
  collection_attribute = relationship('Collection_attribute', backref='collection')

  def __repr__(self):
    '''
    Display Collection entry
    '''
    return \
      "Collection(collection_id = '{self.collection_id}'," \
      "name = '{self.name}'," \
      "type = '{self.type}'," \
      "table = '{self.table}'," \
      "date_stamp = '{self.date_stamp}')".format(self=self)


class File(Base):

  '''
  A table for loading file information

  :param file_id: An integer id for file table
  :param file_path: A required string to specify file path information, allowed length 500
  :param location: An optional enum list to specify storage location, default UNKNOWN, allowed values are

    * ORWELL
    * HPC_PROJECT
    * ELIOT
    * IRODS
    * UNKNOWN

  :param status: An optional enum list to specify experiment status, default is ACTIVE, allowed values are

    * ACTIVE
    * FAILED
    * WITHDRAWN

  :param md5: An optional string to specify file md5 value, allowed length 33
  :param size: An optional string to specify file size, allowed value 15
  :param date_created: An optional timestamp column to record file creation time, default current timestamp
  :param date_updated: An optional timestamp column to record file modification time, default current timestamp
  '''
  __tablename__ = 'file'
  __table_args__ = (
    UniqueConstraint('file_path'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  file_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  file_path = Column(String(500), nullable=False)
  location = Column(Enum('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status = Column(Enum('ACTIVE', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  md5 = Column(String(33))
  size = Column(String(15))
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  date_updated = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now )
  collection_group = relationship('Collection_group', backref='file')
  file_attribute = relationship('File_attribute', backref='file')

  def __repr__(self):
    '''
    Display File entry
    '''
    return \
      "File(file_id = '{self.file_id}'," \
      "file_path = '{self.file_path}'," \
      "location = '{self.location}'," \
      "status = '{self.status}'," \
      "md5 = '{self.md5}'," \
      "size = '{self.size}'," \
      "date_created = '{self.date_created}'," \
      "date_updated = '{self.date_updated}')".format(self=self)


class Collection_group(Base):

  '''
  A table for linking files to the collection entries

  :param collection_group_id: An integer id for collection_group table
  :param collection_id: A required integer id from collection table (foreign key)
  :param file_id: A required integer id from file table (foreign key)
  '''
  __tablename__ = 'collection_group'
  __table_args__ = (
    UniqueConstraint('collection_id','file_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_group_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  collection_id = Column(INTEGER(unsigned=True), ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  file_id = Column(INTEGER(unsigned=True), ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Collection_group entry
    '''
    return \
      "Collection_group(collection_group_id = '{self.collection_group_id}'," \
      "collection_id = '{self.collection_id}'," \
      "file_id = '{self.file_id}')".format(self=self)


class Pipeline(Base):

  '''
  A table for loading pipeline information

  :param pipeline_id: An integer id for pipeline table
  :param pipeline_name: A required string to specify pipeline name, allowed length 50
  :param pipeline_db: A required string to specify pipeline database url, allowed length 200
  :param pipeline_init_conf: An optional json field to specify initial pipeline configuration
  :param pipeline_run_conf: An optional json field to specify modified pipeline configuration
  :param pipeline_type: An optional enum list to specify pipeline type, default EHIVE, allowed values are

    * EHIVE
    * UNKNOWN
    * AIRFLOW
    * NEXTFLOW

  :param is_active: An optional enum list to specify the status of pipeline, default Y
                    allowed values are Y and N
  :param date_stamp: An optional timestamp column to record file creation or modification time, default current timestamp
  '''
  __tablename__ = 'pipeline'
  __table_args__ = (
    UniqueConstraint('pipeline_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  pipeline_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  pipeline_name = Column(String(120), nullable=False)
  pipeline_db = Column(String(200), nullable=False)
  pipeline_init_conf = Column(JSONType)
  pipeline_run_conf = Column(JSONType)
  pipeline_type = Column(Enum('EHIVE', 'AIRFLOW', 'NEXTFLOW', 'UNKNOWN'), nullable=False, server_default='EHIVE')
  is_active = Column(Enum('Y', 'N'), nullable=False, server_default='Y')
  date_stamp = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  pipeline_seed = relationship('Pipeline_seed', backref='pipeline')

  def __repr__(self):
    '''
    Display Pipeline entry
    '''
    return \
      "Pipeline(pipeline_id = '{self.pipeline_id}'," \
      "pipeline_name = '{self.pipeline_name}'," \
      "pipeline_db = '{self.pipeline_db}'," \
      "pipeline_init_conf = '{self.pipeline_init_conf}'," \
      "pipeline_run_conf = '{self.pipeline_run_conf}'," \
      "pipeline_type = '{self.pipeline_type}'," \
      "is_active = '{self.is_active}'," \
      "date_stamp = '{self.date_stamp}')".format(self=self)


class Pipeline_seed(Base):

  '''
  A table for loading pipeline seed information

  :param pipeline_seed_id: An integer id for pipeline_seed table
  :param seed_id: A required integer id
  :param seed_table: An optional enum list to specify seed table information, default unknown,
                      allowed values project, sample, experiment, run, file, seqrun, collection and unknown
  :param pipeline_id: An integer id from pipeline table (foreign key)
  :param status: An optional enum list to specify the status of pipeline, default UNKNOWN,
                  allowed values are

    * SEEDED
    * RUNNING
    * FINISHED
    * FAILED
    * UNKNOWN

  :param date_stamp: An optional timestamp column to record file creation or modification time, default current timestamp
  '''
  __tablename__ = 'pipeline_seed'
  __table_args__ = (
    UniqueConstraint('pipeline_id','seed_id','seed_table'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  pipeline_seed_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seed_id = Column(INTEGER(unsigned=True), nullable=False)
  seed_table = Column(Enum('project','sample','experiment','run','file','seqrun','analysis','collection','unknown'), nullable=False, server_default='unknown')
  pipeline_id = Column(INTEGER(unsigned=True), ForeignKey('pipeline.pipeline_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  status = Column(Enum('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  date_stamp = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)

  def __repr__(self):
    '''
    Display Pipeline_seed entry
    '''
    return \
      "Pipeline_seed(pipeline_seed_id = '{self.pipeline_seed_id}'," \
      "seed_id = '{self.seed_id}'," \
      "seed_table = '{self.seed_table}'," \
      "pipeline_id = '{self.pipeline_id}'," \
      "status = '{self.status}'," \
      "date_stamp = '{self.date_stamp}')".format(self=self)

class History(Base):

  '''
  A table for loading history information

  :param log_id: An integer id for history table
  :param log_type: A required enum value to specify log type, allowed values are

    * CREATED
    * MODIFIED
    * DELETED

  :param table_name: A required enum value to specify table information, allowed values are

    * PROJECT
    * USER
    * SAMPLE
    * EXPERIMENT
    * RUN
    * COLLECTION
    * FILE
    * PLATFORM
    * PROJECT_ATTRIBUTE
    * EXPERIMENT_ATTRIBUTE
    * COLLECTION_ATTRIBUTE
    * SAMPLE_ATTRIBUTE
    * RUN_ATTRIBUTE
    * FILE_ATTRIBUTE

  :param log_date: An optional timestamp column to record file creation or modification time, default current timestamp
  :param message: An optional text field to specify message
  '''
  __tablename__ = 'history'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  log_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  log_type = Column(Enum('CREATED', 'MODIFIED', 'DELETED'), nullable=False)
  table_name = Column(Enum('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE'), nullable=False)
  log_date = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  message = Column(TEXT())

  def __repr__(self):
    '''
    Display History entry
    '''
    return \
      "History(log_id = '{self.log_id}'," \
      "log_type = '{self.log_type}'," \
      "table_name = '{self.table_name}'," \
      "log_date = '{self.log_date}'," \
      "message = '{self.message}')".format(self=self)


class Project_attribute(Base):

  '''
  A table for loading project attributes

  :param project_attribute_id: An integer id for project_attribute table
  :param attribute_name: An optional string attribute name, allowed length 50
  :param attribute_value: An optional string attribute value, allowed length 50
  :param project_id: An integer id from project table (foreign key)
  '''
  __tablename__ = 'project_attribute'
  __table_args__ = (
    UniqueConstraint('project_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(50))
  attribute_value = Column(String(50))
  project_id = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Project_attribute entry
    '''
    return \
      "Project_attribute(project_attribute_id = '{self.project_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "project_id = '{self.project_id}')".format(self=self)


class Experiment_attribute(Base):

  '''
  A table for loading experiment attributes

  :param experiment_attribute_id: An integer id for experiment_attribute table
  :param attribute_name: An optional string attribute name, allowed length 30
  :param attribute_value: An optional string attribute value, allowed length 50
  :param experiment_id: An integer id from experiment table (foreign key)
  '''
  __tablename__ = 'experiment_attribute'
  __table_args__ = (
    UniqueConstraint('experiment_id', 'attribute_name', 'attribute_value'),
    {  'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(30))
  attribute_value = Column(String(50))
  experiment_id = Column(INTEGER(unsigned=True), ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Experiment_attribute entry
    '''
    return \
      "Experiment_attribute(experiment_attribute_id = '{self.experiment_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "experiment_id = '{self.experiment_id}')".format(self=self)


class Collection_attribute(Base):

  '''
  A table for loading collection attributes

  :param collection_attribute_id: An integer id for collection_attribute table
  :param attribute_name: An optional string attribute name, allowed length 200
  :param attribute_value: An optional string attribute value, allowed length 200
  :param collection_id: An integer id from collection table (foreign key)
  '''
  __tablename__ = 'collection_attribute'
  __table_args__ = (
    UniqueConstraint('collection_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(200))
  attribute_value = Column(String(200))
  collection_id = Column(INTEGER(unsigned=True), ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Collection_attribute entry
    '''
    return \
      "Collection_attribute(collection_attribute_id = '{self.collection_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "collection_id = '{self.collection_id}')".format(self=self)


class Sample_attribute(Base):

  '''
  A table for loading sample attributes

  :param sample_attribute_id: An integer id for sample_attribute table
  :param attribute_name: An optional string attribute name, allowed length 50
  :param attribute_value: An optional string attribute value, allowed length 50
  :param sample_id: An integer id from sample table (foreign key)
  '''
  __tablename__ = 'sample_attribute'
  __table_args__ = (
    UniqueConstraint('sample_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  sample_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(50))
  attribute_value = Column(String(50))
  sample_id = Column(INTEGER(unsigned=True), ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Sample_attribute entry
    '''
    return \
      "Sample_attribute(sample_attribute_id = '{self.sample_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "sample_id = '{self.sample_id}')".format(self=self)


class Seqrun_attribute(Base):

  '''
  A table for loading seqrun attributes

  :param seqrun_attribute_id: An integer id for seqrun_attribute table
  :param attribute_name: An optional string attribute name, allowed length 50
  :param attribute_value: An optional string attribute value, allowed length 100
  :param seqrun_id: An integer id from seqrun table (foreign key)
  '''
  __tablename__ = 'seqrun_attribute'
  __table_args__ = (
    UniqueConstraint('seqrun_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
  seqrun_attribute_id  = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(50))
  attribute_value = Column(String(100))
  seqrun_id = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)


class Run_attribute(Base):

  '''
  A table for loading run attributes

  :param run_attribute_id: An integer id for run_attribute table
  :param attribute_name: An optional string attribute name, allowed length 30
  :param attribute_value: An optional string attribute value, allowed length 50
  :param run_id: An integer id from run table (foreign key)
  '''
  __tablename__ = 'run_attribute'
  __table_args__ = (
    UniqueConstraint('run_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_attribute_id  = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(30))
  attribute_value = Column(String(50))
  run_id = Column(INTEGER(unsigned=True), ForeignKey('run.run_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display Run_attribute entry
    '''
    return \
      "Run_attribute(run_attribute_id = '{self.run_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "run_id = '{self.run_id}')".format(self=self)


class File_attribute(Base):

  '''
  A table for loading file attributes

  :param file_attribute_id: An integer id for file_attribute table
  :param attribute_name: An optional string attribute name, allowed length 30
  :param attribute_value: An optional string attribute value, allowed length 50
  :param file_id: An integer id from file table (foreign key)
  '''
  __tablename__ = 'file_attribute'
  __table_args__ = (
    UniqueConstraint('file_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  file_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name = Column(String(30))
  attribute_value = Column(String(50))
  file_id = Column(INTEGER(unsigned=True), ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    '''
    Display File_attribute entry
    '''
    return \
      "File_attribute(file_attribute_id = '{self.file_attribute_id}'," \
      "attribute_name = '{self.attribute_name}'," \
      "attribute_value = '{self.attribute_value}'," \
      "file_id = '{self.file_id}')".format(self=self)