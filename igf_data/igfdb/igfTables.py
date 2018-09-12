import datetime
from igf_data.igfdb.datatype import JSONType
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy import Table, Column, String, Enum, TIMESTAMP, TEXT, ForeignKey, text, DATE, create_engine, ForeignKeyConstraint, UniqueConstraint


Base = declarative_base()

class Project(Base):
  '''
  A table for loading project information
  
  :column project_id: An integer id for project table
  :column project_igf_id: A required string as project id specific to IGF team, allowed length 50
  :column project_name: An optional string as project name
  :column start_timestamp: An optional timestamp for project creation, default current timestamp
  :column description: An optional text column to document project description
  :column deliverable: An enum list to document project deliverable, default FASTQ,
                       allowed entries are FASTQ, ALIGNMENT and ANALYSIS
  '''
  __tablename__ = 'project'
  __table_args__ = (
     UniqueConstraint('project_igf_id'),
     { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_id      = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_igf_id  = Column(String(50), nullable=False)
  project_name    = Column(String(40))
  start_timestamp = Column(TIMESTAMP(), nullable=True, server_default=current_timestamp())
  description     = Column(TEXT())
  deliverable     = Column(Enum('FASTQ', 'ALIGNMENT', 'ANALYSIS'), server_default='FASTQ')
  projectuser       = relationship('ProjectUser', backref="project")
  sample            = relationship('Sample', backref="project")
  experiment        = relationship('Experiment', backref="project")
  project_attribute = relationship('Project_attribute', backref="project")

  def __repr__(self):
    return "Project(project_id = '{self.project_id}', " \
                    "project_igf_id = '{self.project_igf_id}'," \
                    "project_name = '{self.project_name}'," \
                    "start_timestamp = '{self.start_timestamp}'," \
                    "description = '{self.description}'," \
                    "deliverable = '{self.deliverable}')".format(self=self)
                   

class User(Base):
  '''
  A table for loading user information
  
  :column user_id: An integer id for user table
  :column user_igf_id: An optional string as user id specific to IGF team, allowed length 10
  :column name: A required string as user name, allowed length 30
  :column email_id: A required string as email id, allowed length 40
  :column username: A required string as IGF username, allowed length 20
  :column hpc_username: An optional string as Imperial College's HPC login name, allowed length 20
  :column twitter_user: An optional string as twitter user name, allowed length 20
  :column category: An optional enum list as user category, default NON_HPC_USER,
                    allowed values are HPC_USER, NON_HPC_USER and EXTERNAL
  :column status: An optional enum list as user status, default is ACTIVE,
                  allowed values are ACTIVE, BLOCKED and WITHDRAWN
  :column date_created: An optional timestamp, default current timestamp
  :column password: An optional string field to store encrypted password
  :column encryption_salt: An optional string field to store encryption salt
  :column ht_password: An optional field to store password for htaccess
  '''
  __tablename__ = 'user'
  __table_args__ = (
    UniqueConstraint('name'),
    UniqueConstraint('username'),
    UniqueConstraint('email_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  user_id         = Column(INTEGER(unsigned=True), primary_key=True, nullable=False) 
  user_igf_id     = Column(String(10))
  name            = Column(String(30), nullable=False)
  email_id        = Column(String(40), nullable=False)
  username        = Column(String(20))
  hpc_username    = Column(String(20))
  twitter_user    = Column(String(20))
  category        = Column(Enum('HPC_USER','NON_HPC_USER','EXTERNAL'), nullable=False, server_default='NON_HPC_USER')
  status          = Column(Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created    = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  password        = Column(String(129))
  encryption_salt = Column(String(129))
  ht_password     = Column(String(40))
  projectuser     = relationship('ProjectUser', backref="user")
  
  def __repr__(self):
    return "User(user_id = '{self.user_id}'," \
                "user_igf_id = '{self.user_igf_id}'," \
                "name = '{self.name}', " \
                "username = '{self.username}'," \
                "hpc_username = '{self.hpc_username}'," \
                "twitter_user = '{self.twitter_user}, " \
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
  
  :column project_user_id: An integer id for project_user table
  :column project_id: An integer id for project table (foreign key)
  :column user_id: An integer id for user table (foreign key)
  :column data_authority: An optional enum value to denote primary user for the project,
                          allowed value T
  '''
  __tablename__ = 'project_user'
  __table_args__ = (
    UniqueConstraint('project_id','data_authority'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
    
  project_user_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_id      = Column(INTEGER(unsigned=True), ForeignKey("project.project_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  user_id         = Column(INTEGER(unsigned=True), ForeignKey("user.user_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  data_authority  = Column(Enum('T'))

  def __repr__(self):
    return "ProjectUser(project_user_id = '{self.project_user_id}'," \
                       "project_id = '{self.project_id}'," \
                       "user_id = '{self.user_id}'," \
                       "data_authority = '{self.data_authority}')".format(self=self)


class Sample(Base):
  '''
  A table for loading sample information
  
  :column sample_id: An integer id for sample table
  :column sample_igf_id: A required string as sample id specific to IGF team, allowed length 20
  :column sample_submitter_id: An optional string as sample name from user, allowed value 40
  :column taxon_id: An optional integer NCBI taxonomy information for sample
  :column scientific_name: An optional string as scientific name of the species
  :column species_name: An optional string as the species name (genome build code) information
  :column donor_anonymized_id: An optional string as anonymous donor name
  :column description: An optional string as sample description
  :column phenotype: An optional string as sample phenotype information
  :column sex: An optional enum list to specify sample sex, default UNKNOWN
               allowed values are FEMALE, MALE, MIXED and UNKNOWN
  :column status: An optional enum list to specify sample status, default ACTIVE,
                  allowed values are ACTIVE, FAILED and WITHDRAWS
  :column biomaterial_type: An optional enum list as sample biomaterial type, default UNKNOWN,
                            allowed values are PRIMARY_TISSUE, PRIMARY_CELL, PRIMARY_CELL_CULTURE, CELL_LINE and UNKNOWN
  :column cell_type: An optional string to specify sample cell_type information, if biomaterial_type is PRIMARY_CELL or PRIMARY_CELL_CULTURE
  :column tissue_type: An optional string to specify sample tissue information, if biomaterial_type is PRIMARY_TISSUE
  :column cell_line: An optional string to specify cell line information ,if biomaterial_type is CELL_LINE
  :column date_created: An optional timestamp column to specify entry creation date, default current timestamp
  :column project_id:  An integer id for project table (foreign key)
  '''
  __tablename__ = 'sample'
  __table_args__ = (
    UniqueConstraint('sample_igf_id'),
    { 'mysql_engine':'InnoDB','mysql_charset':'utf8' })

  sample_id           = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  sample_igf_id       = Column(String(20), nullable=False)
  sample_submitter_id = Column(String(40))
  taxon_id            = Column(INTEGER(unsigned=True))
  scientific_name     = Column(String(50))
  species_name        = Column(String(50))
  donor_anonymized_id = Column(String(10))
  description         = Column(String(50))
  phenotype           = Column(String(45))
  sex                 = Column(Enum('FEMALE', 'MALE', 'MIXED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status              = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  biomaterial_type    = Column(Enum('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  cell_type           = Column(String(50))
  tissue_type         = Column(String(50))
  cell_line           = Column(String(50))
  date_created        = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  project_id          = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  experiment          = relationship('Experiment', backref="sample")
  sample_attribute    = relationship('Sample_attribute', backref="sample")

  def __repr__(self):
    return "Sample(sample_id = '{self.sample_id}'," \
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
  
  :column platform_id: An integer id for platform table
  :column platform_igf_id: A required string as platform id specific to IGF team, allowed length 10
  :column model_name: A required enum list to specify platform model, allowed values are HISEQ2500,
                      HISEQ4000, MISEQ, NEXTSEQ, NOVASEQ6000 and NANOPORE_MINION
  :column vendor_name: A required enum list to specify vendor's name, allowed values are 
                       ILLUMINA and NANOPORE
  :column software_name: A required enum list for specifying platform software, allowed values are
                         RTA and UNKNOWN
  :column software_version: A required enum list for specifying the software version number,
                            allowed values are RTA1.18.54, RTA1.18.64, RTA2 and UNKNOWN
  :column date_created: An optional timestamp column to record entry creation time, default current timestamp
  '''
  __tablename__ = 'platform'
  __table_args__ = (
    UniqueConstraint('platform_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  platform_id      = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  platform_igf_id  = Column(String(10), nullable=False)
  model_name       = Column(Enum('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ','NOVASEQ6000','NANOPORE_MINION'), nullable=False)
  vendor_name      = Column(Enum('ILLUMINA','NANOPORE'), nullable=False)
  software_name    = Column(Enum('RTA','UNKNOWN'), nullable=False)
  software_version = Column(Enum('RTA1.18.54','RTA1.18.64','RTA2','UNKNOWN'), nullable=False)
  date_created     = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now )
  seqrun           = relationship('Seqrun', backref="platform")
  flowcell_rule    = relationship('Flowcell_barcode_rule', backref="platform")

  def __repr__(self):
    return "Platform(platform_id = '{self.platform_id}'," \
                    "platform_igf_id = '{self.platform_igf_id}'," \
                    "model_name = '{self.model_name}'," \
                    "vendor_name = '{self.vendor_name}'," \
                    "software_name = '{self.software_name}'," \
                    "software_version = '{self.software_version}'," \
                    "date_created = '{self.date_created}')".format(self=self)

class Flowcell_barcode_rule(Base):
  '''
  A table for loading flowcell specific barcode rules information
  
  :column flowcell_rule_id: An integer id for flowcell_barcode_rule table
  :column platform_id: An integer id for platform table (foreign key)
  :column flowcell_type: A required string as flowcell type name, allowed length 50
  :column index_1: An optional enum list as index_1 specific rule, default UNKNOWN,
                   allowed values NO_CHANGE, REVCOMP and UNKNOWN
  :column index_2: An optional enum list as index_2 specific rule, default UNKNOWN,
                   allowed values NO_CHANGE, REVCOMP and UNKNOWN
  '''
  __tablename__ = 'flowcell_barcode_rule'
  __table_args__ = (
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  flowcell_rule_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  platform_id      = Column(INTEGER(unsigned=True), ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  flowcell_type    = Column(String(50), nullable=False)
  index_1          = Column(Enum('NO_CHANGE','REVCOMP','UNKNOWN'), nullable=False, server_default='UNKNOWN')
  index_2          = Column(Enum('NO_CHANGE','REVCOMP','UNKNOWN'), nullable=False, server_default='UNKNOWN')

  def __repr__(self):
    return "Flowcell_barcode_rule(flowcell_rule_id  = '{self.flowcell_rule_id}'," \
                                 "platform_id = '{self.platform_id}'," \
                                 "flowcell_type = '{self.flowcell_type}'," \
                                 "index_1 = '{self.index_1}'," \
                                 "index_2 = '{self.index_2}')".format(self=self)


class Seqrun(Base):
  '''
  A table for loading sequencing run information
  
  :column seqrun_id: An integer id for seqrun table
  :column seqrun_igf_id: A required string as seqrun id specific to IGF team, allowed length 50
  :column reject_run: An optional enum list to specify rejected run information ,default N,
                      allowed values Y and N
  :column date_created: An optional timestamp column to record entry creation time, default current timestamp
  :column flowcell_id: A required string column for storing flowcell_id information, allowed length 20
  :column platform_id: An integer platform id (foreign key)
  '''
  __tablename__ = 'seqrun'
  __table_args__ = (
    UniqueConstraint('seqrun_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  seqrun_id       = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seqrun_igf_id   = Column(String(50), nullable=False)
  reject_run      = Column(Enum('Y','N'), nullable=False, server_default='N')
  date_created    = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  flowcell_id     = Column(String(20), nullable=False)
  platform_id     = Column(INTEGER(unsigned=True), ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  run               = relationship('Run', backref="seqrun")
  seqrun_stats      = relationship('Seqrun_stats', backref="seqrun")
  seqrun_attribute  = relationship('Seqrun_attribute', backref='seqrun')

  def __repr__(self):
    return "Seqrun(seqrun_id = '{self.seqrun_id}'," \
                        "seqrun_igf_id = '{self.seqrun_igf_id}'," \
                        "reject_run = '{self.reject_run}'," \
                        "flowcell_id = '{self.flowcell_id}'," \
                        "date_created = '{self.date_created}'," \
                        "platform_id = '{self.platform_id}')".format(self=self)


class Seqrun_stats(Base):
  '''
  A table for loading sequencing stats information
  
  :column seqrun_stats_id: An integer id for seqrun_stats table
  :column seqrun_id: An integer seqrun id (foreign key)
  :column lane_number: A required enum list for specifying lane information,
                       allowed values 1, 2, 3, 4, 5, 6, 7 and 8
  :column bases_mask: An optional string field for storing bases mask information
  :column undetermined_barcodes: An optional json field to store barcode info for undetermined samples
  :column known_barcodes: An optional json field to store barcode info for known samples
  :column undetermined_fastqc: An optional json field to store qc info for undetermined samples
  '''
  __tablename__  = 'seqrun_stats'
  __table_args__ = (
    UniqueConstraint('seqrun_id', 'lane_number'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  seqrun_stats_id       = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seqrun_id             = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  lane_number           = Column(Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  bases_mask            = Column(String(20))
  undetermined_barcodes = Column(JSONType)
  known_barcodes        = Column(JSONType)
  undetermined_fastqc   = Column(JSONType)
  
  def __repr__(self):
    return "Seqrun_stats(seqrun_stats_id = '{self.seqrun_stats_id}'," \
                   "seqrun_id = '{self.seqrun_id}'," \
                   "lane_number = '{self.lane_number}'," \
                   "bases_mask = '{self.bases_mask}'," \
                   "undetermined_barcodes = '{self.undetermined_barcodes}'," \
                   "known_barcodes = '{self.known_barcodes}'," \
                   "undetermined_fastqc = '{self.undetermined_fastqc}')".format(self=self)


class Experiment(Base):
  '''
  A table for loading experiment (unique combination of sample, library and platform) information.
  
  :column experiment_id: An integer id for experiment table
  :column experiment_igf_id: A required string as experiment id specific to IGF team, allowed length 40
  :column project_id: A required integer id from project table (foreign key)
  :column sample_id: A required integer id from sample table (foreign key)
  :column library_name: A required string to specify library name, allowed length 50
  :column library_source: An optional enum list to specify library source information, default is UNKNOWN,
                          allowed values are GENOMIC, TRANSCRIPTOMIC, GENOMIC_SINGLE_CELL, TRANSCRIPTOMIC_SINGLE_CELL
                          and UNKNOWN
  :column library_strategy: An optional enum list to specify library strategy information, default is UNKNOWN,
                            allowed values are WGS, EXOME, RNA-SEQ, CHIP-SEQ, ATAC-SEQ and UNKNOWN
  :column experiment_type: An optional enum list as experiment type information, default is UNKNOWN,
                           allowed values are POLYA-RNA, TOTAL-RNA, SMALL-RNA, H3K4ME3, WGS, EXOME,
                           H3K27ME3, H3K27AC, H3K9ME3, H3K36ME3, HISTONE-NARROW, HISTONE-BROAD, ATAC-SEQ,
                           TENX-TRANSCRIPTOME, DROP-SEQ-TRANSCRIPTOME, TF and UNKNOWN
  :column library_layout: An optional enum list to specify library layout, default is UNONWN
                          allowed values are SINGLE, PAIRED and UNKNOWN
  :column status: An optional enum list to specify experiment status, default is ACTIVE,
                  allowed values are ACTIVE, FAILED and WITHDRAWN
  :column date_created: An optional timestamp column to record entry creation or modification time, default current timestamp
  :column platform_name: An optional enum list to specify platform model, default is UNKNOWN,
                         allowed values are HISEQ2500, HISEQ4000, MISEQ, NEXTSEQ NANOPORE_MINION and UNKNOWN
  '''
  __tablename__ = 'experiment'
  __table_args__ = (
    UniqueConstraint('sample_id', 'library_name', 'platform_name'),
    UniqueConstraint('experiment_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_id     = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  experiment_igf_id = Column(String(40), nullable=False)
  project_id        = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  sample_id         = Column(INTEGER(unsigned=True), ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="SET NULL"))
  library_name      = Column(String(50), nullable=False)
  library_source    = Column(Enum('GENOMIC', 'TRANSCRIPTOMIC' ,'GENOMIC_SINGLE_CELL', 'TRANSCRIPTOMIC_SINGLE_CELL', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_strategy  = Column(Enum('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ', 'ATAC-SEQ', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment_type   = Column(Enum('POLYA-RNA','TOTAL-RNA','SMALL-RNA','H3K4ME3','WGS','EXOME','H3K27ME3','H3K27AC','H3K9ME3','H3K36ME3','HISTONE-NARROW','HISTONE-BROAD','ATAC-SEQ','TENX-TRANSCRIPTOME','DROP-SEQ-TRANSCRIPTOME','TF','UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_layout    = Column(Enum('SINGLE', 'PAIRED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status            = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created      = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  platform_name     = Column(Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ', 'NANOPORE_MINION', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment            = relationship('Run', backref='experiment')
  experiment_attribute  = relationship('Experiment_attribute', backref='experiment')

  def __repr__(self):
    return "Experiment(experiment_id = '{self.experiment_id}'," \
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
                      "platform_name = '{self.platform_id}')".format(self=self)


class Run(Base):
  '''
  A table for loading run (unique combination of experiment, sequencing flowcell and lane) information
  
  :column run_id: An integer id for run table
  :column run_igf_id: A required string as run id specific to IGF team, allowed length 70
  :column experiment_id: A required integer id from experiment table (foreign key)
  :column seqrun_id: A required integer id from seqrun table (foreign key)
  :column status: An optional enum list to specify experiment status, default is ACTIVE,
                  allowed values are ACTIVE, FAILED and WITHDRAWN
  :column lane_number: A required enum list for specifying lane information,
                       allowed values 1, 2, 3, 4, 5, 6, 7 and 8
  :column date_created: An optional timestamp column to record entry creation time, default current timestamp
  '''
  __tablename__ = 'run'
  __table_args__ = (
    UniqueConstraint('run_igf_id'),
    UniqueConstraint('experiment_id','seqrun_id','lane_number'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_id        = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  run_igf_id    = Column(String(70), nullable=False)
  experiment_id = Column(INTEGER(unsigned=True), ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="SET NULL"))
  seqrun_id     = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="SET NULL"))
  status        = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  lane_number   = Column(Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  date_created  = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  run_attribute = relationship('Run_attribute', backref='run')

  def __repr__(self):
    return "Run(run_id = '{self.run_id}'," \
               "experiment_id = '{self.experiment_id}'," \
               "run_igf_id = '{self.run_igf_id}'," \
               "seqrun_id = '{self.seqrun_id}'," \
               "status = '{self.status}'," \
               "lane_number = '{self.lane_number}'," \
               "date_created = '{self.date_created}')".format(self=self)
  
class Analysis(Base):
  '''
  A table for loading analysis design information
  
  :column analysis_id: An integer id for analysis table
  :column project_id: A required integer id from project table (foreign key)
  :column analysis_type: An optional enum list to specify analysis type, default is UNKNOWN,
                         allowed values are RNA_DIFFERENTIAL_EXPRESSION, RNA_TIME_SERIES,
                         CHIP_PEAK_CALL, SOMATIC_VARIANT_CALLING and UNKNOWN
  :column analysis_description: An optional json description for analysis
  '''
  __tablename__ = 'analysis'
  __table_args__ = (
    UniqueConstraint('project_id', 'analysis_type'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  analysis_id          = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_id           = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  analysis_type        = Column(Enum('RNA_DIFFERENTIAL_EXPRESSION','RNA_TIME_SERIES','CHIP_PEAK_CALL','SOMATIC_VARIANT_CALLING','UNKNOWN'), nullable=False, server_default='UNKNOWN')
  analysis_description = Column(JSONType)

  def __repr__(self):
    return "Analysis(analysis_id = '{self.analysis_id}'," \
                    "project_id = '{self.project_id}'," \
                    "analysis_type = '{self.analysis_type}'," \
                    "analysis_description = '{self.analysis_description}')".format(self=self)


class Collection(Base):
  '''
  A table for loading collection information
  
  :column collection_id: An integer id for collection table
  :column name: A required string to specify collection name, allowed length 70
  :column type: A required string to specify collection type, allowed length 50
  :column table: An optional enum list to specify collection table information, default unknown,
                 allowed values are sample, experiment, run, file, project, seqrun and unknown
  :column date_stamp: An optional timestamp column to record entry creation or modification time, default current timestamp
  '''
  __tablename__ = 'collection'
  __table_args__ = (
    UniqueConstraint('name','type'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  name          = Column(String(70), nullable=False)
  type          = Column(String(50), nullable=False)
  table         = Column(Enum('sample', 'experiment', 'run', 'file', 'project', 'seqrun','unknown'), nullable=False, server_default='unknown')
  date_stamp    = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now) 
  collection_group      = relationship('Collection_group', backref='collection')
  collection_attribute  = relationship('Collection_attribute', backref='collection')

  def __repr__(self):
    return "Collection(collection_id = '{self.collection_id}'," \
                      "name = '{self.name}'," \
                      "type = '{self.type}'," \
                      "table = '{self.table}'," \
                      "date_stamp = '{self.date_stamp}')".format(self=self)


class File(Base):
  '''
  A table for loading file information
  
  :column file_id: An integer id for file table
  :column file_path: A required string to specify file path information, allowed length 500
  :column location: An optional enum list to specify storage location, default UNKNOWN,
                    allowed values are ORWELL, HPC_PROJECT, ELIOT, IRODS and UNKNOWN
  :column status: An optional enum list to specify experiment status, default is ACTIVE,
                  allowed values are ACTIVE, FAILED and WITHDRAWN
  :column md5: An optional string to specify file md5 value, allowed length 33
  :column size: An optional string to specify file size, allowed value 15
  :column date_created: An optional timestamp column to record file creation time, default current timestamp
  :column date_updated: An optional timestamp column to record file modification time, default current timestamp
  '''
  __tablename__ = 'file'
  __table_args__ = (
    UniqueConstraint('file_path'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  file_id      = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  file_path    = Column(String(500), nullable=False)
  location     = Column(Enum('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status       = Column(Enum('ACTIVE', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  md5          = Column(String(33))
  size         = Column(String(15))
  date_created = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp())
  date_updated = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now )
  collection_group  = relationship('Collection_group', backref='file')
  file_attribute    = relationship('File_attribute', backref='file')

  def __repr__(self):
    return "File(file_id = '{self.file_id}'," \
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
  
  :column collection_group_id: An integer id for collection_group table
  :column collection_id: A required integer id from collection table (foreign key)
  :column file_id: A required integer id from file table (foreign key)
  '''
  __tablename__ = 'collection_group'
  __table_args__ = (
    UniqueConstraint('collection_id','file_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_group_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  collection_id       = Column(INTEGER(unsigned=True), ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  file_id             = Column(INTEGER(unsigned=True), ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Collection_group(collection_group_id = '{self.collection_group_id}'," \
                            "collection_id = '{self.collection_id}'," \
                            "file_id = '{self.file_id}')".format(self=self)


class Pipeline(Base):
  '''
  A table for loading pipeline information
  
  :column pipeline_id: An integer id for pipeline table
  :column pipeline_name: A required string to specify pipeline name, allowed length 50
  :column pipeline_db: A required string to specify pipeline database url, allowed length 200
  :column pipeline_init_conf: An optional json field to specify initial pipeline configuration
  :column pipeline_run_conf: An optional json field to specify modified pipeline configuration
  :column pipeline_type: An optional enum list to specify pipeline type, default EHIVE,
                         allowed values are EHIVE and UNKNOWN
  :column is_active: An optional enum list to specify the status of pipeline, default Y,
                     allowed values are Y and N
  :column date_stamp: An optional timestamp column to record file creation or modification time, default current timestamp
  '''
  __tablename__ = 'pipeline'
  __table_args__ = ( 
    UniqueConstraint('pipeline_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  pipeline_id        = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  pipeline_name      = Column(String(50), nullable=False)
  pipeline_db        = Column(String(200), nullable=False)
  pipeline_init_conf = Column(JSONType)
  pipeline_run_conf  = Column(JSONType)
  pipeline_type      = Column(Enum('EHIVE','UNKNOWN'), nullable=False, server_default='EHIVE')
  is_active          = Column(Enum('Y', 'N'), nullable=False, server_default='Y')
  date_stamp         = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  pipeline_seed      = relationship('Pipeline_seed', backref='pipeline')

  def __repr__(self):
    return "Pipeline(pipeline_id = '{self.pipeline_id}'," \
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
  
  :column pipeline_seed_id: An integer id for pipeline_seed table
  :column seed_id: A required integer id
  :column seed_table: An optional enum list to specify seed table information, default unknown,
                      allowed values project, sample, experiment, run, file, seqrun, collection and unknown
  :column pipeline_id: An integer id from pipeline table (foreign key)
  :column status: An optional enum list to specify the status of pipeline, default UNKNOWN,
                  allowed values are SEEDED, RUNNING, FINISHED, FAILED and UNKNOWN
  :column date_stamp: An optional timestamp column to record file creation or modification time, default current timestamp
  '''
  __tablename__ = 'pipeline_seed'
  __table_args__ = (
    UniqueConstraint('pipeline_id','seed_id','seed_table'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  pipeline_seed_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  seed_id          = Column(INTEGER(unsigned=True), nullable=False)
  seed_table       = Column(Enum('project','sample','experiment','run','file','seqrun','collection','unknown'), nullable=False, server_default='unknown')
  pipeline_id      = Column(INTEGER(unsigned=True), ForeignKey('pipeline.pipeline_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  status           = Column(Enum('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  date_stamp       = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
 
  def __repr__(self):
    return "Pipeline_seed(pipeline_seed_id = '{self.pipeline_seed_id}'," \
                         "seed_id = '{self.seed_id}'," \
                         "seed_table = '{self.seed_table}'," \
                         "pipeline_id = '{self.pipeline_id}'," \
                         "status = '{self.status}'," \
                         "date_stamp = '{self.date_stamp}')".format(self=self)

class History(Base):
  '''
  A table for loading history information
  
  :column log_id: An integer id for history table
  :column log_type: A required enum value to specify log type, allowed values are 
                    CREATED, MODIFIED and DELETED
  :column table_name: A required enum value to specify table information, allowed values are
                      PROJECT, USER, SAMPLE, EXPERIMENT, RUN, COLLECTION, FILE, PLATFORM,
                      PROJECT_ATTRIBUTE, EXPERIMENT_ATTRIBUTE, COLLECTION_ATTRIBUTE,
                      SAMPLE_ATTRIBUTE, RUN_ATTRIBUTE, FILE_ATTRIBUTE
  :column log_date: An optional timestamp column to record file creation or modification time, default current timestamp
  :column message: An optional text field to specify message
  '''
  __tablename__ = 'history'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  log_id     = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  log_type   = Column(Enum('CREATED', 'MODIFIED', 'DELETED'), nullable=False)
  table_name = Column(Enum('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE'), nullable=False)
  log_date   = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  message    = Column(TEXT())

  def __repr__(self):
    return "History(log_id = '{self.log_id}'," \
                   "log_type = '{self.log_type}'," \
                   "table_name = '{self.table_name}'," \
                   "log_date = '{self.log_date}'," \
                   "message = '{self.message}')".format(self=self)


class Project_attribute(Base):
  '''
  A table for loading project attributes
  
  :column project_attribute_id: An integer id for project_attribute table
  :column attribute_name: An optional string attribute name, allowed length 50
  :column attribute_value: An optional string attribute value, allowed length 50
  :column project_id: An integer id from project table (foreign key)
  '''
  __tablename__ = 'project_attribute'
  __table_args__ = (
    UniqueConstraint('project_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name       = Column(String(50))
  attribute_value      = Column(String(50))
  project_id           = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Project_attribute(project_attribute_id = '{self.project_attribute_id}'," \
                             "attribute_name = '{self.attribute_name}'," \
                             "attribute_value = '{self.attribute_value}'," \
                             "project_id = '{self.project_id}')".format(self=self)


class Experiment_attribute(Base):
  '''
  A table for loading experiment attributes
  
  :column experiment_attribute_id: An integer id for experiment_attribute table
  :column attribute_name: An optional string attribute name, allowed length 30
  :column attribute_value: An optional string attribute value, allowed length 50
  :column experiment_id: An integer id from experiment table (foreign key)
  '''
  __tablename__ = 'experiment_attribute'
  __table_args__ = (
    UniqueConstraint('experiment_id', 'attribute_name', 'attribute_value'),
    {  'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name          = Column(String(30))
  attribute_value         = Column(String(50))
  experiment_id           = Column(INTEGER(unsigned=True), ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Experiment_attribute(experiment_attribute_id = '{self.experiment_attribute_id}'," \
                                "attribute_name = '{self.attribute_name}'," \
                                "attribute_value = '{self.attribute_value}'," \
                                "experiment_id = '{self.experiment_id}')".format(self=self)
  

class Collection_attribute(Base):
  '''
  A table for loading collection attributes
  
  :column collection_attribute_id: An integer id for collection_attribute table
  :column attribute_name: An optional string attribute name, allowed length 45
  :column attribute_value: An optional string attribute value, allowed length 45
  :column collection_id: An integer id from collection table (foreign key)
  '''
  __tablename__ = 'collection_attribute'
  __table_args__ = (
    UniqueConstraint('collection_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name          = Column(String(45))
  attribute_value         = Column(String(45))
  collection_id           = Column(INTEGER(unsigned=True), ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Collection_attribute(collection_attribute_id = '{self.collection_attribute_id}'," \
                                "attribute_name = '{self.attribute_name}'," \
                                "attribute_value = '{self.attribute_value}'," \
                                "collection_id = '{self.collection_id}')".format(self=self)  


class Sample_attribute(Base):
  '''
  A table for loading sample attributes
  
  :column sample_attribute_id: An integer id for sample_attribute table
  :column attribute_name: An optional string attribute name, allowed length 30
  :column attribute_value: An optional string attribute value, allowed length 50
  :column sample_id: An integer id from sample table (foreign key)
  '''
  __tablename__ = 'sample_attribute'
  __table_args__ = (
    UniqueConstraint('sample_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  sample_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name      = Column(String(30))
  attribute_value     = Column(String(50))
  sample_id           = Column(INTEGER(unsigned=True), ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Sample_attribute(sample_attribute_id = '{self.sample_attribute_id}'," \
                            "attribute_name = '{self.attribute_name}'," \
                            "attribute_value = '{self.attribute_value}'," \
                            "sample_id = '{self.sample_id}')".format(self=self)


class Seqrun_attribute(Base):
  '''
  A table for loading seqrun attributes
  
  :column seqrun_attribute_id: An integer id for seqrun_attribute table
  :column attribute_name: An optional string attribute name, allowed length 50
  :column attribute_value: An optional string attribute value, allowed length 100
  :column seqrun_id: An integer id from seqrun table (foreign key)
  '''
  __tablename__ = 'seqrun_attribute'
  __table_args__ = (
    UniqueConstraint('seqrun_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
  seqrun_attribute_id  = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name       = Column(String(50))
  attribute_value      = Column(String(100))
  seqrun_id            = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)


class Run_attribute(Base):
  '''
  A table for loading run attributes
  
  :column run_attribute_id: An integer id for run_attribute table
  :column attribute_name: An optional string attribute name, allowed length 30
  :column attribute_value: An optional string attribute value, allowed length 50
  :column run_id: An integer id from run table (foreign key)
  '''
  __tablename__ = 'run_attribute'
  __table_args__ = (
    UniqueConstraint('run_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_attribute_id  = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name    = Column(String(30))
  attribute_value   = Column(String(50))
  run_id            = Column(INTEGER(unsigned=True), ForeignKey('run.run_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Run_attribute(run_attribute_id = '{self.run_attribute_id}'," \
                         "attribute_name = '{self.attribute_name}'," \
                         "attribute_value = '{self.attribute_value}'," \
                         "run_id = '{self.run_id}')".format(self=self)


class File_attribute(Base):
  '''
  A table for loading file attributes
  
  :column file_attribute_id: An integer id for file_attribute table
  :column attribute_name: An optional string attribute name, allowed length 30
  :column attribute_value: An optional string attribute value, allowed length 50
  :column file_id: An integer id from file table (foreign key)
  '''
  __tablename__ = 'file_attribute'
  __table_args__ = (
    UniqueConstraint('file_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  file_attribute_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name    = Column(String(30))
  attribute_value   = Column(String(50))
  file_id           = Column(INTEGER(unsigned=True), ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "File_attribute(file_attribute_id = '{self.file_attribute_id}'," \
                          "attribute_name = '{self.attribute_name}'," \
                          "attribute_value = '{self.attribute_value}'," \
                          "file_id = '{self.file_id}')".format(self=self)
  
