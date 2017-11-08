import datetime
from igf_data.igfdb.datatype import JSONType
from sqlalchemy.sql.functions import current_timestamp
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy import Table, Column, String, Enum, TIMESTAMP, TEXT, ForeignKey, text, DATE, create_engine, ForeignKeyConstraint, UniqueConstraint


Base = declarative_base()

class Project(Base):
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
  __tablename__ = 'user'
  __table_args__ = (
    UniqueConstraint('user_igf_id', 'username'),
    UniqueConstraint('email_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  user_id         = Column(INTEGER(unsigned=True), primary_key=True, nullable=False) 
  user_igf_id     = Column(String(10))
  name            = Column(String(30), nullable=False)
  email_id        = Column(String(20), nullable=False)
  username        = Column(String(10))
  hpc_username    = Column(String(10))
  category        = Column(Enum('HPC_USER','NON_HPC_USER','EXTERNAL'), nullable=False, server_default='NON_HPC_USER')
  status          = Column(Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created    = Column(TIMESTAMP(), nullable=False, server_default=current_timestamp(), onupdate=datetime.datetime.now)
  password        = Column(String(129))
  encryption_salt = Column(String(129))
  projectuser     = relationship('ProjectUser', backref="user")
  
  def __repr__(self):
    return "User(user_id = '{self.user_id}'," \
                "user_igf_id = '{self.user_igf_id}'," \
                "name = '{self.name}', " \
                "username = '{self.username}',"\
                "hpc_username = '{self.hpc_username}',"\
                "category = '{self.category}'," \
                "status = '{self.status}'," \
                "email_id = '{self.email_id}'," \
                "date_created = '{self.date_created}'," \
                "password = '{self.password}'," \
                "encryption_salt = '{self.encryption_salt}')".format(self=self) 

class ProjectUser(Base):
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
  __tablename__ = 'sample'
  __table_args__ = (
    UniqueConstraint('sample_igf_id'),
    { 'mysql_engine':'InnoDB','mysql_charset':'utf8' })

  sample_id           = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  sample_igf_id       = Column(String(20), nullable=False)
  taxon_id            = Column(INTEGER(unsigned=True))
  scientific_name     = Column(String(50))
  common_name         = Column(String(50))
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
                  "taxon_id = '{self.taxon_id}'," \
                  "scientific_name = '{self.scientific_name}'," \
                  "common_name = '{self.common_name}'," \
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
  __tablename__ = 'platform'
  __table_args__ = (
    UniqueConstraint('platform_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  platform_id      = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  platform_igf_id  = Column(String(10), nullable=False)
  model_name       = Column(Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ', 'NANOPORE_MINION'), nullable=False)
  vendor_name      = Column(Enum('ILLUMINA', 'NANOPORE'), nullable=False)
  software_name    = Column(Enum('RTA'), nullable=False)
  software_version = Column(Enum('RTA1.18.54', 'RTA1.18.64', 'RTA2'), nullable=False)
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
  library_strategy  = Column(Enum('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment_type   = Column(Enum('POLYA-RNA', 'TOTAL-RNA', 'SMALL_RNA', 'H3K4ME3', 'WGS', 'EXOME', 'H3k27ME3', 'H3K27AC', 'H3K9ME3', 'H3K36ME3', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
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
  __tablename__ = 'run'
  __table_args__ = (
    UniqueConstraint('run_igf_id'),
    UniqueConstraint('experiment_id','lane_number'),
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
  __tablename__ = 'seqrun_attribute'
  __table_args__ = (
    UniqueConstraint('seqrun_id', 'attribute_name', 'attribute_value'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
  seqrun_attribute_id  = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  attribute_name       = Column(String(50))
  attribute_value      = Column(String(100))
  seqrun_id            = Column(INTEGER(unsigned=True), ForeignKey('seqrun.seqrun_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)


class Run_attribute(Base):
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
  
