import datetime
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

  project_id     = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_igf_id = Column(String(20), nullable=False)
  project_name   = Column(String(100))
  start_date     = Column(DATE(), nullable=False, default=datetime.date.today())
  description    = Column(TEXT())
  deliverable    = Column(Enum('FASTQ', 'ALIGNMENT', 'ANALYSIS'), server_default='FASTQ')
  projectuser       = relationship('ProjectUser', backref="project")
  sample            = relationship('Sample', backref="project")
  experiment        = relationship('Experiment', backref="project")
  project_attribute = relationship('Project_attribute', backref="project")

  def __repr__(self):
    return "Project(project_id = '{self.project_id}', " \
                    "project_igf_id = '{self.project_igf_id}'," \
                    "project_name = '{self.project_name}'," \
                    "start_date = '{self.start_date}'," \
                    "description = '{self.description}'," \
                    "deliverable = '{self.deliverable}')".format(self=self)
                   

class User(Base):
  __tablename__ = 'user'
  __table_args__ = (
    UniqueConstraint('user_igf_id'),
    UniqueConstraint('email_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  user_id       = Column(INTEGER(unsigned=True), primary_key=True, nullable=False) 
  user_igf_id   = Column(String(10))
  name          = Column(String(30), nullable=False)
  email_id      = Column(String(20), nullable=False)
  username      = Column(String(10))
  hpc_username  = Column(String(10))
  category      = Column(Enum('HPC_USER','NON_HPC_USER','EXTERNAL'), nullable=False, server_default='NON_HPC_USER')
  status        = Column(Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created  = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now)
  password      = Column(String(129))
  projectuser   = relationship('ProjectUser', backref="user")
  
  def __repr__(self):
    return "User(user_id = '{self.user_id}'," \
                "user_igf_id = '{self.user_igf_id}'," \
                "name = '{self.name}', " \
                "username = '{self.username}',"\
                "hpc_username = '{self.hpc_username}',"\
                "category = '{self.category}'," \
                "status = '{self.status}'," \
                "email_id = '{self.email_id}'," \
                "date_stamp = '{self.date_stamp}'," \
                "password = '{self.password}')".format(self=self) 

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
  sample_igf_id       = Column(String(10), nullable=False)
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
  date_created        = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now)
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
  model_name       = Column(Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ'), nullable=False)
  vendor_name      = Column(Enum('ILLUMINA', 'NANOPORE'), nullable=False)
  software_name    = Column(Enum('RTA'), nullable=False)
  software_version = Column(Enum('RTA1.18.54', 'RTA1.18.64', 'RTA2'), nullable=False)
  date_created     = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now )
  experiment       = relationship('Experiment', backref="platform")

  def __repr__(self):
    return "Platform(platform_id = '{self.platform_id}'," \
                    "platform_igf_id = '{self.igf_id}'," \
                    "model_name = '{self.model_name}'," \
                    "vendor_name = '{self.vendor_name}'," \
                    "software_name = '{self.software_name}'," \
                    "software_version = '{self.software_version}'," \
                    "date_created = '{self.date_created}')".format(self=self)


class Rejected_run(Base):
  __tablename__ = 'rejected_run'
  __table_args__ = (
    UniqueConstraint('rejected_run_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  rejected_run_id     = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  rejected_run_igf_id = Column(String(50), nullable=False)
  date_created        = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now)

  def __repr__(self):
    return "Rejected_run(rejected_run_id = '{self.rejected_run_id}'," \
                        "rejected_run_igf_id = '{self.rejected_run_igf_id}'," \
                        "date_created = '{self.date_created}')".format(self=self)


class Experiment(Base):
  __tablename__ = 'experiment'
  __table_args__ = (
    UniqueConstraint('sample_id', 'library_name', 'platform_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_id    = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  project_id       = Column(INTEGER(unsigned=True), ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  sample_id        = Column(INTEGER(unsigned=True), ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="SET NULL"))
  library_name     = Column(String(50), nullable=False)
  library_strategy = Column(Enum('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment_type  = Column(Enum('POLYA-RNA', 'TOTAL-RNA', 'SMALL_RNA', 'H3K4ME3', 'WGS', 'EXOME', 'H3k27ME3', 'H3K27AC', 'H3K9ME3', 'H3K36ME3', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_layout   = Column(Enum('SINGLE', 'PAIRED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status           = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created     = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now)
  platform_id      = Column(INTEGER(unsigned=True), ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  experiment            = relationship('Run', backref='experiment')
  experiment_attribute  = relationship('Experiment_attribute', backref='experiment')

  def __repr__(self):
    return "Experiment(experiment_id = '{self.experiment_id}'," \
                      "project_id = '{self.project_id}'," \
                      "sample_id = '{self.sample_id}'," \
                      "library_name = '{self.library_name}'," \
                      "library_strategy = '{self.library_strategy}'," \
                      "experiment_type = '{self.experiment_type}'," \
                      "library_layout = '{self.library_layout}'," \
                      "status = '{self.status}'," \
                      "date_created = '{self.date_created}'," \
                      "platform_id = '{self.platform_id}')".format(self=self)


class Run(Base):
  __tablename__ = 'run'
  __table_args__ = (
    UniqueConstraint('run_igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_id        = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  experiment_id = Column(INTEGER(unsigned=True), ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="SET NULL"))
  run_igf_id    = Column(String(50), nullable=False)
  flowcell_id   = Column(String(10), nullable=False)
  status        = Column(Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  lane_number   = Column(Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  date_created  = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now)
  run_attribute = relationship('Run_attribute', backref='run')

  def __repr__(self):
    return "Run(run_id = '{self.run_id}'," \
               "experiment_id = '{self.experiment_id}'," \
               "run_igf_id = '{self.run_igf_id}'," \
               "flowcell_id = '{self.flowcell_id}'," \
               "status = '{self.status}'," \
               "lane_number = '{self.lane_number}'," \
               "date_created = '{self.date_created}')".format(self=self)
  

class Collection(Base):
  __tablename__ = 'collection'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  name          = Column(String(20), nullable=False)
  type          = Column(String(30), nullable=False)
  table         = Column(Enum('sample', 'experiment', 'run', 'file'), nullable=False)
  date_stamp    = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now) 
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
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  file_id      = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  file_path    = Column(String(500), nullable=False)
  location     = Column(Enum('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  type         = Column(Enum('BCL_DIR', 'BCL_TAR', 'FASTQ_TAR', 'FASTQC_TAR', 'FASTQ', 'FASTQGZ', 'BAM', 'CRAM', 'GFF', 'BED', 'GTF', 'FASTA', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  md5          = Column(String(33))
  size         = Column(String(15))
  date_created = Column(DATE(), nullable=False, default=datetime.date.today())
  date_updated = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now )
  collection_group  = relationship('Collection_group', backref='file')
  file_attribute    = relationship('File_attribute', backref='file')

  def __repr__(self):
    return "File(file_id = '{self.file_id}'," \
                "file_path = '{self.file_path}'," \
                "location = '{self.location}'," \
                "type = '{self.type}'," \
                "md5 = '{self.md5}'," \
                "size = '{self.size}'," \
                "date_created = '{self.date_created}'," \
                "date_updated = '{self.date_updated}')".format(self=self)


class Collection_group(Base):
  __tablename__ = 'collection_group'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_group_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  collection_id       = Column(INTEGER(unsigned=True), ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  file_id             = Column(INTEGER(unsigned=True), ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

  def __repr__(self):
    return "Collection_group(collection_group_id = '{self.collection_group_id}'," \
                            "collection_id = '{self.collection_id}'," \
                            "file_id = '{self.file_id}')".format(self=self)


class Pipeline(Base):
  __tablename__ = 'pipeline'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  pipeline_id   = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  pipeline_name = Column(String(50), nullable=False)
  is_active     = Column(Enum('Y', 'N'), nullable=False)
  date_stamp    = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now)
  pipeline_seed = relationship('Pipeline_seed', backref='pipeline')

  def __repr__(self):
    return "Pipeline(pipeline_id = '{self.pipeline_id}'," \
                    "pipeline_name = '{self.pipeline_name}'," \
                    "is_active = '{self.is_active}'," \
                    "date_stamp = '{self.date_stamp}')".format(self=self)


class Hive_db(Base):
  __tablename__ = 'hive_db'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  hive_db_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  dbname     = Column(String(100), nullable=False)
  is_active  = Column(Enum('Y', 'N'), nullable=False)
  pipeline_seed = relationship('Pipeline_seed', backref='hive_db')

  def __repr__(self):
    return "Hive_db(hive_db_id = '{self.hive_db_id}'," \
                   "dbname = '{self.dbname}'," \
                   "is_active = '{self.is_active}')".format(self=self)


class Pipeline_seed(Base):
  __tablename__ = 'pipeline_seed'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  pipeline_seed_id = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  pipeline_id      = Column(INTEGER(unsigned=True), ForeignKey('pipeline.pipeline_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  hive_db_id       = Column(INTEGER(unsigned=True), ForeignKey('hive_db.hive_db_id', onupdate="CASCADE", ondelete="SET NULL"))
  status           = Column(Enum('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
 
  def __repr__(self):
    return "Pipeline_seed(pipeline_seed_id = '{self.pipeline_seed_id}'," \
                         "pipeline_id = '{self.pipeline_id}'," \
                         "hive_db_id = '{self.hive_db_id}'," \
                         "status = '{self.status}')".format(self=self)

class History(Base):
  __tablename__ = 'history'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  log_id     = Column(INTEGER(unsigned=True), primary_key=True, nullable=False)
  log_type   = Column(Enum('CREATED', 'MODIFIED', 'DELETED'), nullable=False)
  table_name = Column(Enum('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE'), nullable=False)
  log_date   = Column(TIMESTAMP(), nullable=False, default=datetime.datetime.now, onupdate=datetime.datetime.now)
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
    UniqueConstraint('project_id', 'attribute_name'),
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
    UniqueConstraint('experiment_id', 'attribute_name'),
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
    UniqueConstraint('collection_id', 'attribute_name'),
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
    UniqueConstraint('sample_id', 'attribute_name'),
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


class Run_attribute(Base):
  __tablename__ = 'run_attribute'
  __table_args__ = (
    UniqueConstraint('run_id', 'attribute_name'),
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
    UniqueConstraint('file_id', 'attribute_name'),
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
  
