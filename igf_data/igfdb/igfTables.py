from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Table, Column, Integer, String, Enum, TIMESTAMP, TEXT, ForeignKey, text, DATE, create_engine, ForeignKeyConstraint, UniqueConstraint


Base = declarative_base()

class Project(Base):
  __tablename__ = 'project'
  __table_args__ = (
     UniqueConstraint('igf_id'),
     { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_id   = Column('project_id', Integer, primary_key=True, nullable=False)
  igf_id       = Column('igf_id', String(20), nullable=False)
  project_name = Column('project_name', String(100))
  start_date   = Column('start_date', DATE(), nullable=False)
  description  = Column('description', TEXT())
  requirement  = Column('requirement', Enum('FASTQ', 'ALIGNMENT', 'ANALYSIS'), server_default='FASTQ')
  projectuser       = relationship('ProjectUser', backref="project")
  sample            = relationship('Sample', backref="project")
  experiment        = relationship('Experiment', backref="project")
  project_attribute = relationship('Project_attribute', backref="project")

  def __repr__(self):
    return "Project(project_id = '{self.project_id}', " \
                    "igf_id = '{self.igf_id}'," \
                    "project_name = '{self.project_name}'," \
                    "start_date = '{self.start_date}'," \
                    "description = '{self.description}'," \
                    "requirement = '{self.requirement}')".format(self=self)
                   

class User(Base):
  __tablename__ = 'user'
  __table_args__ = (
    UniqueConstraint('user_igf_id'),
    UniqueConstraint('email_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  user_id       = Column('user_id', Integer, primary_key=True, nullable=False) 
  igf_id        = Column('igf_id', String(10))
  name          = Column('name', String(25), nullable=False)
  hpc_user_name = Column('hpc_user_name', String(8))
  category      = Column('category', Enum('HPC_USER','NON_HPC_USER','EXTERNAL'), nullable=False, server_default='NON_HPC_USER')
  status        = Column('status', Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  email_id      = Column('email_id', String(20), nullable=False)
  date_stamp    = Column('date_stamp', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  password      = Column('password', String(129))
  projectuser   = relationship('ProjectUser', backref="user")
  
  def __repr__(self):
    return "User(user_id = '{self.user_id}'," \
                 "igf_id = '{self.igf_id}'," \
                 "name = '{self.name}', " \
                 "hpc_user_name = '{self.hpc_user_name}',"\
                 "category = '{self.category}'," \
                 "status = '{self.status}'," \
                 "email_id = '{self.email_id}'," \
                 "date_stamp = '{self.date_stamp}'," \
                 "date_stamp = '{self.date_stamp}'," \
                 "password = '{self.password}')".format(self=self) 

class ProjectUser(Base):
  __tablename__ = 'project_user'
  __table_args__ = (
    UniqueConstraint('project_id','data_authority'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
    
  project_user_id = Column('project_user_id', Integer, primary_key=True, nullable=False)
  project_id      = Column('project_id', Integer, ForeignKey("project.project_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  user_id         = Column('user_id', Integer, ForeignKey("user.user_id", onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  data_authority  = Column('data_authority', Enum('T'))

  def __repr__(self):
    return "ProjectUser(project_user_id = '{self.project_user_id}'," \
                        "project_id = '{self.project_id}'," \
                        "user_id = '{self.user_id}'," \
                        "data_authority = '{self.data_authority}')".format(self=self)

class Sample(Base):
  __tablename__ = 'sample'
  __table_args__ = (
    UniqueConstraint('igf_id'),
    { 'mysql_engine':'InnoDB','mysql_charset':'utf8' })

  sample_id           = Column('sample_id', Integer, primary_key=True, nullable=False)
  igf_id              = Column('igf_id', String(10), nullable=False)
  taxon_id            = Column('taxon_id', Integer)
  scientific_name     = Column('scientific_name', String(50))
  common_name         = Column('common_name', String(50))
  donor_anonymized_id = Column('donor_anonymized_id', String(10))
  description         = Column('description', String(50))
  phenotype           = Column('phenotype', String(45))
  sex                 = Column('sex', Enum('FEMALE', 'MALE', 'OTHER', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status              = Column('status', Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  biomaterial_type    = Column('biomaterial_type', Enum('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  cell_type           = Column('cell_type', String(50))
  tissue_type         = Column('tissue_type', String(50))
  cell_line           = Column('cell_line', String(50))
  date_created        = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
  project_id          = Column('project_id', Integer, ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  experiment          = relationship('Experiment', backref="sample")
  sample_attribute    = relationship('Sample_attribute', backref="sample")

class Platform(Base):
  __tablename__ = 'platform'
  __table_args__ = (
    UniqueConstraint('igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  platform_id      = Column('platform_id', Integer, primary_key=True, nullable=False)
  igf_id           = Column('igf_id', String(10), nullable=False)
  model_name       = Column('model_name', Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ'), nullable=False)
  vendor_name      = Column('vendor_name', Enum('ILLUMINA', 'NANOPORE'), nullable=False)
  software_name    = Column('software_name', Enum('RTA'), nullable=False)
  software_version = Column('software_version', Enum('RTA1.18.54', 'RTA1.18.64', 'RTA2'), nullable=False)
  date_created     = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  experiment       = relationship('Experiment', backref="platform")


class Rejected_run(Base):
  __tablename__ = 'rejected_run'
  __table_args__ = (
    UniqueConstraint('igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_id = Column('run_id', Integer, primary_key=True, nullable=False)
  igf_id = Column('igf_id', String(50), nullable=False)
  date_created = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))


class Experiment(Base):
  __tablename__ = 'experiment'
  __table_args__ = (
    UniqueConstraint('sample_id', 'library_name', 'platform_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_id    = Column('experiment_id', Integer, primary_key=True, nullable=False)
  project_id       = Column('project_id', Integer, ForeignKey('project.project_id', onupdate="CASCADE", ondelete="SET NULL"))
  sample_id        = Column('sample_id', Integer, ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="SET NULL"))
  library_name     = Column('library_name', String(50), nullable=False)
  library_strategy = Column('library_strategy', Enum('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  experiment_type  = Column('experiment_type', Enum('POLYA-RNA', 'TOTAL-RNA', 'SMALL_RNA', 'H3K4ME3', 'WGS', 'EXOME', 'H3k27ME3', 'H3K27AC', 'H3K9ME3', 'H3K36ME3', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  library_layout   = Column('library_layout', Enum('SINGLE', 'PAIRED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  status           = Column('status', Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  date_created     = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  platform_id      = Column('platform_id', Integer, ForeignKey('platform.platform_id', onupdate="CASCADE", ondelete="SET NULL"))
  experiment            = relationship('Run', backref='experiment')
  experiment_attribute  = relationship('Experiment_attribute', backref='experiment')

class Run(Base):
  __tablename__ = 'run'
  __table_args__ = (
    UniqueConstraint('igf_id'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  run_id        = Column('run_id', Integer, primary_key=True, nullable=False)
  experiment_id = Column('experiment_id', Integer, ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="SET NULL"))
  igf_id        = Column('igf_id', String(50), nullable=False)
  flowcell_id   = Column('flowcell_id', String(10), nullable=False)
  status        = Column('status', Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), nullable=False, server_default='ACTIVE')
  lane_number   = Column('lane_number', Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False)
  date_created  = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
  run_attribute = relationship('Run_attribute', backref='run')


class Collection(Base):
  __tablename__ = 'collection'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_id = Column('collection_id', Integer, primary_key=True, nullable=False)
  name          = Column('name', String(20), nullable=False)
  type          = Column('type', String(30), nullable=False)
  table         = Column('table', Enum('sample', 'experiment', 'run', 'file'), nullable=False)
  date_stamp    = Column('date_stamp', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')) 
  collection_group      = relationship('Collection_group', backref='collection')
  collection_attribute  = relationship('Collection_attribute', backref='collection')

class File(Base):
  __tablename__ = 'file'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  file_id      = Column('file_id', Integer, primary_key=True, nullable=False)
  file_path    = Column('file_path', String(500), nullable=False)
  location     = Column('location', Enum('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  type         = Column('type', Enum('BCL_DIR', 'BCL_TAR', 'FASTQ_TAR', 'FASTQC_TAR', 'FASTQ', 'FASTQGZ', 'BAM', 'CRAM', 'GFF', 'BED', 'GTF', 'FASTA', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')
  md5          = Column('md5', String(33))
  size         = Column('size', String(15))
  date_created = Column('date_created', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP'))
  date_updated = Column('date_updated', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  collection_group  = relationship('Collection_group', backref='file')
  file_attribute    = relationship('File_attribute', backref='file')


class Collection_group(Base):
  __tablename__ = 'collection_group'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_group_id = Column('collection_group_id', Integer, primary_key=True, nullable=False)
  collection_id       = Column('collection_id', Integer, ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  file_id             = Column('file_id', Integer, ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

class Pipeline(Base):
  __tablename__ = 'pipeline'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  pipeline_id   = Column('pipeline_id', Integer, primary_key=True, nullable=False)
  pipeline_name = Column('pipeline_name', String(50), nullable=False)
  is_active     = Column('is_active', Enum('Y', 'N'), nullable=False)
  date_stamp    = Column('date_stamp', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  pipeline_seed = relationship('Pipeline_seed', backref='pipeline')

class Hive_db(Base):
  __tablename__ = 'hive_db'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  hive_db_id = Column('hive_db_id', Integer, primary_key=True, nullable=False)
  dbname     = Column('dbname', String(100), nullable=False)
  is_active  = Column('is_active', Enum('Y', 'N'), nullable=False)
  pipeline_seed = relationship('Pipeline_seed', backref='hive_db')


class Pipeline_seed(Base):
  __tablename__ = 'pipeline_seed'
  __table_args__ = ({ 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  pipeline_seed_id = Column('pipeline_seed_id', Integer, primary_key=True, nullable=False)
  pipeline_id      = Column('pipeline_id', Integer, ForeignKey('pipeline.pipeline_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
  hive_db_id       = Column('hive_db_id', Integer, ForeignKey('hive_db.hive_db_id', onupdate="CASCADE", ondelete="SET NULL"))
  status           = Column('status', Enum('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN'), nullable=False, server_default='UNKNOWN')


class History(Base):
  __tablename__ = 'history'
  __table_args__ = ( { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  log_id     = Column('log_id', Integer, primary_key=True, nullable=False)
  log_type   = Column('log_type', Enum('CREATED', 'MODIFIED', 'DELETED'), nullable=False)
  table_name = Column('table_name', Enum('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE'), nullable=False)
  log_date   = Column('log_date', TIMESTAMP(), nullable=False, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  message    = Column('message', TEXT())


class Project_attribute(Base):
  __tablename__ = 'project_attribute'
  __table_args__ = (
    UniqueConstraint('project_id', 'project_attribute_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  project_attribute_id    = Column('project_attribute_id', Integer, primary_key=True, nullable=False)
  project_attribute_name  = Column('project_attribute_name', String(50))
  project_attribute_value = Column('project_attribute_value', String(50))
  project_id              = Column('project_id', Integer, ForeignKey('project.project_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

class Experiment_attribute(Base):
  __tablename__ = 'experiment_attribute'
  __table_args__ = (
    UniqueConstraint('experiment_id', 'experiment_attribute_name'),
    {  'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  experiment_attribute_id    = Column('experiment_attribute_id', Integer, primary_key=True, nullable=False)
  experiment_attribute_name  = Column('experiment_attribute_name', String(30))
  experiment_attribute_value = Column('experiment_attribute_value', String(50))
  experiment_id              = Column('experiment_id', Integer, ForeignKey('experiment.experiment_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)


class Collection_attribute(Base):
  __tablename__ = 'collection_attribute'
  __table_args__ = (
    UniqueConstraint('collection_id', 'collection_attribute_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  collection_attribute_id    = Column('collection_attribute_id', Integer, primary_key=True, nullable=False)
  collection_attribute_name  = Column('collection_attribute_name', String(45))
  collection_attribute_value = Column('collection_attribute_value', String(45))
  collection_id              = Column('collection_id', Integer, ForeignKey('collection.collection_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

class Sample_attribute(Base):
  __tablename__ = 'sample_attribute'
  __table_args__ = (
    UniqueConstraint('sample_id', 'sample_attribute_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })

  sample_attribute_id    = Column('sample_attribute_id', Integer, primary_key=True, nullable=False)
  sample_attribute_name  = Column('sample_attribute_name', String(30))
  sample_attribute_value = Column('sample_attribute_value', String(50))
  sample_id              = Column('sample_id', Integer, ForeignKey('sample.sample_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

class Run_attribute(Base):
  __tablename__ = 'run_attribute'
  __table_args__ = (
    UniqueConstraint('run_id', 'run_attribute_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8' })
  run_attribute_id    = Column('run_attribute_id', Integer, primary_key=True, nullable=False)
  run_attribute_name  = Column('run_attribute_name', String(30))
  run_attribute_value = Column('run_attribute_value', String(50))
  run_id              = Column('run_id', Integer, ForeignKey('run.run_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)


class File_attribute(Base):
  __tablename__ = 'file_attribute'
  __table_args__ = (
    UniqueConstraint('file_id', 'file_attribute_name'),
    { 'mysql_engine':'InnoDB', 'mysql_charset':'utf8'  })

  file_attribute_id    = Column('file_attribute_id', Integer, primary_key=True, nullable=False)
  file_attribute_name  = Column('file_attribute_name', String(30))
  file_attribute_value = Column('file_attribute_value', String(50))
  file_id              = Column('file_id', Integer, ForeignKey('file.file_id', onupdate="CASCADE", ondelete="CASCADE"), nullable=False)

