"""adding tables

Revision ID: 4c97401b8961
Revises: bd380507518c
Create Date: 2021-02-07 17:43:03.412092

"""
import igf_data
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = '4c97401b8961'
down_revision = 'bd380507518c'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('collection',
    sa.Column('collection_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('name', sa.String(length=70), nullable=False),
    sa.Column('type', sa.String(length=50), nullable=False),
    sa.Column('table', sa.Enum('sample', 'experiment', 'run', 'file', 'project', 'seqrun', 'unknown'), server_default='unknown', nullable=False),
    sa.Column('date_stamp', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('collection_id'),
    sa.UniqueConstraint('name', 'type'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('file',
    sa.Column('file_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('file_path', sa.String(length=500), nullable=False),
    sa.Column('location', sa.Enum('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('status', sa.Enum('ACTIVE', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('md5', sa.String(length=33), nullable=True),
    sa.Column('size', sa.String(length=15), nullable=True),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('date_updated', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('file_id'),
    sa.UniqueConstraint('file_path'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('history',
    sa.Column('log_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('log_type', sa.Enum('CREATED', 'MODIFIED', 'DELETED'), nullable=False),
    sa.Column('table_name', sa.Enum('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE'), nullable=False),
    sa.Column('log_date', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('message', sa.TEXT(), nullable=True),
    sa.PrimaryKeyConstraint('log_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('pipeline',
    sa.Column('pipeline_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('pipeline_name', sa.String(length=50), nullable=False),
    sa.Column('pipeline_db', sa.String(length=200), nullable=False),
    sa.Column('pipeline_init_conf', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.Column('pipeline_run_conf', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.Column('pipeline_type', sa.Enum('EHIVE', 'UNKNOWN'), server_default='EHIVE', nullable=False),
    sa.Column('is_active', sa.Enum('Y', 'N'), server_default='Y', nullable=False),
    sa.Column('date_stamp', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('pipeline_id'),
    sa.UniqueConstraint('pipeline_name'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('platform',
    sa.Column('platform_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('platform_igf_id', sa.String(length=10), nullable=False),
    sa.Column('model_name', sa.Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ', 'NOVASEQ6000', 'NANOPORE_MINION', 'DNBSEQ-G400', 'DNBSEQ-G50', 'DNBSEQ-T7'), nullable=False),
    sa.Column('vendor_name', sa.Enum('ILLUMINA', 'NANOPORE', 'MGI'), nullable=False),
    sa.Column('software_name', sa.Enum('RTA', 'UNKNOWN'), nullable=False),
    sa.Column('software_version', sa.String(length=20), server_default='UNKNOWN', nullable=False),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.PrimaryKeyConstraint('platform_id'),
    sa.UniqueConstraint('platform_igf_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('project',
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('project_igf_id', sa.String(length=50), nullable=False),
    sa.Column('project_name', sa.String(length=40), nullable=True),
    sa.Column('start_timestamp', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
    sa.Column('description', sa.TEXT(), nullable=True),
    sa.Column('status', sa.Enum('ACTIVE', 'FINISHED', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('deliverable', sa.Enum('FASTQ', 'ALIGNMENT', 'ANALYSIS'), server_default='FASTQ', nullable=True),
    sa.PrimaryKeyConstraint('project_id'),
    sa.UniqueConstraint('project_igf_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('user',
    sa.Column('user_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('user_igf_id', sa.String(length=10), nullable=True),
    sa.Column('name', sa.String(length=30), nullable=False),
    sa.Column('email_id', sa.String(length=40), nullable=False),
    sa.Column('username', sa.String(length=20), nullable=True),
    sa.Column('hpc_username', sa.String(length=20), nullable=True),
    sa.Column('twitter_user', sa.String(length=20), nullable=True),
    sa.Column('orcid_id', sa.String(length=50), nullable=True),
    sa.Column('category', sa.Enum('HPC_USER', 'NON_HPC_USER', 'EXTERNAL'), server_default='NON_HPC_USER', nullable=False),
    sa.Column('status', sa.Enum('ACTIVE', 'BLOCKED', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('password', sa.String(length=129), nullable=True),
    sa.Column('encryption_salt', sa.String(length=129), nullable=True),
    sa.Column('ht_password', sa.String(length=40), nullable=True),
    sa.PrimaryKeyConstraint('user_id'),
    sa.UniqueConstraint('email_id'),
    sa.UniqueConstraint('name'),
    sa.UniqueConstraint('username'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('analysis',
    sa.Column('analysis_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('analysis_type', sa.Enum('RNA_DIFFERENTIAL_EXPRESSION', 'RNA_TIME_SERIES', 'CHIP_PEAK_CALL', 'SOMATIC_VARIANT_CALLING', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('analysis_description', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('analysis_id'),
    sa.UniqueConstraint('project_id', 'analysis_type'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('collection_attribute',
    sa.Column('collection_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=200), nullable=True),
    sa.Column('attribute_value', sa.String(length=200), nullable=True),
    sa.Column('collection_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['collection_id'], ['collection.collection_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('collection_attribute_id'),
    sa.UniqueConstraint('collection_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('collection_group',
    sa.Column('collection_group_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('collection_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('file_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['collection_id'], ['collection.collection_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['file_id'], ['file.file_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('collection_group_id'),
    sa.UniqueConstraint('collection_id', 'file_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('file_attribute',
    sa.Column('file_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=30), nullable=True),
    sa.Column('attribute_value', sa.String(length=50), nullable=True),
    sa.Column('file_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['file_id'], ['file.file_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('file_attribute_id'),
    sa.UniqueConstraint('file_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('flowcell_barcode_rule',
    sa.Column('flowcell_rule_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('platform_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('flowcell_type', sa.String(length=50), nullable=False),
    sa.Column('index_1', sa.Enum('NO_CHANGE', 'REVCOMP', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('index_2', sa.Enum('NO_CHANGE', 'REVCOMP', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.ForeignKeyConstraint(['platform_id'], ['platform.platform_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('flowcell_rule_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('pipeline_seed',
    sa.Column('pipeline_seed_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('seed_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('seed_table', sa.Enum('project', 'sample', 'experiment', 'run', 'file', 'seqrun', 'collection', 'unknown'), server_default='unknown', nullable=False),
    sa.Column('pipeline_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('status', sa.Enum('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('date_stamp', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.ForeignKeyConstraint(['pipeline_id'], ['pipeline.pipeline_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('pipeline_seed_id'),
    sa.UniqueConstraint('pipeline_id', 'seed_id', 'seed_table'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('project_attribute',
    sa.Column('project_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=50), nullable=True),
    sa.Column('attribute_value', sa.String(length=50), nullable=True),
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('project_attribute_id'),
    sa.UniqueConstraint('project_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('project_user',
    sa.Column('project_user_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('user_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('data_authority', sa.Enum('T'), nullable=True),
    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.ForeignKeyConstraint(['user_id'], ['user.user_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('project_user_id'),
    sa.UniqueConstraint('project_id', 'data_authority'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('sample',
    sa.Column('sample_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('sample_igf_id', sa.String(length=20), nullable=False),
    sa.Column('sample_submitter_id', sa.String(length=40), nullable=True),
    sa.Column('taxon_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('scientific_name', sa.String(length=50), nullable=True),
    sa.Column('species_name', sa.String(length=50), nullable=True),
    sa.Column('donor_anonymized_id', sa.String(length=10), nullable=True),
    sa.Column('description', sa.String(length=50), nullable=True),
    sa.Column('phenotype', sa.String(length=45), nullable=True),
    sa.Column('sex', sa.Enum('FEMALE', 'MALE', 'MIXED', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('status', sa.Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('biomaterial_type', sa.Enum('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE', 'SINGLE_NUCLEI', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('cell_type', sa.String(length=50), nullable=True),
    sa.Column('tissue_type', sa.String(length=50), nullable=True),
    sa.Column('cell_line', sa.String(length=50), nullable=True),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('sample_id'),
    sa.UniqueConstraint('sample_igf_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('seqrun',
    sa.Column('seqrun_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('seqrun_igf_id', sa.String(length=50), nullable=False),
    sa.Column('reject_run', sa.Enum('Y', 'N'), server_default='N', nullable=False),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('flowcell_id', sa.String(length=20), nullable=False),
    sa.Column('platform_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.ForeignKeyConstraint(['platform_id'], ['platform.platform_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('seqrun_id'),
    sa.UniqueConstraint('seqrun_igf_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('experiment',
    sa.Column('experiment_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('experiment_igf_id', sa.String(length=40), nullable=False),
    sa.Column('project_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('sample_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('library_name', sa.String(length=50), nullable=False),
    sa.Column('library_source', sa.Enum('GENOMIC', 'TRANSCRIPTOMIC', 'GENOMIC_SINGLE_CELL', 'METAGENOMIC', 'METATRANSCRIPTOMIC', 'TRANSCRIPTOMIC_SINGLE_CELL', 'SYNTHETIC', 'VIRAL_RNA', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('library_strategy', sa.Enum('WGS', 'WXS', 'WGA', 'RNA-SEQ', 'CHIP-SEQ', 'ATAC-SEQ', 'MIRNA-SEQ', 'NCRNA-SEQ', 'FL-CDNA', 'EST', 'HI-C', 'DNASE-SEQ', 'WCS', 'RAD-SEQ', 'CLONE', 'POOLCLONE', 'AMPLICON', 'CLONEEND', 'FINISHING', 'MNASE-SEQ', 'DNASE-HYPERSENSITIVITY', 'BISULFITE-SEQ', 'CTS', 'MRE-SEQ', 'MEDIP-SEQ', 'MBD-SEQ', 'TN-SEQ', 'VALIDATION', 'FAIRE-SEQ', 'SELEX', 'RIP-SEQ', 'CHIA-PET', 'SYNTHETIC-LONG-READ', 'TARGETED-CAPTURE', 'TETHERED', 'NOME-SEQ', 'CHIRP SEQ', '4-C-SEQ', '5-C-SEQ', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('experiment_type', sa.Enum('POLYA-RNA', 'POLYA-RNA-3P', 'TOTAL-RNA', 'SMALL-RNA', 'WGS', 'WGA', 'WXS', 'WXS-UTR', 'RIBOSOME-PROFILING', 'RIBODEPLETION', '16S', 'NCRNA-SEQ', 'FL-CDNA', 'EST', 'HI-C', 'DNASE-SEQ', 'WCS', 'RAD-SEQ', 'CLONE', 'POOLCLONE', 'AMPLICON', 'CLONEEND', 'FINISHING', 'DNASE-HYPERSENSITIVITY', 'RRBS-SEQ', 'WGBS', 'CTS', 'MRE-SEQ', 'MEDIP-SEQ', 'MBD-SEQ', 'TN-SEQ', 'VALIDATION', 'FAIRE-SEQ', 'SELEX', 'RIP-SEQ', 'CHIA-PET', 'SYNTHETIC-LONG-READ', 'TARGETED-CAPTURE', 'TETHERED', 'NOME-SEQ', 'CHIRP-SEQ', '4-C-SEQ', '5-C-SEQ', 'METAGENOMIC', 'METATRANSCRIPTOMIC', 'TF', 'H3K27ME3', 'H3K27AC', 'H3K9ME3', 'H3K36ME3', 'H3F3A', 'H3K4ME1', 'H3K79ME2', 'H3K79ME3', 'H3K9ME1', 'H3K9ME2', 'H4K20ME1', 'H2AFZ', 'H3AC', 'H3K4ME2', 'H3K4ME3', 'H3K9AC', 'HISTONE-NARROW', 'HISTONE-BROAD', 'CHIP-INPUT', 'ATAC-SEQ', 'TENX-TRANSCRIPTOME-3P', 'TENX-TRANSCRIPTOME-5P', 'DROP-SEQ-TRANSCRIPTOME', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('library_layout', sa.Enum('SINGLE', 'PAIRED', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.Column('status', sa.Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.Column('platform_name', sa.Enum('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ', 'NANOPORE_MINION', 'DNBSEQ-G400', 'DNBSEQ-G50', 'DNBSEQ-T7', 'UNKNOWN'), server_default='UNKNOWN', nullable=False),
    sa.ForeignKeyConstraint(['project_id'], ['project.project_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['sample_id'], ['sample.sample_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('experiment_id'),
    sa.UniqueConstraint('experiment_igf_id'),
    sa.UniqueConstraint('sample_id', 'library_name', 'platform_name'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('sample_attribute',
    sa.Column('sample_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=50), nullable=True),
    sa.Column('attribute_value', sa.String(length=50), nullable=True),
    sa.Column('sample_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['sample_id'], ['sample.sample_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('sample_attribute_id'),
    sa.UniqueConstraint('sample_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('seqrun_attribute',
    sa.Column('seqrun_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=50), nullable=True),
    sa.Column('attribute_value', sa.String(length=100), nullable=True),
    sa.Column('seqrun_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['seqrun_id'], ['seqrun.seqrun_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('seqrun_attribute_id'),
    sa.UniqueConstraint('seqrun_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('seqrun_stats',
    sa.Column('seqrun_stats_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('seqrun_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('lane_number', sa.Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False),
    sa.Column('bases_mask', sa.String(length=20), nullable=True),
    sa.Column('undetermined_barcodes', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.Column('known_barcodes', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.Column('undetermined_fastqc', igf_data.igfdb.datatype.JSONType(), nullable=True),
    sa.ForeignKeyConstraint(['seqrun_id'], ['seqrun.seqrun_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('seqrun_stats_id'),
    sa.UniqueConstraint('seqrun_id', 'lane_number'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('experiment_attribute',
    sa.Column('experiment_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=30), nullable=True),
    sa.Column('attribute_value', sa.String(length=50), nullable=True),
    sa.Column('experiment_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['experiment_id'], ['experiment.experiment_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('experiment_attribute_id'),
    sa.UniqueConstraint('experiment_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('run',
    sa.Column('run_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('run_igf_id', sa.String(length=70), nullable=False),
    sa.Column('experiment_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('seqrun_id', mysql.INTEGER(unsigned=True), nullable=True),
    sa.Column('status', sa.Enum('ACTIVE', 'FAILED', 'WITHDRAWN'), server_default='ACTIVE', nullable=False),
    sa.Column('lane_number', sa.Enum('1', '2', '3', '4', '5', '6', '7', '8'), nullable=False),
    sa.Column('date_created', sa.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False),
    sa.ForeignKeyConstraint(['experiment_id'], ['experiment.experiment_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.ForeignKeyConstraint(['seqrun_id'], ['seqrun.seqrun_id'], onupdate='CASCADE', ondelete='SET NULL'),
    sa.PrimaryKeyConstraint('run_id'),
    sa.UniqueConstraint('experiment_id', 'seqrun_id', 'lane_number'),
    sa.UniqueConstraint('run_igf_id'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    op.create_table('run_attribute',
    sa.Column('run_attribute_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.Column('attribute_name', sa.String(length=30), nullable=True),
    sa.Column('attribute_value', sa.String(length=50), nullable=True),
    sa.Column('run_id', mysql.INTEGER(unsigned=True), nullable=False),
    sa.ForeignKeyConstraint(['run_id'], ['run.run_id'], onupdate='CASCADE', ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('run_attribute_id'),
    sa.UniqueConstraint('run_id', 'attribute_name', 'attribute_value'),
    mysql_charset='utf8',
    mysql_engine='InnoDB'
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('run_attribute')
    op.drop_table('run')
    op.drop_table('experiment_attribute')
    op.drop_table('seqrun_stats')
    op.drop_table('seqrun_attribute')
    op.drop_table('sample_attribute')
    op.drop_table('experiment')
    op.drop_table('seqrun')
    op.drop_table('sample')
    op.drop_table('project_user')
    op.drop_table('project_attribute')
    op.drop_table('pipeline_seed')
    op.drop_table('flowcell_barcode_rule')
    op.drop_table('file_attribute')
    op.drop_table('collection_group')
    op.drop_table('collection_attribute')
    op.drop_table('analysis')
    op.drop_table('user')
    op.drop_table('project')
    op.drop_table('platform')
    op.drop_table('pipeline')
    op.drop_table('history')
    op.drop_table('file')
    op.drop_table('collection')
    # ### end Alembic commands ###
