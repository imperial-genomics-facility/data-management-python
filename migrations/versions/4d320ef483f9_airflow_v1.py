"""airflow_v1

Revision ID: 4d320ef483f9
Revises: 4c97401b8961
Create Date: 2021-02-07 17:55:06.960019

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '4d320ef483f9'
down_revision = '4c97401b8961'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('analysis', sa.Column('analysis_name', sa.String(length=120), nullable=False))
    op.create_unique_constraint(None, 'analysis', [ 'project_id','analysis_name'])
    op.drop_index('project_id','analysis')
    op.execute("ALTER TABLE `pipeline_seed` MODIFY COLUMN `seed_table` ENUM('project','sample','experiment','run','file','seqrun','collection','analysis','unknown') NOT NULL DEFAULT 'unknown'")
    op.execute("ALTER TABLE `collection` MODIFY COLUMN `table` ENUM('sample','experiment','run','file','project','seqrun','analysis','unknown') NOT NULL DEFAULT 'unknown'")
    op.execute("ALTER TABLE `pipeline` MODIFY COLUMN `pipeline_type` ENUM('EHIVE','AIRFLOW','NEXTFLOW','UNKNOWN') NOT NULL DEFAULT 'EHIVE'")
    op.execute("ALTER TABLE `analysis` MODIFY COLUMN `analysis_type` varchar(120) NOT NULL")
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_unique_constraint(None, 'analysis', 'project_id')
    op.drop_constraint(None, 'analysis', type_='unique')
    op.drop_column('analysis', 'analysis_name')
    op.execute("ALTER TABLE `pipeline_seed` MODIFY COLUMN `seed_table` ENUM('project','sample','experiment','run','file','seqrun','collection','unknown') NOT NULL DEFAULT 'unknown'")
    op.execute("ALTER TABLE `collection` MODIFY COLUMN `table` ENUM('sample','experiment','run','file','project','seqrun','unknown') NOT NULL DEFAULT 'unknown'")
    op.execute("ALTER TABLE `pipeline` MODIFY COLUMN `pipeline_type` ENUM('EHIVE','UNKNOWN') NOT NULL DEFAULT 'EHIVE'")
    op.execute("ALTER TABLE `analysis` MODIFY COLUMN `analysis_type` enum('RNA_DIFFERENTIAL_EXPRESSION','RNA_TIME_SERIES','CHIP_PEAK_CALL','SOMATIC_VARIANT_CALLING','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN'")
    # ### end Alembic commands ###