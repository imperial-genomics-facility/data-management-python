CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> bd380507518c

INSERT INTO alembic_version (version_num) VALUES ('bd380507518c');

-- Running upgrade bd380507518c -> 4c97401b8961

CREATE TABLE collection (
    collection_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    name VARCHAR(70) NOT NULL, 
    type VARCHAR(50) NOT NULL, 
    `table` ENUM('sample','experiment','run','file','project','seqrun','unknown') NOT NULL DEFAULT 'unknown', 
    date_stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (collection_id), 
    UNIQUE (name, type)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE file (
    file_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    file_path VARCHAR(500) NOT NULL, 
    location ENUM('ORWELL','HPC_PROJECT','ELIOT','IRODS','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    status ENUM('ACTIVE','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    md5 VARCHAR(33), 
    size VARCHAR(15), 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    date_updated TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (file_id), 
    UNIQUE (file_path)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE history (
    log_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    log_type ENUM('CREATED','MODIFIED','DELETED') NOT NULL, 
    table_name ENUM('PROJECT','USER','SAMPLE','EXPERIMENT','RUN','COLLECTION','FILE','PLATFORM','PROJECT_ATTRIBUTE','EXPERIMENT_ATTRIBUTE','COLLECTION_ATTRIBUTE','SAMPLE_ATTRIBUTE','RUN_ATTRIBUTE','FILE_ATTRIBUTE') NOT NULL, 
    log_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    message TEXT, 
    PRIMARY KEY (log_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE pipeline (
    pipeline_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    pipeline_name VARCHAR(50) NOT NULL, 
    pipeline_db VARCHAR(200) NOT NULL, 
    pipeline_init_conf JSON, 
    pipeline_run_conf JSON, 
    pipeline_type ENUM('EHIVE','UNKNOWN') NOT NULL DEFAULT 'EHIVE', 
    is_active ENUM('Y','N') NOT NULL DEFAULT 'Y', 
    date_stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (pipeline_id), 
    UNIQUE (pipeline_name)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE platform (
    platform_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    platform_igf_id VARCHAR(10) NOT NULL, 
    model_name ENUM('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ','NOVASEQ6000','NANOPORE_MINION','DNBSEQ-G400','DNBSEQ-G50','DNBSEQ-T7') NOT NULL, 
    vendor_name ENUM('ILLUMINA','NANOPORE','MGI') NOT NULL, 
    software_name ENUM('RTA','UNKNOWN') NOT NULL, 
    software_version VARCHAR(20) NOT NULL DEFAULT 'UNKNOWN', 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (platform_id), 
    UNIQUE (platform_igf_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE project (
    project_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    project_igf_id VARCHAR(50) NOT NULL, 
    project_name VARCHAR(40), 
    start_timestamp TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP, 
    description TEXT, 
    status ENUM('ACTIVE','FINISHED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    deliverable ENUM('FASTQ','ALIGNMENT','ANALYSIS') DEFAULT 'FASTQ', 
    PRIMARY KEY (project_id), 
    UNIQUE (project_igf_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE user (
    user_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    user_igf_id VARCHAR(10), 
    name VARCHAR(30) NOT NULL, 
    email_id VARCHAR(40) NOT NULL, 
    username VARCHAR(20), 
    hpc_username VARCHAR(20), 
    twitter_user VARCHAR(20), 
    orcid_id VARCHAR(50), 
    category ENUM('HPC_USER','NON_HPC_USER','EXTERNAL') NOT NULL DEFAULT 'NON_HPC_USER', 
    status ENUM('ACTIVE','BLOCKED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    password VARCHAR(129), 
    encryption_salt VARCHAR(129), 
    ht_password VARCHAR(40), 
    PRIMARY KEY (user_id), 
    UNIQUE (email_id), 
    UNIQUE (name), 
    UNIQUE (username)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE analysis (
    analysis_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    project_id INTEGER UNSIGNED, 
    analysis_type ENUM('RNA_DIFFERENTIAL_EXPRESSION','RNA_TIME_SERIES','CHIP_PEAK_CALL','SOMATIC_VARIANT_CALLING','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    analysis_description JSON, 
    PRIMARY KEY (analysis_id), 
    FOREIGN KEY(project_id) REFERENCES project (project_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    UNIQUE (project_id, analysis_type)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE collection_attribute (
    collection_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(200), 
    attribute_value VARCHAR(200), 
    collection_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (collection_attribute_id), 
    FOREIGN KEY(collection_id) REFERENCES collection (collection_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (collection_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE collection_group (
    collection_group_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    collection_id INTEGER UNSIGNED NOT NULL, 
    file_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (collection_group_id), 
    FOREIGN KEY(collection_id) REFERENCES collection (collection_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY(file_id) REFERENCES file (file_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (collection_id, file_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE file_attribute (
    file_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(30), 
    attribute_value VARCHAR(50), 
    file_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (file_attribute_id), 
    FOREIGN KEY(file_id) REFERENCES file (file_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (file_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE flowcell_barcode_rule (
    flowcell_rule_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    platform_id INTEGER UNSIGNED, 
    flowcell_type VARCHAR(50) NOT NULL, 
    index_1 ENUM('NO_CHANGE','REVCOMP','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    index_2 ENUM('NO_CHANGE','REVCOMP','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    PRIMARY KEY (flowcell_rule_id), 
    FOREIGN KEY(platform_id) REFERENCES platform (platform_id) ON DELETE SET NULL ON UPDATE CASCADE
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE pipeline_seed (
    pipeline_seed_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    seed_id INTEGER UNSIGNED NOT NULL, 
    seed_table ENUM('project','sample','experiment','run','file','seqrun','collection','unknown') NOT NULL DEFAULT 'unknown', 
    pipeline_id INTEGER UNSIGNED NOT NULL, 
    status ENUM('SEEDED','RUNNING','FINISHED','FAILED','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    date_stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (pipeline_seed_id), 
    FOREIGN KEY(pipeline_id) REFERENCES pipeline (pipeline_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (pipeline_id, seed_id, seed_table)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE project_attribute (
    project_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(50), 
    attribute_value VARCHAR(50), 
    project_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (project_attribute_id), 
    FOREIGN KEY(project_id) REFERENCES project (project_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (project_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE project_user (
    project_user_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    project_id INTEGER UNSIGNED NOT NULL, 
    user_id INTEGER UNSIGNED NOT NULL, 
    data_authority ENUM('T'), 
    PRIMARY KEY (project_user_id), 
    FOREIGN KEY(project_id) REFERENCES project (project_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    FOREIGN KEY(user_id) REFERENCES user (user_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (project_id, data_authority)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE sample (
    sample_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    sample_igf_id VARCHAR(20) NOT NULL, 
    sample_submitter_id VARCHAR(40), 
    taxon_id INTEGER UNSIGNED, 
    scientific_name VARCHAR(50), 
    species_name VARCHAR(50), 
    donor_anonymized_id VARCHAR(10), 
    description VARCHAR(50), 
    phenotype VARCHAR(45), 
    sex ENUM('FEMALE','MALE','MIXED','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    status ENUM('ACTIVE','FAILED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    biomaterial_type ENUM('PRIMARY_TISSUE','PRIMARY_CELL','PRIMARY_CELL_CULTURE','CELL_LINE','SINGLE_NUCLEI','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    cell_type VARCHAR(50), 
    tissue_type VARCHAR(50), 
    cell_line VARCHAR(50), 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    project_id INTEGER UNSIGNED, 
    PRIMARY KEY (sample_id), 
    FOREIGN KEY(project_id) REFERENCES project (project_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    UNIQUE (sample_igf_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE seqrun (
    seqrun_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    seqrun_igf_id VARCHAR(50) NOT NULL, 
    reject_run ENUM('Y','N') NOT NULL DEFAULT 'N', 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    flowcell_id VARCHAR(20) NOT NULL, 
    platform_id INTEGER UNSIGNED, 
    PRIMARY KEY (seqrun_id), 
    FOREIGN KEY(platform_id) REFERENCES platform (platform_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    UNIQUE (seqrun_igf_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE experiment (
    experiment_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    experiment_igf_id VARCHAR(40) NOT NULL, 
    project_id INTEGER UNSIGNED, 
    sample_id INTEGER UNSIGNED, 
    library_name VARCHAR(50) NOT NULL, 
    library_source ENUM('GENOMIC','TRANSCRIPTOMIC','GENOMIC_SINGLE_CELL','METAGENOMIC','METATRANSCRIPTOMIC','TRANSCRIPTOMIC_SINGLE_CELL','SYNTHETIC','VIRAL_RNA','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    library_strategy ENUM('WGS','WXS','WGA','RNA-SEQ','CHIP-SEQ','ATAC-SEQ','MIRNA-SEQ','NCRNA-SEQ','FL-CDNA','EST','HI-C','DNASE-SEQ','WCS','RAD-SEQ','CLONE','POOLCLONE','AMPLICON','CLONEEND','FINISHING','MNASE-SEQ','DNASE-HYPERSENSITIVITY','BISULFITE-SEQ','CTS','MRE-SEQ','MEDIP-SEQ','MBD-SEQ','TN-SEQ','VALIDATION','FAIRE-SEQ','SELEX','RIP-SEQ','CHIA-PET','SYNTHETIC-LONG-READ','TARGETED-CAPTURE','TETHERED','NOME-SEQ','CHIRP SEQ','4-C-SEQ','5-C-SEQ','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    experiment_type ENUM('POLYA-RNA','POLYA-RNA-3P','TOTAL-RNA','SMALL-RNA','WGS','WGA','WXS','WXS-UTR','RIBOSOME-PROFILING','RIBODEPLETION','16S','NCRNA-SEQ','FL-CDNA','EST','HI-C','DNASE-SEQ','WCS','RAD-SEQ','CLONE','POOLCLONE','AMPLICON','CLONEEND','FINISHING','DNASE-HYPERSENSITIVITY','RRBS-SEQ','WGBS','CTS','MRE-SEQ','MEDIP-SEQ','MBD-SEQ','TN-SEQ','VALIDATION','FAIRE-SEQ','SELEX','RIP-SEQ','CHIA-PET','SYNTHETIC-LONG-READ','TARGETED-CAPTURE','TETHERED','NOME-SEQ','CHIRP-SEQ','4-C-SEQ','5-C-SEQ','METAGENOMIC','METATRANSCRIPTOMIC','TF','H3K27ME3','H3K27AC','H3K9ME3','H3K36ME3','H3F3A','H3K4ME1','H3K79ME2','H3K79ME3','H3K9ME1','H3K9ME2','H4K20ME1','H2AFZ','H3AC','H3K4ME2','H3K4ME3','H3K9AC','HISTONE-NARROW','HISTONE-BROAD','CHIP-INPUT','ATAC-SEQ','TENX-TRANSCRIPTOME-3P','TENX-TRANSCRIPTOME-5P','DROP-SEQ-TRANSCRIPTOME','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    library_layout ENUM('SINGLE','PAIRED','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    status ENUM('ACTIVE','FAILED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    platform_name ENUM('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ','NANOPORE_MINION','DNBSEQ-G400','DNBSEQ-G50','DNBSEQ-T7','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN', 
    PRIMARY KEY (experiment_id), 
    FOREIGN KEY(project_id) REFERENCES project (project_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    FOREIGN KEY(sample_id) REFERENCES sample (sample_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    UNIQUE (experiment_igf_id), 
    UNIQUE (sample_id, library_name, platform_name)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE sample_attribute (
    sample_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(50), 
    attribute_value VARCHAR(50), 
    sample_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (sample_attribute_id), 
    FOREIGN KEY(sample_id) REFERENCES sample (sample_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (sample_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE seqrun_attribute (
    seqrun_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(50), 
    attribute_value VARCHAR(100), 
    seqrun_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (seqrun_attribute_id), 
    FOREIGN KEY(seqrun_id) REFERENCES seqrun (seqrun_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (seqrun_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE seqrun_stats (
    seqrun_stats_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    seqrun_id INTEGER UNSIGNED NOT NULL, 
    lane_number ENUM('1','2','3','4','5','6','7','8') NOT NULL, 
    bases_mask VARCHAR(20), 
    undetermined_barcodes JSON, 
    known_barcodes JSON, 
    undetermined_fastqc JSON, 
    PRIMARY KEY (seqrun_stats_id), 
    FOREIGN KEY(seqrun_id) REFERENCES seqrun (seqrun_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (seqrun_id, lane_number)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE experiment_attribute (
    experiment_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(30), 
    attribute_value VARCHAR(50), 
    experiment_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (experiment_attribute_id), 
    FOREIGN KEY(experiment_id) REFERENCES experiment (experiment_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (experiment_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE run (
    run_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    run_igf_id VARCHAR(70) NOT NULL, 
    experiment_id INTEGER UNSIGNED, 
    seqrun_id INTEGER UNSIGNED, 
    status ENUM('ACTIVE','FAILED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE', 
    lane_number ENUM('1','2','3','4','5','6','7','8') NOT NULL, 
    date_created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    PRIMARY KEY (run_id), 
    FOREIGN KEY(experiment_id) REFERENCES experiment (experiment_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    FOREIGN KEY(seqrun_id) REFERENCES seqrun (seqrun_id) ON DELETE SET NULL ON UPDATE CASCADE, 
    UNIQUE (experiment_id, seqrun_id, lane_number), 
    UNIQUE (run_igf_id)
)CHARSET=utf8 ENGINE=InnoDB;

CREATE TABLE run_attribute (
    run_attribute_id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, 
    attribute_name VARCHAR(30), 
    attribute_value VARCHAR(50), 
    run_id INTEGER UNSIGNED NOT NULL, 
    PRIMARY KEY (run_attribute_id), 
    FOREIGN KEY(run_id) REFERENCES run (run_id) ON DELETE CASCADE ON UPDATE CASCADE, 
    UNIQUE (run_id, attribute_name, attribute_value)
)CHARSET=utf8 ENGINE=InnoDB;

UPDATE alembic_version SET version_num='4c97401b8961' WHERE alembic_version.version_num = 'bd380507518c';

-- Running upgrade 4c97401b8961 -> 4d320ef483f9

ALTER TABLE analysis ADD COLUMN analysis_name VARCHAR(120) NOT NULL;

ALTER TABLE analysis ADD UNIQUE (project_id, analysis_name);

DROP INDEX project_id ON analysis;

ALTER TABLE `pipeline_seed` MODIFY COLUMN `seed_table` ENUM('project','sample','experiment','run','file','seqrun','collection','analysis','unknown') NOT NULL DEFAULT 'unknown';

ALTER TABLE `collection` MODIFY COLUMN `table` ENUM('sample','experiment','run','file','project','seqrun','analysis','unknown') NOT NULL DEFAULT 'unknown';

ALTER TABLE `pipeline` MODIFY COLUMN `pipeline_type` ENUM('EHIVE','AIRFLOW','NEXTFLOW','UNKNOWN') NOT NULL DEFAULT 'EHIVE';

ALTER TABLE `analysis` MODIFY COLUMN `analysis_type` varchar(120) NOT NULL;

UPDATE alembic_version SET version_num='4d320ef483f9' WHERE alembic_version.version_num = '4c97401b8961';

