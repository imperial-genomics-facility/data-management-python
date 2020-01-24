## PROJECT
ALTER TABLE `project` ADD COLUMN `status` enum('ACTIVE','FINISHED','WITHDRAWN') NOT NULL DEFAULT 'ACTIVE';
## USER
ALTER TABLE `user` ADD COLUMN `orcid_id` varchar(50) DEFAULT NULL;
## PLATFORM
ALTER TABLE `platform` MODIFY COLUMN `model_name` enum('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ','NOVASEQ6000','NANOPORE_MINION','DNBSEQ-G400','DNBSEQ-G50','DNBSEQ-T7') NOT NULL;
ALTER TABLE `platform` MODIFY COLUMN `vendor_name` enum('ILLUMINA','NANOPORE','MGI') NOT NULL;
## EXPERIMENT
ALTER TABLE `experiment` MODIFY COLUMN `library_source` enum('GENOMIC','TRANSCRIPTOMIC','GENOMIC_SINGLE_CELL','METAGENOMIC','METATRANSCRIPTOMIC','TRANSCRIPTOMIC_SINGLE_CELL','SYNTHETIC','VIRAL_RNA','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE `experiment` MODIFY COLUMN `library_strategy` enum('WGS','WXS','WGA','RNA-SEQ','CHIP-SEQ','ATAC-SEQ','MIRNA-SEQ','NCRNA-SEQ','FL-CDNA','EST','HI-C','DNASE-SEQ','WCS','RAD-SEQ','CLONE','POOLCLONE','AMPLICON','CLONEEND','FINISHING','MNASE-SEQ','DNASE-HYPERSENSITIVITY','BISULFITE-SEQ','CTS','MRE-SEQ','MEDIP-SEQ','MBD-SEQ','TN-SEQ','VALIDATION','FAIRE-SEQ','SELEX','RIP-SEQ','CHIA-PET','SYNTHETIC-LONG-READ','TARGETED-CAPTURE','TETHERED','NOME-SEQ','CHIRP SEQ','4-C-SEQ','5-C-SEQ','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE `experiment` MODIFY COLUMN `platform_name` enum('HISEQ2500','HISEQ4000','MISEQ','NEXTSEQ','NANOPORE_MINION','DNBSEQ-G400','DNBSEQ-G50','DNBSEQ-T7','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN';
ALTER TABLE `experiment` MODIFY COLUMN `experiment_type` enum('POLYA-RNA','POLYA-RNA-3P','TOTAL-RNA','SMALL-RNA','WGS','WGA','WXS','WXS-UTR','RIBOSOME-PROFILING','RIBODEPLETION','16S','NCRNA-SEQ','FL-CDNA','EST','HI-C','DNASE-SEQ','WCS','RAD-SEQ','CLONE','POOLCLONE','AMPLICON','CLONEEND','FINISHING','DNASE-HYPERSENSITIVITY','RRBS-SEQ','WGBS','CTS','MRE-SEQ','MEDIP-SEQ','MBD-SEQ','TN-SEQ','VALIDATION','FAIRE-SEQ','SELEX','RIP-SEQ','CHIA-PET','SYNTHETIC-LONG-READ','TARGETED-CAPTURE','TETHERED','NOME-SEQ','CHIRP-SEQ','4-C-SEQ','5-C-SEQ','METAGENOMIC','METATRANSCRIPTOMIC','TF','H3K27ME3','H3K27AC','H3K9ME3','H3K36ME3','H3F3A','H3K4ME1','H3K79ME2','H3K79ME3','H3K9ME1','H3K9ME2','H4K20ME1','H2AFZ','H3AC','H3K4ME2','H3K4ME3','H3K9AC','HISTONE-NARROW','HISTONE-BROAD','CHIP-INPUT','ATAC-SEQ','TENX-TRANSCRIPTOME-3P','TENX-TRANSCRIPTOME-5P','DROP-SEQ-TRANSCRIPTOME','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN';