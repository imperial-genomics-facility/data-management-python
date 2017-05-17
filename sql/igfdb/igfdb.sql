SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';


-- -----------------------------------------------------
-- Schema igfdb
-- -----------------------------------------------------
USE `igfdb` ;

-- -----------------------------------------------------
-- Table `igfdb`.`project`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`project` (
  `project_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(20) NOT NULL,
  `project_name` VARCHAR(100) NULL DEFAULT NULL,
  `start_date` DATE NOT NULL,
  `project_requirement` ENUM('FASTQ', 'ALIGNMENT', 'ANALYSIS') NULL DEFAULT NULL,
  PRIMARY KEY (`project_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`user`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`user` (
  `user_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_igf_id` VARCHAR(10) NULL,
  `name` VARCHAR(25) NOT NULL,
  `category` ENUM('IMPERIAL_HPC', 'EXTERNAL') NULL DEFAULT NULL,
  `status` ENUM('ACTIVE', 'BLOCKED', 'WITHDRAWN') NOT NULL,
  `email_id` VARCHAR(20) NULL DEFAULT NULL,
  `date_created` DATE NULL DEFAULT NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE INDEX `date_created_UNIQUE` (`date_created` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`project_user`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`project_user` (
  `project_id` INT UNSIGNED  NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `data_authority` ENUM('T') NULL,
  UNIQUE INDEX `project_id_UNIQUE` (`project_id` ASC),
  UNIQUE INDEX `data_authority_UNIQUE` (`data_authority` ASC),
  UNIQUE INDEX `user_id_UNIQUE` (`user_id` ASC),
  CONSTRAINT `fk_project_user_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_project_user_2`
    FOREIGN KEY (`user_id`)
    REFERENCES `igfdb`.`user` (`user_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`sample`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`sample` (
  `sample_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(10) NOT NULL,
  `taxon_id` INT NULL DEFAULT NULL,
  `scientific_name` VARCHAR(50) NULL DEFAULT NULL,
  `common_name` VARCHAR(50) NULL DEFAULT NULL,
  `donor_anonymized_id` VARCHAR(10) NULL DEFAULT NULL,
  `description` VARCHAR(50) NULL DEFAULT NULL,
  `phenotype` VARCHAR(45) NULL DEFAULT NULL,
  `sex` ENUM('F', 'M', 'O', 'U') NOT NULL,
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL,
  `biomaterial_type` ENUM('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE') NOT NULL,
  `cell_type` VARCHAR(50) NULL,
  `tissue_type` VARCHAR(50) NULL DEFAULT NULL,
  `cell_line` VARCHAR(50) NULL,
  `project_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`sample_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id` ASC),
  INDEX `fk_sample_1_idx` (`project_id` ASC),
  CONSTRAINT `fk_sample_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`platform`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`platform` (
  `platform_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(10) NULL DEFAULT NULL,
  `model_name` ENUM('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ') NOT NULL,
  `vendor_name` ENUM('ILLUMINA', 'NANOPORE') NOT NULL,
  `software_name` ENUM('RTA') NOT NULL,
  `software_version` ENUM('RTA1.18.54', 'RTA1.18.64', 'RTA2') NOT NULL,
  PRIMARY KEY (`platform_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`run`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`run` (
  `run_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(30) NULL DEFAULT NULL,
  `flowcell_id` VARCHAR(10) NOT NULL,
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL,
  `lane_number` ENUM('1', '2', '3', '4', '5', '6', '7', '8') NOT NULL,
  PRIMARY KEY (`run_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id` ASC))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`experiment`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`experiment` (
  `experiment_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_id` INT UNSIGNED NOT NULL,
  `sample_id` INT UNSIGNED NULL DEFAULT NULL,
  `library_name` VARCHAR(50) NULL,
  `library_strategy` ENUM('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ') NOT NULL,
  `experiment_type` ENUM('POLYA-RNA', 'TOTAL-RNA', 'H3K4ME3', 'WGS', 'EXOME', 'H3k27me3', 'H3K27ac', 'H3K9me3', 'H3K36me3') NOT NULL,
  `library_layout`  ENUM('SINGLE', 'PAIRED') NOT NULL,
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL,
  `platform_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`experiment_id`),
  INDEX `fk_library_1_idx` (`platform_id` ASC),
  UNIQUE INDEX `sample_id_UNIQUE` (`sample_id` ASC),
  UNIQUE INDEX `library_name_UNIQUE` (`library_name` ASC),
  UNIQUE INDEX `platform_id_UNIQUE` (`platform_id` ASC),
  CONSTRAINT `fk_project_run_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_project_run_2`
    FOREIGN KEY (`sample_id`)
    REFERENCES `igfdb`.`sample` (`sample_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_library_1`
    FOREIGN KEY (`platform_id`)
    REFERENCES `igfdb`.`platform` (`platform_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`collection`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`collection` (
  `collection_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(20) NOT NULL,
  `type` VARCHAR(30) NOT NULL,
  `table` ENUM('sample', 'experiment', 'run', 'file') NOT NULL,
  PRIMARY KEY (`collection_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`file`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`file` (
  `file_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `path` VARCHAR(500) NOT NULL,
  `location` ENUM('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS') NOT NULL,
  `type` ENUM('BCL_TAR', 'FASTQ_TAR', 'FASTQC_TAR', 'FASTQ', 'BAM', 'CRAM', 'GFF', 'BED', 'GTF', 'FASTA') NOT NULL,
  `md5` VARCHAR(33) NULL,
  `size` VARCHAR(15) NULL,
  PRIMARY KEY (`file_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`collection_group`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`collection_group` (
  `collection_id` INT UNSIGNED NOT NULL,
  `file_id` INT UNSIGNED NOT NULL,
  INDEX `fk_collection_group_2_idx` (`file_id` ASC),
  UNIQUE INDEX `collection_id_UNIQUE` (`collection_id` ASC),
  UNIQUE INDEX `file_id_UNIQUE` (`file_id` ASC),
  CONSTRAINT `fk_collection_group_1`
    FOREIGN KEY (`collection_id`)
    REFERENCES `igfdb`.`collection` (`collection_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_collection_group_2`
    FOREIGN KEY (`file_id`)
    REFERENCES `igfdb`.`file` (`file_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`pipeline`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`pipeline` (
  `pipeline_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `pipeline_name` VARCHAR(50) NOT NULL,
  `is_active` ENUM('Y', 'N') NOT NULL,
  PRIMARY KEY (`pipeline_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`hive_db`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`hive_db` (
  `hive_db_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `dbname` VARCHAR(500) NOT NULL,
  `is_active` ENUM('Y', 'N') NOT NULL,
  PRIMARY KEY (`hive_db_id`))
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`pipeline_seed`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`pipeline_seed` (
  `pipeline_seed_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `pipeline_id` INT UNSIGNED NOT NULL,
  `hive_db_id` INT UNSIGNED NOT NULL,
  `status` ENUM('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN') NOT NULL,
  PRIMARY KEY (`pipeline_seed_id`),
  INDEX `fk_pipeline_seed_1_idx` (`pipeline_id` ASC),
  INDEX `fk_pipeline_seed_2_idx` (`hive_db_id` ASC),
  CONSTRAINT `fk_pipeline_seed_1`
    FOREIGN KEY (`pipeline_id`)
    REFERENCES `igfdb`.`pipeline` (`pipeline_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_pipeline_seed_2`
    FOREIGN KEY (`hive_db_id`)
    REFERENCES `igfdb`.`hive_db` (`hive_db_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`project_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`project_attribute` (
  `project_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_attribute_name` VARCHAR(50) NULL,
  `project_attribute_value` VARCHAR(50) NULL,
  `project_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`project_attribute_id`),
  UNIQUE INDEX `project_attribute_name_UNIQUE` (`project_attribute_name` ASC),
  INDEX `fk_project_attribute_1_idx` (`project_id` ASC),
  UNIQUE INDEX `project_id_UNIQUE` (`project_id` ASC),
  CONSTRAINT `fk_project_attribute_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`experiment_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`experiment_attribute` (
  `experiment_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `experiment_attribute_name` VARCHAR(30) NULL,
  `experiment_attribute_value` VARCHAR(50) NULL,
  `experiment_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`experiment_attribute_id`),
  UNIQUE INDEX `library_attribute_name_UNIQUE` (`experiment_attribute_name` ASC),
  INDEX `fk_library_attribute_1_idx` (`experiment_id` ASC),
  UNIQUE INDEX `library_id_UNIQUE` (`experiment_id` ASC),
  CONSTRAINT `fk_library_attribute_1`
    FOREIGN KEY (`experiment_id`)
    REFERENCES `igfdb`.`experiment` (`experiment_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`collection_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`collection_attribute` (
  `collection_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `collection_attribute_name` VARCHAR(45) NULL,
  `collection_attribute_value` VARCHAR(45) NULL,
  `collection_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`collection_attribute_id`),
  UNIQUE INDEX `collection_attribute_name_UNIQUE` (`collection_attribute_name` ASC),
  INDEX `fk_collection_attribute_1_idx` (`collection_id` ASC),
  UNIQUE INDEX `collection_id_UNIQUE` (`collection_id` ASC),
  CONSTRAINT `fk_collection_attribute_1`
    FOREIGN KEY (`collection_id`)
    REFERENCES `igfdb`.`collection` (`collection_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`sample_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`sample_attribute` (
  `sample_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `sample_attribute_name` VARCHAR(30) NULL,
  `sample_attribute_value` VARCHAR(50) NULL,
  `sample_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`sample_attribute_id`),
  UNIQUE INDEX `sample_attribute_name_UNIQUE` (`sample_attribute_name` ASC),
  UNIQUE INDEX `sample_attributecol_UNIQUE` (`sample_id` ASC),
  CONSTRAINT `fk_sample_attribute_1`
    FOREIGN KEY (`sample_id`)
    REFERENCES `igfdb`.`sample` (`sample_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`run_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`run_attribute` (
  `run_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `run_attribute_name` VARCHAR(30) NULL,
  `run_attribute_value` VARCHAR(50) NULL,
  `run_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`run_attribute_id`),
  UNIQUE INDEX `run_attribute_name_UNIQUE` (`run_attribute_name` ASC),
  UNIQUE INDEX `run_id_UNIQUE` (`run_id` ASC),
  CONSTRAINT `fk_run_attribute_1`
    FOREIGN KEY (`run_id`)
    REFERENCES `igfdb`.`run` (`run_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


-- -----------------------------------------------------
-- Table `igfdb`.`file_attribute`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`file_attribute` (
  `file_attribute_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `file_attribute_name` VARCHAR(30) NULL,
  `file_attribute_value` VARCHAR(50) NULL,
  `file_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`file_attribute_id`),
  UNIQUE INDEX `file_attribute_name_UNIQUE` (`file_attribute_name` ASC),
  UNIQUE INDEX `file_id_UNIQUE` (`file_id` ASC),
  CONSTRAINT `fk_file_attribute_1`
    FOREIGN KEY (`file_id`)
    REFERENCES `igfdb`.`file` (`file_id`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
