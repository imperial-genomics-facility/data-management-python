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
  `project_name` VARCHAR(100) NULL,
  `start_date` DATE NOT NULL,
  `project_requirement` ENUM('FASTQ', 'ALIGNMENT', 'ANALYSIS') NULL DEFAULT 'FASTQ',
  `description` TEXT NULL,
  PRIMARY KEY (`project_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`user`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`user` (
  `user_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `user_igf_id` VARCHAR(10) NULL,
  `name` VARCHAR(25) NOT NULL,
  `hpc_user_name` VARCHAR(8) NULL,
  `category` ENUM('HPC_USER', 'NON_HPC_USER', 'EXTERNAL') NOT NULL DEFAULT 'NON_HPC_USER',
  `status` ENUM('ACTIVE', 'BLOCKED', 'WITHDRAWN') NOT NULL DEFAULT 'ACTIVE',
  `email_id` VARCHAR(20) NOT NULL,
  `date_stamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `password` VARCHAR(129) NULL,
  PRIMARY KEY (`user_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`user_igf_id`),
  UNIQUE INDEX `email_id_UNIQUE` (`email_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`project_user`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`project_user` (
  `project_user_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_id` INT UNSIGNED NOT NULL,
  `user_id` INT UNSIGNED NOT NULL,
  `data_authority` ENUM('T') NULL,
  PRIMARY KEY (`project_user_id`),
  UNIQUE INDEX `project_data_auth_UNIQUE` (`project_id`, `data_authority`),
  CONSTRAINT `fk_project_user_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_project_user_2`
    FOREIGN KEY (`user_id`)
    REFERENCES `igfdb`.`user` (`user_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`sample`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`sample` (
  `sample_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(10) NOT NULL,
  `taxon_id` INT NULL,
  `scientific_name` VARCHAR(50) NULL,
  `common_name` VARCHAR(50) NULL,
  `donor_anonymized_id` VARCHAR(10) NULL,
  `description` VARCHAR(50) NULL,
  `phenotype` VARCHAR(45) NULL,
  `sex` ENUM('FEMALE', 'MALE', 'OTHER', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL DEFAULT 'ACTIVE',
  `biomaterial_type` ENUM('PRIMARY_TISSUE', 'PRIMARY_CELL', 'PRIMARY_CELL_CULTURE', 'CELL_LINE', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `cell_type` VARCHAR(50) NULL,
  `tissue_type` VARCHAR(50) NULL,
  `cell_line` VARCHAR(50) NULL,
  `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `project_id` INT UNSIGNED NULL,
  PRIMARY KEY (`sample_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id`),
  CONSTRAINT `fk_sample_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`platform`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`platform` (
  `platform_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(10) NOT NULL,
  `model_name` ENUM('HISEQ2500', 'HISEQ4000', 'MISEQ', 'NEXTSEQ') NOT NULL,
  `vendor_name` ENUM('ILLUMINA', 'NANOPORE') NOT NULL,
  `software_name` ENUM('RTA') NOT NULL,
  `software_version` ENUM('RTA1.18.54', 'RTA1.18.64', 'RTA2') NOT NULL,
  `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`platform_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`run`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`run` (
  `run_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(50) NOT NULL,
  `flowcell_id` VARCHAR(10) NOT NULL,
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL DEFAULT 'ACTIVE',
  `lane_number` ENUM('1', '2', '3', '4', '5', '6', '7', '8') NOT NULL,
  `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`run_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id` ASC))
ENGINE = InnoDB CHARSET=UTF8;

-- -----------------------------------------------------
-- Table `igfdb`.`rejected_run`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`rejected_run` (
  `run_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `igf_id` VARCHAR(50) NOT NULL,
 `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`run_id`),
  UNIQUE INDEX `igf_id_UNIQUE` (`igf_id` ASC))
ENGINE = InnoDB CHARSET=UTF8;

-- -----------------------------------------------------
-- Table `igfdb`.`experiment`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`experiment` (
  `experiment_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `project_id` INT UNSIGNED NULL,
  `sample_id` INT UNSIGNED NULL,
  `library_name` VARCHAR(50) NOT NULL,
  `library_strategy` ENUM('WGS', 'EXOME', 'RNA-SEQ', 'CHIP-SEQ','UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `experiment_type` ENUM('POLYA-RNA', 'TOTAL-RNA', 'SMALL_RNA','H3K4ME3', 'WGS', 'EXOME', 'H3k27ME3', 'H3K27AC', 'H3K9ME3', 'H3K36ME3', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `library_layout`  ENUM('SINGLE', 'PAIRED', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `status` ENUM('ACTIVE', 'FAILED', 'WITHDRAWN') NOT NULL DEFAULT 'ACTIVE',
  `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `platform_id` INT UNSIGNED NULL,
  PRIMARY KEY (`experiment_id`),
  UNIQUE INDEX `sample_lib_plat_UNIQUE` (`sample_id`, `library_name`, `platform_id`),
  CONSTRAINT `fk_project_run_1`
    FOREIGN KEY (`project_id`)
    REFERENCES `igfdb`.`project` (`project_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
  CONSTRAINT `fk_project_run_2`
    FOREIGN KEY (`sample_id`)
    REFERENCES `igfdb`.`sample` (`sample_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE,
  CONSTRAINT `fk_platform_1`
    FOREIGN KEY (`platform_id`)
    REFERENCES `igfdb`.`platform` (`platform_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`collection`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`collection` (
  `collection_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(20) NOT NULL,
  `type` VARCHAR(30) NOT NULL,
  `table` ENUM('sample', 'experiment', 'run', 'file') NOT NULL,
  `date_stamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`collection_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`file`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`file` (
  `file_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `file_path` VARCHAR(500) NOT NULL,
  `location` ENUM('ORWELL', 'HPC_PROJECT', 'ELIOT', 'IRODS', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `type` ENUM('BCL_DIR', 'BCL_TAR', 'FASTQ_TAR', 'FASTQC_TAR', 'FASTQ', 'FASTQGZ', 'BAM', 'CRAM', 'GFF', 'BED', 'GTF', 'FASTA' ,'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  `md5` VARCHAR(33) NULL,
  `size` VARCHAR(15) NULL,
  `date_created` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_updated` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`file_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`collection_group`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`collection_group` (
  `collection_group_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `collection_id` INT UNSIGNED NOT NULL,
  `file_id` INT UNSIGNED NOT NULL,
  PRIMARY KEY (`collection_group_id`),
  CONSTRAINT `fk_collection_group_1`
    FOREIGN KEY (`collection_id`)
    REFERENCES `igfdb`.`collection` (`collection_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_collection_group_2`
    FOREIGN KEY (`file_id`)
    REFERENCES `igfdb`.`file` (`file_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`pipeline`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`pipeline` (
  `pipeline_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `pipeline_name` VARCHAR(50) NOT NULL,
  `is_active` ENUM('Y', 'N') NOT NULL,
  `date_stamp` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`pipeline_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`hive_db`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`hive_db` (
  `hive_db_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `dbname` VARCHAR(500) NOT NULL,
  `is_active` ENUM('Y', 'N') NOT NULL,
  PRIMARY KEY (`hive_db_id`))
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`pipeline_seed`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`pipeline_seed` (
  `pipeline_seed_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `pipeline_id` INT UNSIGNED NOT NULL,
  `hive_db_id` INT UNSIGNED NULL,
  `status` ENUM('SEEDED', 'RUNNING', 'FINISHED', 'FAILED', 'UNKNOWN') NOT NULL DEFAULT 'UNKNOWN',
  PRIMARY KEY (`pipeline_seed_id`),
  INDEX `fk_pipeline_seed_1_idx` (`pipeline_id` ASC),
  INDEX `fk_pipeline_seed_2_idx` (`hive_db_id` ASC),
  CONSTRAINT `fk_pipeline_seed_1`
    FOREIGN KEY (`pipeline_id`)
    REFERENCES `igfdb`.`pipeline` (`pipeline_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_pipeline_seed_2`
    FOREIGN KEY (`hive_db_id`)
    REFERENCES `igfdb`.`hive_db` (`hive_db_id`)
    ON DELETE SET NULL
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Table `igfdb`.`history`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `igfdb`.`history` (
  `log_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
  `log_type` ENUM('CREATED', 'MODIFIED', 'DELETED') NOT NULL,
  `table_name` ENUM('PROJECT', 'USER', 'SAMPLE', 'EXPERIMENT', 'RUN', 'COLLECTION', 'FILE', 'PLATFORM', 'PROJECT_ATTRIBUTE', 'EXPERIMENT_ATTRIBUTE', 'COLLECTION_ATTRIBUTE', 'SAMPLE_ATTRIBUTE', 'RUN_ATTRIBUTE', 'FILE_ATTRIBUTE') NOT NULL,
  `log_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `message` TEXT NULL,
  PRIMARY KEY (`log_id`))
ENGINE = InnoDB CHARSET=UTF8;

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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


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
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB CHARSET=UTF8;


-- -----------------------------------------------------
-- Trigger `igfdb`.`user_pass_sha2`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS user_insert_pass //
CREATE TRIGGER user_insert_pass BEFORE INSERT ON user
FOR EACH ROW
  SET NEW.password=sha2(NEW.PASSWORD, 512)
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS user_update_pass //
CREATE TRIGGER user_update_pass BEFORE UPDATE ON user
FOR EACH ROW
  BEGIN
    IF ( OLD.password IS NOT NULL AND NEW.password IS NOT NULL AND OLD.password <> NEW.password ) THEN 
      SET NEW.password=sha2(NEW.PASSWORD, 512);
    ELSEIF ( OLD.password IS NULL AND NEW.password IS NOT NULL) THEN
      SET NEW.password=sha2(NEW.PASSWORD, 512);
    END IF;
END;//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`project_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS project_update_log //
CREATE TRIGGER project_update_log AFTER UPDATE ON project
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'PROJECT', CONCAT('OLD:', OLD.project_id, ',', OLD.igf_id,',',OLD.project_name,',',OLD.start_date,',',OLD.project_requirement,',', IFNULL(OLD.description,'NA'),', NEW:', NEW.project_id, ',', NEW.igf_id,',',NEW.project_name,',',NEW.start_date,',',NEW.project_requirement,',', IFNULL(NEW.description,'NA') ));
//  
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS project_delete_log //
CREATE TRIGGER project_delete_log AFTER DELETE ON project
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'PROJECT', CONCAT('OLD:', OLD.project_id, ',', OLD.igf_id,',',OLD.project_name,',',OLD.start_date,',',OLD.project_requirement,',', IFNULL(OLD.description,'NA') ));
//  
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`user_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS user_update_log //
CREATE TRIGGER user_update_log AFTER UPDATE ON user
FOR EACH ROW
 INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'USER', CONCAT('OLD:', OLD.user_id, ',', OLD.user_igf_id, ',', OLD.name, ',', IFNULL(OLD.hpc_user_name,'NA'), ',', OLD.category, ',', OLD.status,',', OLD.email_id, ',', OLD.date_stamp, ', NEW:', NEW.user_id, ',', NEW.user_igf_id, ',', NEW.name, ',', IFNULL(NEW.hpc_user_name,'NA'), ',',  NEW.category, ',', NEW.status, ',', NEW.email_id, ',', NEW.date_stamp ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS user_delete_log // 
CREATE TRIGGER user_delete_log AFTER DELETE ON user
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'USER', CONCAT('OLD:',  OLD.user_id, ',', OLD.user_igf_id,',', OLD.name,',', IFNULL(OLD.hpc_user_name,'NA'), ',', OLD.category, ',', OLD.status,',', OLD.email_id,',', OLD.date_stamp ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`sample_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS sample_update_log //
CREATE TRIGGER sample_update_log AFTER UPDATE ON sample
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'SAMPLE', CONCAT('OLD:', OLD.sample_id, ',', OLD.igf_id, ',', IFNULL(OLD.taxon_id, 'NA'), ',', IFNULL(OLD.scientific_name, 'NA'), ',', IFNULL(OLD.common_name,'NA'), ',', IFNULL(OLD.donor_anonymized_id, 'NA'), ',', IFNULL(OLD.description, 'NA'), ',', IFNULL(OLD.phenotype, 'NA'), ',', OLD.sex ,',', OLD.status, ',', OLD.biomaterial_type, ',', IFNULL(OLD.cell_type,'NA'), ',', IFNULL(OLD.tissue_type,'NA'), ',', IFNULL(OLD.cell_line,'NA'), ',', IFNULL(OLD.project_id,'NA') ,',NEW:', NEW.sample_id, ',', NEW.igf_id, ',', IFNULL(NEW.taxon_id,'NA'), ',', IFNULL(NEW.scientific_name,'NA'), ',', IFNULL(NEW.common_name,'NA'), ',', IFNULL(NEW.donor_anonymized_id,'NA'), ',', IFNULL(NEW.description,'NA'), ',', IFNULL(NEW.phenotype,'NA'), ',', NEW.sex ,',', NEW.status, ',', NEW.biomaterial_type, ',', IFNULL(NEW.cell_type, 'NA'), IFNULL(NEW.tissue_type,'NA'), ',', IFNULL(NEW.cell_line,'NA'), ',', IFNULL(NEW.project_id,'NA') ) );
//
DELIMITER ;


DELIMITER //
DROP TRIGGER IF EXISTS sample_delete_log //
CREATE TRIGGER sample_delete_log AFTER DELETE ON sample
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'SAMPLE', CONCAT('OLD:', OLD.sample_id, ',', OLD.igf_id, ',', IFNULL(OLD.taxon_id,'NA'), ',', IFNULL(OLD.scientific_name,'NA'), ',', IFNULL(OLD.common_name,'NA'), ',', IFNULL(OLD.donor_anonymized_id,'NA'), ',', IFNULL(OLD.description,'NA'), ',', IFNULL(OLD.phenotype,'NA'), ',', OLD.sex ,',', OLD.status, ',', OLD.biomaterial_type, ',', IFNULL(OLD.cell_type,'NA'), IFNULL(OLD.tissue_type,'NA'), ',', IFNULL(OLD.cell_line,'NA'), ',', IFNULL(OLD.project_id,'NA') ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`experiment_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS experiment_update_log //
CREATE TRIGGER experiment_update_log AFTER UPDATE ON experiment
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'EXPERIMENT', CONCAT('OLD:', OLD.experiment_id, ',', IFNULL(OLD.project_id,'NA'), ',', IFNULL(OLD.sample_id,'NA'), ',', OLD.library_name, ',', OLD.library_strategy, ',', OLD.experiment_type, ',', OLD.library_layout, ',', OLD.status, ',', IFNULL(OLD.platform_id,'NA'), ', NEW:', NEW.experiment_id, ',', IFNULL(NEW.project_id,'NA'), ',', IFNULL(NEW.sample_id,'NA'), ',', NEW.library_name, ',', NEW.library_strategy, ',', NEW.experiment_type, ',', NEW.library_layout, ',', NEW.status, ',', IFNULL(NEW.platform_id,'NA') ));
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS experiment_delete_log //
CREATE TRIGGER experiment_delete_log AFTER DELETE ON experiment
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'EXPERIMENT', CONCAT('OLD:', OLD.experiment_id, ',', IFNULL(OLD.project_id,'NA'), ',', IFNULL(OLD.sample_id,'NA'), ',', OLD.library_name, ',', OLD.library_strategy, ',', OLD.experiment_type, ',', OLD.library_layout, ',', OLD.status, ',', IFNULL(OLD.platform_id,'NA') ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`run_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS run_update_log //
CREATE TRIGGER run_update_log AFTER UPDATE ON run
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'RUN', CONCAT('OLD:', OLD.run_id, ',', OLD.igf_id, ',', OLD.flowcell_id, ',', OLD.status, ',', OLD.lane_number, ', NEW:', NEW.run_id, ',', NEW.igf_id, ',', NEW.flowcell_id, ',', NEW.status, ',', NEW.lane_number  ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS run_delete_log //
CREATE TRIGGER run_delete_log AFTER DELETE ON run
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'RUN', CONCAT('OLD:', OLD.run_id, ',', OLD.igf_id, ',', OLD.flowcell_id, ',', OLD.status, ',', OLD.lane_number ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`platform_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS platform_update_log //
CREATE TRIGGER platform_update_log AFTER UPDATE ON platform
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'PLATFORM', CONCAT('OLD:', OLD.platform_id, ',', OLD.igf_id, ',', OLD.model_name, ',', OLD.vendor_name, ',', OLD.software_name, ',', OLD.software_version, ', NEW:', NEW.platform_id, ',', NEW.igf_id, ',', NEW.model_name, ',', NEW.vendor_name, ',', NEW.software_name, ',', NEW.software_version ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS platform_delete_log //
CREATE TRIGGER platform_delete_log AFTER DELETE ON platform
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'PLATFORM', CONCAT('OLD:', OLD.platform_id, ',', OLD.igf_id, ',', OLD.model_name, ',', OLD.vendor_name, ',', OLD.software_name, ',', OLD.software_version ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`collection_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS collection_update_log //
CREATE TRIGGER collection_update_log AFTER UPDATE ON collection
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'COLLECTION', CONCAT('OLD:', OLD.collection_id, ',', OLD.name, ',', OLD.type, ',', OLD.table, ', NEW:', NEW.collection_id, ',', NEW.name, ',', NEW.type, ',', NEW.table ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS collection_delete_log //
CREATE TRIGGER collection_delete_log AFTER DELETE ON collection
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'COLLECTION', CONCAT('OLD:', OLD.collection_id, ',', OLD.name, ',', OLD.type, ',', OLD.table ) );
//
DELIMITER ;


-- -----------------------------------------------------
-- Trigger `igfdb`.`file_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS file_update_log //
CREATE TRIGGER file_update_log AFTER UPDATE ON file
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'FILE', CONCAT('OLD:', OLD.file_id, ',', OLD.file_path, ',', OLD.location, ',', OLD.type, ',', IFNULL(OLD.md5, 'NA'), ',', IFNULL(OLD.size, 'NA'), ',', OLD.date_updated, ', NEW:', NEW.file_id, ',', NEW.file_path, ',', NEW.location, ',', NEW.type, ',', IFNULL(NEW.md5,'NA'), ',', IFNULL(NEW.size,'NA'), ',', NEW.date_updated ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS file_delete_log //
CREATE TRIGGER file_delete_log AFTER DELETE ON file
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'FILE', CONCAT('OLD:', OLD.file_id, ',', OLD.file_path, ',', OLD.location, ',', OLD.type, ',', IFNULL(OLD.md5,'NA'), ',', IFNULL(OLD.size, 'NA'), ',', OLD.date_updated ) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`project_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS project_attribute_update_log //
CREATE TRIGGER project_attribute_update_log AFTER UPDATE ON project_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'PROJECT_ATTRIBUTE', CONCAT('OLD:', OLD.project_attribute_id, ',', OLD.project_attribute_name, ',', OLD.project_attribute_value, ',', OLD.project_id, ', NEW:',  NEW.project_attribute_id, ',', NEW.project_attribute_name, ',', NEW.project_attribute_value, ',', NEW.project_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS project_attribute_delete_log //
CREATE TRIGGER project_attribute_delete_log AFTER DELETE ON project_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'PROJECT_ATTRIBUTE', CONCAT('OLD:', OLD.project_attribute_id, ',', OLD.project_attribute_name, ',', OLD.project_attribute_value, ',', OLD.project_id) );
//
DELIMITER ;


-- -----------------------------------------------------
-- Trigger `igfdb`.`experiment_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS experiment_attribute_update_log //
CREATE TRIGGER experiment_attribute_update_log AFTER UPDATE ON experiment_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'EXPERIMENT_ATTRIBUTE', CONCAT('OLD:', OLD.experiment_attribute_id, ',', OLD.experiment_attribute_name, ',', OLD.experiment_attribute_value, ',', OLD.experiment_id, ', NEW:',  NEW.experiment_attribute_id, ',', NEW.experiment_attribute_name, ',', NEW.experiment_attribute_value, ',', NEW.experiment_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS experiment_attribute_delete_log //
CREATE TRIGGER experiment_attribute_delete_log AFTER DELETE ON experiment_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'EXPERIMENT_ATTRIBUTE', CONCAT('OLD:', OLD.experiment_attribute_id, ',', OLD.experiment_attribute_name, ',', OLD.experiment_attribute_value, ',', OLD.experiment_id) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`collection_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS collection_attribute_update_log //
CREATE TRIGGER collection_attribute_update_log AFTER UPDATE ON collection_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'COLLECTION_ATTRIBUTE', CONCAT('OLD:', OLD.collection_attribute_id, ',', OLD.collection_attribute_name, ',', OLD.collection_attribute_value, ',', OLD.collection_id, ', NEW:',  NEW.collection_attribute_id, ',', NEW.collection_attribute_name, ',', NEW.collection_attribute_value, ',', NEW.collection_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS collection_attribute_delete_log //
CREATE TRIGGER collection_attribute_delete_log AFTER DELETE ON collection_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'COLLECTION_ATTRIBUTE', CONCAT('OLD:', OLD.collection_attribute_id, ',', OLD.collection_attribute_name, ',', OLD.collection_attribute_value, ',', OLD.collection_id) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`sample_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS sample_attribute_update_log //
CREATE TRIGGER sample_attribute_update_log AFTER UPDATE ON sample_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'SAMPLE_ATTRIBUTE', CONCAT('OLD:', OLD.sample_attribute_id, ',', OLD.sample_attribute_name, ',', OLD.sample_attribute_value, ',', OLD.sample_id, ', NEW:',  NEW.sample_attribute_id, ',', NEW.sample_attribute_name, ',', NEW.sample_attribute_value, ',', NEW.sample_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS sample_attribute_delete_log //
CREATE TRIGGER sample_attribute_delete_log AFTER DELETE ON sample_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'SAMPLE_ATTRIBUTE', CONCAT('OLD:', OLD.sample_attribute_id, ',', OLD.sample_attribute_name, ',', OLD.sample_attribute_value, ',', OLD.sample_id) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`run_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS run_attribute_update_log //
CREATE TRIGGER run_attribute_update_log AFTER UPDATE ON run_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'RUN_ATTRIBUTE', CONCAT('OLD:', OLD.run_attribute_id, ',', OLD.run_attribute_name, ',', OLD.run_attribute_value, ',', OLD.run_id, ', NEW:',  NEW.run_attribute_id, ',', NEW.run_attribute_name, ',', NEW.run_attribute_value, ',', NEW.run_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS run_attribute_delete_log //
CREATE TRIGGER run_attribute_delete_log AFTER DELETE ON run_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'RUN_ATTRIBUTE', CONCAT('OLD:', OLD.run_attribute_id, ',', OLD.run_attribute_name, ',', OLD.run_attribute_value, ',', OLD.run_id) );
//
DELIMITER ;

-- -----------------------------------------------------
-- Trigger `igfdb`.`file_attribute_log`
-- -----------------------------------------------------
DELIMITER //
DROP TRIGGER IF EXISTS file_attribute_update_log //
CREATE TRIGGER file_attribute_update_log AFTER UPDATE ON file_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'FILE_ATTRIBUTE', CONCAT('OLD:', OLD.file_attribute_id, ',', OLD.file_attribute_name, ',', OLD.file_attribute_value, ',', OLD.file_id, ', NEW:',  NEW.file_attribute_id, ',', NEW.file_attribute_name, ',', NEW.file_attribute_value, ',', NEW.file_id) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS file_attribute_delete_log //
CREATE TRIGGER file_attribute_delete_log AFTER DELETE ON file_attribute
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'FILE_ATTRIBUTE', CONCAT('OLD:', OLD.file_attribute_id, ',', OLD.file_attribute_name, ',', OLD.file_attribute_value, ',', OLD.file_id) );
//
DELIMITER ;

