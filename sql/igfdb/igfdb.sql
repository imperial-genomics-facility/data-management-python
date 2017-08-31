-- -----------------------------------------------------
-- Schema igfdb
-- -----------------------------------------------------
USE `igfdb` ;


/* REQUIRE GLOBAL log_bin_trust_function_creators AS TRUE
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
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('MODIFIED', 'RUN', CONCAT('OLD:', OLD.run_id, ',', OLD.igf_id, ',', OLD.flowcell_id, ',', OLD.experiment_id, ',', OLD.status, ',', OLD.lane_number, ', NEW:', NEW.run_id, ',', NEW.igf_id, ',', NEW.flowcell_id, ',', NEW.experiment_id, ',', NEW.status, ',', NEW.lane_number  ) );
//
DELIMITER ;

DELIMITER //
DROP TRIGGER IF EXISTS run_delete_log //
CREATE TRIGGER run_delete_log AFTER DELETE ON run
FOR EACH ROW
  INSERT INTO history (`log_type`, `table_name`, `message`) value ('DELETED', 'RUN', CONCAT('OLD:', OLD.run_id, ',', OLD.igf_id, ',', OLD.flowcell_id, ',', OLD.experiment_id, ',', OLD.status, ',', OLD.lane_number ) );
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

*/
