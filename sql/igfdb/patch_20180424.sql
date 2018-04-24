ALTER TABLE run ADD UNIQUE INDEX(`experiment_id`,`seqrun_id`,`lane_number`);
ALTER TABLE run DROP INDEX `experiment_id`;