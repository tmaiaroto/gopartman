/**
 * This file contains functions for managing partitions.
 * Creating, removing, and displaying.
 */

package main

// Creates a parent from a given table and creatse partitions based on the given settings.
func (db DB) CreateParent(p *Partition) {
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	if count > 0 {
		l.Info("Partition already exists for " + p.Table + " you must first run `undo` on it.")
		return
	}

	// SELECT partman.create_parent('test.part_test', 'col3', 'time-static', 'daily');
	_, err = db.NamedExec(`SELECT partman.create_parent(:table, :column, :type, :interval);`, p)
	if err != nil {
		l.Error(err)
	}

	// If a retention period was set, the record in partman.part_config table must be updated to include it. It does not get set with create_parent()
	db.SetRetention(p)
}

// Creates parents from all configured partitions for a database.
func (db DB) CreateParents() {
	if len(db.Partitions) == 0 {
		l.Info("There are no configured partitions to be created.")
	} else {
		for _, p := range db.Partitions {
			db.CreateParent(&p)
		}
	}
}

// Calls the `run_maintenance()` function and adds new partition tables and drops old partitions if a retention period was set. If a partition name is passed, it will run maintenance for that partition table ONLY. "NULL" will run maintenance on all tables.
func (db DB) RunMaintenance(partitionName string, analyze bool, jobmon bool) {
	_, err := db.NamedExec(`SELECT partman.run_maintenance(:table, :analyze, :jobmon);`, struct {
		Table   string
		Analyze bool
		Jobmon  bool
	}{
		Table:   db.Partitions[partitionName].Table,
		Analyze: analyze,
		Jobmon:  jobmon,
	})
	if err != nil {
		l.Error(err)
	}
}

// Undo any partition by copying data from the child partition tables to the parent. Note: Batches can not be smaller than the partition interval because this copies entire tables.
func (db DB) UndoPartition(p *Partition) {
	// These get reversed a bit in the phrasing
	keepTable := true
	if p.Options.DropTableOnUndo {
		keepTable = false
	}
	m := map[string]interface{}{"table": p.Table, "batchCount": p.Options.BatchCount, "keepTable": keepTable, "jobmon": p.Options.Jobmon, "lockWait": p.Options.LockWait}
	_, err := db.NamedExec(`SELECT partman.undo_partition(:table, :batchCount, :keepTable, :jobmon, :lockWait);`, m)
	if err != nil {
		l.Error(err)
	}

	// undo_partition() doesn't seem to remove the part_config record. It seems as if it should be removed too because a new partition on the same table can't be made until it is.
	_, err = db.NamedExec(`DELETE FROM partman.part_config WHERE parent_table = :table;`, m)
	if err != nil {
		l.Error(err)
	}

}

// Gets information about a partition.
func (db DB) PartitionInfo(p *Partition) PartConfig {
	pc := PartConfig{}
	err := db.Get(&pc, "SELECT parent_table,control,type,part_interval,premake FROM partman.part_config WHERE parent_table = $1 LIMIT 1", p.Table)
	if err != nil {
		l.Error(err)
	}
	return pc
}

// Sets a retention period on a partition
func (db DB) SetRetention(p *Partition) {
	if p.Retention == "" {
		l.Info("No retention period configured.")
		return
	}
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		retentionSchema := "NULL"
		if p.Options.RetentionSchema != "" {
			retentionSchema = p.Options.RetentionSchema
		}
		// By default this is true, but our struct becomes false if not set. So we need to flip it around in order to keep the default behavior of pg_partman.
		retentionKeepTable := true
		if p.Options.RetentionRemoveTable {
			retentionKeepTable = false
		}
		m := map[string]interface{}{"table": p.Table, "retention": p.Retention, "retentionSchema": retentionSchema, "retentionKeepTable": retentionKeepTable}
		_, err = db.NamedExec(`UPDATE partman.part_config SET retention = :retention, retention_schema = :retentionSchema, retention_keep_table = :retentionKeepTable WHERE parent_table = :table;`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("A retention period has been set for " + p.Table + ". Maintenance will remove old child partition tables.")
		}
	}
}

// Removes retention on a partition. Maintenance will no longer remove old child partition tables.
func (db DB) RemoveRetention(p *Partition) {
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		m := map[string]interface{}{"table": p.Table, "retention": "NULL", "retentionSchema": "NULL", "retentionKeepTable": true}
		_, err = db.NamedExec(`UPDATE partman.part_config SET retention = :retention, retention_schema = :retentionSchema, retention_keep_table = :retentionKeepTable WHERE parent_table = :table;`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("The retention period has been removed for " + p.Table + ".")
		}
	} else {
		l.Info("There was no retention period set for " + p.Table + ".")
	}
}

// For time based partitions, this fixes/cleans up partitions which may have accidentally had data written to the parent table. Or, maybe it was data before the partition was created.
func (db DB) PartitionDataTime(p *Partition) {
	//partition_data_time(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval interval DEFAULT NULL, p_lock_wait numeric DEFAULT 0, p_order text DEFAULT 'ASC')
}

// For id based partitions, this fixes/cleans up partitions which may have accidentally had data written to the parent table. Or, maybe it was data before the partition was created.
func (db DB) PartitionDataId(p *Partition) {
	//partition_data_id(p_parent_table text, p_batch_count int DEFAULT 1, p_batch_interval int DEFAULT NULL, p_lock_wait numeric DEFAULT 0)
}

// Manually uninherits (and optionally drops) a child partition table from a time based partition set.
func (db DB) DropPartitionTime(p *Partition) {
	//drop_partition_time(p_parent_table text, p_retention interval DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
	//This function is used to drop child tables from a time-based partition set. By default, the table is just uninherited and not actually dropped. For automatically dropping old tables, it is recommended to use the run_maintenance() function with retention configured instead of calling this directly.
}

// Manually uninherits (and optionally drops) a child partition table from an id based partition set.
func (db DB) DropPartitionId(p *Partition) {
	//drop_partition_id(p_parent_table text, p_retention bigint DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
}
