/**
 * This file contains functions for managing partitions.
 * Creating, removing, and displaying.
 */

package main

import (
	"github.com/imdario/mergo"
	"gopkg.in/guregu/null.v2"
)

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
func (db DB) UndoPartition(p *Partition, opts ...map[string]interface{}) {
	// Pull basic arguments
	m := map[string]interface{}{"table": p.Table}
	// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
	if len(opts) > 0 {
		if err := mergo.Merge(&m, opts[0]); err != nil {
			l.Error(err)
		}
	}
	// Pull custom function arguments if set in configuration
	if err := mergo.Merge(&m, p.Options.Functions.UndoPartition); err != nil {
		l.Error(err)
	}
	// Defaults (https://github.com/keithf4/pg_partman/blob/master/sql/functions/undo_partition.sql#L5)
	if err := mergo.Merge(&m, map[string]interface{}{"batchCount": 1, "keepTable": true, "jobmon": true, "lockWait": 0}); err != nil {
		l.Error(err)
	}

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

// Shows child partitions for a partition table.
func (db DB) GetChildPartitions(p *Partition) []Child {
	c := []Child{}
	err := db.Select(&c, "SELECT partman.show_partitions($1) AS table", p.Table)
	if err != nil {
		l.Error(err)
	} else {
		// Also get the record count and size on disk for each partition
		for i, child := range c {
			err := db.Get(&c[i].Records, "SELECT COUNT(*) FROM "+child.Table)
			if err != nil {
				l.Error(err)
			}
			// pg_size_pretty() will say "bytes" or "kB" etc.
			//err = db.Get(&bytesStr, "SELECT pg_size_pretty(pg_total_relation_size('"+child.Table+"'));")
			err = db.Get(&c[i].BytesOnDisk, "SELECT pg_total_relation_size('"+child.Table+"');")
			if err != nil {
				l.Error(err)
			}
		}
	}
	return c
}

// Sets a retention period on a partition
func (db DB) SetRetention(p *Partition, opts ...map[string]interface{}) {
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
		// Pull basic arguments (TODO: Maybe allow more to be set)
		m := map[string]interface{}{"table": p.Table, "retention": p.Retention, "retentionSchema": p.Options.RetentionSchema, "retentionKeepTable": p.Options.RetentionKeepTable}
		// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
		if len(opts) > 0 {
			if err := mergo.Merge(&m, opts[0]); err != nil {
				l.Error(err)
			}
		}
		// Pull custom function arguments if set in configuration
		if err := mergo.Merge(&m, p.Options.Functions.SetRetention); err != nil {
			l.Error(err)
		}
		// Defaults are actually going to come from the existing record in this case
		pc := PartConfig{}
		err := db.Select(&pc, "SELECT * FROM partman.part_config WHERE parent_table = $1", p.Table)
		if err != nil {
			l.Error(err)
		}
		if err := mergo.Merge(&m, map[string]interface{}{"retention_schema": pc.RetentionSchema, "retention_keep_table": pc.RetentionKeepTable}); err != nil {
			l.Error(err)
		}

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
		m := map[string]interface{}{"table": p.Table, "retention": null.String{}, "retentionSchema": null.String{}, "retentionKeepTable": true}
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
func (db DB) PartitionDataTime(p *Partition, opts ...map[string]interface{}) {
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		// Pull basic arguments
		m := map[string]interface{}{"table": p.Table}
		// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
		if len(opts) > 0 {
			if err := mergo.Merge(&m, opts[0]); err != nil {
				l.Error(err)
			}
		}
		// Pull custom function arguments if set in configuration
		if err := mergo.Merge(&m, p.Options.Functions.PartitionDataTime); err != nil {
			l.Error(err)
		}
		// Defaults (https://github.com/keithf4/pg_partman/blob/master/sql/functions/partition_data_time.sql#L4)
		if err := mergo.Merge(&m, map[string]interface{}{"batchCount": 1, "batchInterval": null.String{}, "lockWait": 0, "order": "ASC"}); err != nil {
			l.Error(err)
		}

		_, err = db.NamedExec(`SELECT partman.partition_data_time(:table, :batchCount, :batchInterval, :lockWait, :order);`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("The partition on " + p.Table + " has been cleaned up. Any data written to the parent has now been moved to child partition tables (if they were available).")
		}
	} else {
		l.Info("There appears to be no partition set for " + p.Table + ".")
	}
}

// For id based partitions, this fixes/cleans up partitions which may have accidentally had data written to the parent table. Or, maybe it was data before the partition was created.
func (db DB) PartitionDataId(p *Partition, opts ...map[string]interface{}) {
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		// Pull basic arguments
		m := map[string]interface{}{"table": p.Table}
		// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
		if len(opts) > 0 {
			if err := mergo.Merge(&m, opts[0]); err != nil {
				l.Error(err)
			}
		}
		// Pull custom function arguments if set in configuration
		if err := mergo.Merge(&m, p.Options.Functions.PartitionDataId); err != nil {
			l.Error(err)
		}
		// Defaults (https://github.com/keithf4/pg_partman/blob/master/sql/functions/partition_data_id.sql#L4)
		if err := mergo.Merge(&m, map[string]interface{}{"batchCount": 1, "batchInterval": null.String{}, "lockWait": 0, "order": "ASC"}); err != nil {
			l.Error(err)
		}

		_, err = db.NamedExec(`SELECT partman.partition_data_id(:table, :batchCount, :batchInterval, :lockWait, :order);`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("The partition on " + p.Table + " has been cleaned up. Any data written to the parent has now been moved to child partition tables (if they were available).")
		}
	} else {
		l.Info("There appears to be no partition set for " + p.Table + ".")
	}
}

// Manually uninherits (and optionally drops) child partition tables from a time based partition set.
func (db DB) DropPartitionTime(p *Partition, opts ...map[string]interface{}) {
	//drop_partition_time(p_parent_table text, p_retention interval DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
	//This function is used to drop child tables from a time-based partition set. By default, the table is just uninherited and not actually dropped. For automatically dropping old tables, it is recommended to use the run_maintenance() function with retention configured instead of calling this directly.
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		// Pull basic arguments
		m := map[string]interface{}{"table": p.Table}
		// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
		if len(opts) > 0 {
			if err := mergo.Merge(&m, opts[0]); err != nil {
				l.Error(err)
			}
		}
		// Pull custom function arguments if set in configuration
		if err := mergo.Merge(&m, p.Options.Functions.DropPartitionTime); err != nil {
			l.Error(err)
		}
		// Defaults (https://github.com/keithf4/pg_partman/blob/master/sql/functions/drop_partition_time.sql#L5)
		if err := mergo.Merge(&m, map[string]interface{}{"retention": null.String{}, "keepTable": null.String{}, "keepIndex": null.String{}, "retentionSchema": null.String{}}); err != nil {
			l.Error(err)
		}

		_, err = db.NamedExec(`SELECT partman.drop_partition_time(:table, :retention, :keepTable, :keepIndex, :retentionSchema);`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("The partition on " + p.Table + " has been dropped.")
		}
	} else {
		l.Info("There appears to be no partition set for " + p.Table + ".")
	}
}

// Manually uninherits (and optionally drops) a child partition table from an id based partition set.
func (db DB) DropPartitionId(p *Partition, opts ...map[string]interface{}) {
	//drop_partition_id(p_parent_table text, p_retention bigint DEFAULT NULL, p_keep_table boolean DEFAULT NULL, p_keep_index boolean DEFAULT NULL, p_retention_schema text DEFAULT NULL) RETURNS int
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", p.Table)
	if err != nil {
		l.Error(err)
	}
	// Make sure it exists.
	if count > 0 {
		// Pull basic arguments
		m := map[string]interface{}{"table": p.Table}
		// Pull overrides passed to this function (won't come from standalone gopartman, but could from any other package which may use it)
		if len(opts) > 0 {
			if err := mergo.Merge(&m, opts[0]); err != nil {
				l.Error(err)
			}
		}
		// Pull custom function arguments if set in configuration
		if err := mergo.Merge(&m, p.Options.Functions.DropPartitionTime); err != nil {
			l.Error(err)
		}
		// Defaults (https://github.com/keithf4/pg_partman/blob/master/sql/functions/drop_partition_id.sql#L5)
		if err := mergo.Merge(&m, map[string]interface{}{"retention": null.String{}, "keepTable": null.String{}, "keepIndex": null.String{}, "retentionSchema": null.String{}}); err != nil {
			l.Error(err)
		}

		_, err = db.NamedExec(`SELECT partman.drop_partition_time(:table, :retention, :keepTable, :keepIndex, :retentionSchema);`, m)
		if err != nil {
			l.Error(err)
		} else {
			l.Info("The partition on " + p.Table + " has been dropped.")
		}
	} else {
		l.Info("There appears to be no partition set for " + p.Table + ".")
	}
}
