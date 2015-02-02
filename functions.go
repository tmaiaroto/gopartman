/**
 * This file contains functions for managing partitions.
 * Creating, removing, and displaying.
 */

package main

// Creates a parent from a given table and creatse partitions based on the given settings.
func (db DB) CreateParent(partitionName string) {
	var count int
	err := db.Get(&count, "SELECT COUNT(*) FROM partman.part_config WHERE parent_table = $1", db.Partitions[partitionName].Table)
	if err != nil {
		log.Error("%v", err)
	}
	if count > 0 {
		log.Info("Partition already exists for " + db.Partitions[partitionName].Table + " you must first run `undo` on it.")
		return
	}

	// SELECT partman.create_parent('test.part_test', 'col3', 'time-static', 'daily');
	_, err = db.NamedExec(`SELECT partman.create_parent(:table, :column, :type, :interval);`, db.Partitions[partitionName])
	if err != nil {
		log.Error("%v", err)
	}
}

// Creates parents from all configured partitions for a database.
func (db DB) CreateParents() {
	if len(db.Partitions) == 0 {
		log.Info("There are no configured partitions to be created.")
	}
	for partitionName, _ := range db.Partitions {
		db.CreateParent(partitionName)
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
		log.Error("%v", err)
	}
}

// Undo any partition by copying data from the child partition tables to the parent. Note: Batches can not be smaller than the partition interval because this copies entire tables.
func (db DB) UndoPartition(partitionName string, batchCount int, dropTable bool, jobmon bool, lockWait int) {
	// These get reversed a bit in the phrasing
	keepTable := true
	if dropTable {
		keepTable = false
	}
	m := map[string]interface{}{"table": db.Partitions[partitionName].Table, "batchCount": batchCount, "keepTable": keepTable, "jobmon": jobmon, "lockWait": lockWait}
	_, err := db.NamedExec(`SELECT partman.undo_partition(:table, :batchCount, :keepTable, :jobmon, :lockWait);`, m)
	if err != nil {
		log.Error("%v", err)
	}

	// undo_partition() doesn't seem to remove the part_config record. It seems as if it should be removed too because a new partition on the same table can't be made until it is.
	_, err = db.NamedExec(`DELETE FROM partman.part_config WHERE parent_table = :table;`, m)
	if err != nil {
		log.Error("%v", err)
	}

}
