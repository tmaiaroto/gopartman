/**
 * This file contains functions for managing partitions.
 * Creating, removing, and displaying.
 */

package main

// Creates a parent from a given table and creatse partitions based on the given settings.
func (db DB) CreateParent(partitionName string) {
	// SELECT partman.create_parent('test.part_test', 'col3', 'time-static', 'daily');
	_, err := db.NamedExec(`SELECT partman.create_parent(:table, :column, :type, :interval);`, db.Partitions[partitionName])
	if err != nil {
		log.Error("%v", err)
	}
}

// Creates parents from all configured partitions for a database.
func (db DB) CreateParents() {
	for partitionName, _ := range db.Partitions {
		db.CreateParent(partitionName)
	}
}

// Calls the `run_maintenance()` function and adds new partition tables and drops old partitions if a retention period was set. If a table name is passed, it will run maintenance for that table ONLY. "NULL" will run maintenance on all tables.
func (db DB) RunMaintenance(table string) {
	_, err := db.NamedExec(`SELECT partman.run_maintenance(:table);`, struct{ Table string }{Table: table})
	if err != nil {
		log.Error("%v", err)
	}
}
