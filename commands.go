package main

import (
	"fmt"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gopartman",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println(color.YellowString("gopartman v" + ver))
	},
}

// Installs the partman schema and its objects.
var installPartmanCmd = &cobra.Command{
	Use:   "install",
	Short: "Installs pg_partman",
	Long:  "\nInstalls pg_partman into a `partman` schema with its objects to manage partitions\n(Note: This is automatically installed, if not installed, when creating a partition).",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			log.Info("Installing pg_partman on " + flags.pgDatabase)
			flaggedDB.loadPgPartman()
		} else {
			log.Info("pg_partman has already been installed on " + flags.pgDatabase)
		}
	},
}

// Reinstalls the partman schema and its objects by first dropping the `partman` schema and then installing again.
var reinstallPartmanCmd = &cobra.Command{
	Use:   "reinstall",
	Short: "Re-installs pg_partman",
	Long:  "\nNote that re-installing pg_partman will drop the `partman` schema and all objects.\nSo any existing partitions on the database will cease to be managed.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		log.Info("Re-installing pg_partman on " + flags.pgDatabase)
		flaggedDB.unloadPartman()
		flaggedDB.loadPgPartman()
	},
}

// Create a partition.
var createParentCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a partition",
	Long:  "\nCreates a partition for a given table. You must also pass a partition type and any other applicable options.\n(Note: This partition will require manual maintenance if the type is not `id-static` or `id-dynamic`)",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			flaggedDB.loadPgPartman()
		}

		log.Info("Creating a partition on " + flags.pgDatabase + " for table " + flags.partTable)
		flaggedDB.CreateParent("flagged")
	},
}

// Runs maintenance on partitions.
var runMaintenanceCmd = &cobra.Command{
	Use:   "maintenance",
	Short: "Runs maintenance on partitions",
	Long:  "\nRuns maintenance on all tables if no table name was given. Maintenance includes adding new partition tables and removing old ones if a retention policy was set.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			log.Error("Error: pg_partman not installed. Please run the `install` command first.")
			return
		}

		if flags.partTable != "" {
			log.Info("Running maintenance on " + flags.pgDatabase + " for table " + flags.partTable)
			flaggedDB.RunMaintenance("flagged", flags.analyze, flags.jobmon)
		} else {
			log.Info("Running maintenance on " + flags.pgDatabase + " for all tables")
			flaggedDB.RunMaintenance("NULL", flags.analyze, flags.jobmon)
		}
	},
}

// Undo a partition.
var undoPartitionCmd = &cobra.Command{
	Use:   "undo",
	Short: "Undo a partition",
	Long:  "\nReverts a partition back to only using its parent table.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			flaggedDB.loadPgPartman()
		}

		log.Info("Reverting a partition on " + flags.pgDatabase + " for table " + flags.partTable)
		flaggedDB.UndoPartition("flagged", flags.batchCount, flags.dropTable, flags.jobmon, flags.lockWaitTime)
	},
}

// Get information about a partition for a given table.
var getPartitionInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Info about a partition",
	Long:  "\nDisplays information about a partition given its parent table.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		info := flaggedDB.PartitionInfo("flagged")

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Table", "Control Column", "Type", "Interval", "# of Tables to Premake"})
		table.Append([]string{info.ParentTable, info.Control, info.Type, info.PartInterval, strconv.Itoa(info.Premake)})
		table.Render()
	},
}
