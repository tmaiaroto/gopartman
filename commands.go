package main

import (
	"errors"
	"fmt"
	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"os"
	"strconv"
)

// Checks to see if the server and partition passed from the command line has actually been configured and returns it if so.
func getFlaggedPartition() (*DB, *Partition, error) {
	var err error
	if sVal, ok := cfg.Connections[flags.server]; ok {
		if pVal, ok := cfg.Connections[flags.server].Partitions[flags.partition]; ok {
			err = nil
			return &sVal, &pVal, err
		}
	}
	err = errors.New("that partition does not seem to be configured in gopartman.yml")
	return &DB{}, &Partition{}, err
}

// Gets just the database server connection passed from the command line
func getFlaggedServer() (*DB, error) {
	var err error
	if sVal, ok := cfg.Connections[flags.server]; ok {
		return &sVal, err
	}
	err = errors.New("that server does not seem to be configured in gopartman.yml")
	return &DB{}, err
}

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
		fServer, _, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			l.Info("Installing pg_partman on " + flags.server)
			fServer.loadPgPartman()
		} else {
			l.Info("pg_partman has already been installed on " + flags.server)
		}
	},
}

// Reinstalls the partman schema and its objects by first dropping the `partman` schema and then installing again.
var reinstallPartmanCmd = &cobra.Command{
	Use:   "reinstall",
	Short: "Re-installs pg_partman",
	Long:  "\nNote that re-installing pg_partman will drop the `partman` schema and all objects.\nSo any existing partitions on the database will cease to be managed.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, _, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		l.Info("Re-installing pg_partman on " + flags.server)
		fServer.unloadPartman()
		fServer.loadPgPartman()
	},
}

// Create a partition.
var createParentCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a partition",
	Long: "\n" + `Creates a partition for a given table based on the ` + "\x1b[33m\x1b[40m" + `gopartman.yml` + "\x1b[0m\x1b[0m" + ` configuration file.
	
	Example: ./gopartman create -c /some/other/gopartman.yml -p mypartition

	That call would create a daily partition on a posts table with 4 days before and 4 days ahead of the current date. 
	Of course, more tables would need to be created for the future. If gopartman is not running as a daemon, you will manually 
	need to call additional maintenance commands (on crontab perhaps).

	So one might then run: ./gopartman maintenance -c /some/other/gopartman.yml -p mypartition
	`,
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			fServer.loadPgPartman()
		}

		l.Info("Creating a partition on " + flags.server + " for table " + fPartition.Table + " (" + flags.partition + ")")
		fServer.CreateParent(fPartition)
	},
}

// Runs maintenance on partitions.
var runMaintenanceCmd = &cobra.Command{
	Use:   "maintenance",
	Short: "Runs maintenance on partitions",
	Long:  "\nRuns maintenance on all tables if no table name was given. Maintenance includes adding new partition tables and removing old ones if a retention policy was set.",
	Run: func(cmd *cobra.Command, args []string) {
		if len(flags.partition) == 0 && len(flags.server) > 0 {
			fServer, _ := getFlaggedServer()
			l.Info("Running maintenance on " + flags.server + " for all tables")
			fServer.RunMaintenance(&Partition{Table: ""})
		} else {
			fServer, fPartition, err := getFlaggedPartition()
			if err != nil {
				l.Critical(err)
				return
			}
			if !fServer.sqlFunctionsExist() {
				l.Error("Error: pg_partman not installed. Please run the `install` command first.")
				return
			}

			l.Info("Running maintenance on " + flags.server + " for table " + fPartition.Table)
			fServer.RunMaintenance(fPartition)
		}
	},
}

// Undo a partition.
var undoPartitionCmd = &cobra.Command{
	Use:   "undo",
	Short: "Undo a partition",
	Long:  "\nReverts a partition back to only using its parent table.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			fServer.loadPgPartman()
		}

		l.Info("Reverting a partition on " + flags.server + " for table " + flags.partition)
		fServer.UndoPartition(fPartition)
	},
}

// Get information about a partition for a given table.
var getPartitionInfoCmd = &cobra.Command{
	Use:   "info",
	Short: "Info about a partition",
	Long:  "\nDisplays information about a partition.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}

		info := fServer.PartitionInfo(fPartition)
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Table", "Control Column", "Type", "Interval", "# of Tables to Premake"})
		table.Append([]string{info.ParentTable, info.Control, info.Type, info.PartInterval, strconv.Itoa(info.Premake)})
		table.Render()
	},
}

// Get information about a partition's children.
var getPartitionChildrenCmd = &cobra.Command{
	Use:   "children",
	Short: "Child table info for a partition",
	Long:  "\nDisplays information about a partition's child tables.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}

		children := fServer.GetChildPartitions(fPartition)
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Table", "# of Records", "Size (bytes)"})
		for _, child := range children {
			table.Append([]string{child.Table, strconv.Itoa(child.Records), strconv.FormatUint(child.BytesOnDisk, 10)})
		}
		table.Render()
	},
}

// Shows number of records inserted into the parent tables instead of child partition tables.
var checkParentCmd = &cobra.Command{
	Use:   "check",
	Short: "Number of records left in parent tables",
	Long:  "\nDisplays number of records inserted into parent tables instead of child partition tables." + "\n" + `Records can be moved with the ` + "\x1b[33m\x1b[40m" + `fix` + "\x1b[0m\x1b[0m" + ` command if child partition tables exist.`,
	Run: func(cmd *cobra.Command, args []string) {
		fServer, _, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}

		parents := fServer.CheckParent()
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Parent Table", "# of Records"})
		for _, parent := range parents {
			table.Append([]string{parent.Table, strconv.Itoa(parent.Records)})

		}
		table.Render()
	},
}

// Set a retention period for a partition.
var setPartitionRetentionCmd = &cobra.Command{
	Use:   "set-retention",
	Short: "Set a partition retention period",
	Long:  "\nSets a retention period for a partition. Maintenance will now remove old child partition tables and the data within them.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			fServer.loadPgPartman()
		}

		fServer.SetRetention(fPartition)
	},
}

// Removes a retention period for a partition.
var removePartitionRetentionCmd = &cobra.Command{
	Use:   "remove-retention",
	Short: "Remove a partition retention period",
	Long:  "\nRemoves a retention period for a partition. Maintenance will create new partition tables, but no tables or data will be removed.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			fServer.loadPgPartman()
		}

		fServer.RemoveRetention(fPartition)
	},
}

// Cleans up a partition, moving any records from the parent table into child partition tables where possible.
var fixPartitionCmd = &cobra.Command{
	Use:   "fix",
	Short: "Fix and clean up parent table",
	Long:  "\nMoves data that accidentally gets inserted into the parent (or existing data before partitioning) into the proper child partition tables if available.",
	Run: func(cmd *cobra.Command, args []string) {
		fServer, fPartition, err := getFlaggedPartition()
		if err != nil {
			l.Critical(err)
			return
		}
		if !fServer.sqlFunctionsExist() {
			fServer.loadPgPartman()
		}

		pi := fServer.PartitionInfo(fPartition)
		switch pi.Type {
		case "time-dynamic", "time-static", "time-custom":
			fServer.PartitionDataTime(fPartition)
		case "id-dynamic", "id-static":
			fServer.PartitionDataId(fPartition)
		default:
			l.Critical("The partition does not seem to have a proper type.")
		}
	},
}
