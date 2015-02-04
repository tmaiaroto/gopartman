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
			l.Info("Installing pg_partman on " + flags.pgDatabase)
			flaggedDB.loadPgPartman()
		} else {
			l.Info("pg_partman has already been installed on " + flags.pgDatabase)
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
		l.Info("Re-installing pg_partman on " + flags.pgDatabase)
		flaggedDB.unloadPartman()
		flaggedDB.loadPgPartman()
	},
}

// Create a partition.
var createParentCmd = &cobra.Command{
	Use:   "create",
	Short: "Creates a partition",
	Long: `Creates a partition for a given table based on the options passed.
	Partitions must be maintained with the ` + "\x1b[33m\x1b[40m" + `maintenance` + "\x1b[0m\x1b[0m" + ` command (optional for types marked with *) 
	which will create new child partition tables as well as update any necessary triggeres. Note: Only newly inserted data will appear in the
	new partition tables. Existing data will remain in the parent table. Any data inserted for which a partition table does not exist will 
	be stored in the original parent table.

	Partition types can be one of the following:
	
	time-static   - Trigger function inserts only into specifically named partitions. The number of partitions
	                managed behind and ahead of the current one is determined by the **premake** config value 
	                (default of 4 means data for 4 previous and 4 future partitions are handled automatically).
	                *Beware setting the premake value too high as that will lessen the efficiency of this 
	                partitioning method.*
	                Inserts to parent table outside the hard-coded time window will go to the parent.
	                Ideal for high TPS tables that get inserts of new data only.

	time-dynamic  - Trigger function can insert into any existing child partition based on the value of the control 
	                column at the time of insertion.
	                More flexible but not as efficient as time-static.

	time-custom   - Allows use of any time interval instead of the premade ones below. Note this uses the same 
	                method as time-dynamic (so it can insert into any child at any time) as well as a lookup table.
	                So, while it is the most flexible of the time-based options, it is the least performant.

	id-static *   - Same functionality and use of the premake value as time-static but for a numeric range 
	                instead of time.
	                By default, when the id value reaches 50% of the max value for that partition, it will automatically create 
	                the next partition in sequence if it doesn't yet exist.
	                Only supports id values greater than or equal to zero.

	id-dynamic *  - Same functionality and limitations as time-dynamic but for a numeric range instead of time.
	                Uses same 50% rule as id-static to create future partitions or can use maintenance if desired.
	                Only supports id values greater than or equal to zero.
	
	The partition time interval must be set for certain partition types. Some valid values include:

	yearly          - One partition per year
	quarterly       - One partition per yearly quarter. Partitions are named as YYYYqQ (ex: 2012q4)
	monthly         - One partition per month
	weekly          - One partition per week. Follows ISO week date format (http://en.wikipedia.org/wiki/ISO_week_date). 
	                  Partitions are named as IYYYwIW (ex: 2012w36)
	daily           - One partition per day
	hourly          - One partition per hour
	half-hour       - One partition per 30 minute interval on the half-hour (1200, 1230)
	quarter-hour    - One partition per 15 minute interval on the quarter-hour (1200, 1215, 1230, 1245)
	<interval>      - For the time-custom partitioning type, this can be any interval value that is valid for the 
	                  PostgreSQL interval data type. Do not type cast the parameter value, just leave as text.
	<integer>       - For ID based partitions, the integer value range of the ID that should be set per partition. 
	                  Enter this as an integer in text format ('100' not 100). Must be greater than one.

	Example: ./gopartman create -h localhost -u username -d myblogdb -t public.posts -c created -y time-static -i monthly

	That call would create a daily partition on a posts table with 4 days before and 4 days ahead of the current date. 
	Of course, more tables would need to be created for the future. 
	So one would then run: ./gopartman maintenance -s localhost -u username -d myblogdb -t public.posts

	That call would then create additional tables if necessary. This maintenance call should be executed on a regular basis.
	In the case of this example, monthly maintenance calls would work because there are 4 months created in advance.

	Any new data will now be stored in an available partition (if one does not exist, it will be stored in the parent table).
	`,
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			flaggedDB.loadPgPartman()
		}

		l.Info("Creating a partition on " + flags.pgDatabase + " for table " + flags.partTable)
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
			l.Error("Error: pg_partman not installed. Please run the `install` command first.")
			return
		}

		if flags.partTable != "" {
			l.Info("Running maintenance on " + flags.pgDatabase + " for table " + flags.partTable)
			flaggedDB.RunMaintenance("flagged", flags.analyze, flags.jobmon)
		} else {
			l.Info("Running maintenance on " + flags.pgDatabase + " for all tables")
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

		l.Info("Reverting a partition on " + flags.pgDatabase + " for table " + flags.partTable)
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

// Set a retention period for a partition.
var setPartitionRetentionCmd = &cobra.Command{
	Use:   "set-retention",
	Short: "Set a partition retention period",
	Long:  "\nSets a retention period for a partition. Maintenance will now remove old child partition tables and the data within them.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			flaggedDB.loadPgPartman()
		}

		flaggedDB.SetRetention("flagged")
	},
}

// Removes a retention period for a partition.
var removePartitionRetentionCmd = &cobra.Command{
	Use:   "remove-retention",
	Short: "Remove a partition retention period",
	Long:  "\nRemoves a retention period for a partition. Maintenance will create new partition tables, but no tables or data will be removed.",
	Run: func(cmd *cobra.Command, args []string) {
		flaggedDB := NewFlaggedDb()
		defer flaggedDB.Close()
		if !flaggedDB.sqlFunctionsExist() {
			flaggedDB.loadPgPartman()
		}

		flaggedDB.RemoveRetention("flagged")
	},
}
