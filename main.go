package main

import (
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
	"github.com/tmaiaroto/cron"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

// Version of gopartman
const ver = "0.1.0"

var GoPartManCmd = &cobra.Command{
	Use:   "gopartman",
	Short: "A Postgres Partition Manager",
	//Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

type GoPartManFlags struct {
	verbose       bool
	daemon        bool
	pgHost        string
	pgUser        string
	pgPassword    string
	pgPort        string
	pgDatabase    string
	partTable     string
	partColumn    string
	partType      string
	partInterval  string
	partRetention string
	analyze       bool
	lockWaitTime  int
	batchCount    int
	dropTable     bool
	jobmon        bool
}

var flags = GoPartManFlags{}

// Logging the pretty way.
var log = logging.MustGetLogger("gopartmanLogger")
var logFormat = logging.MustStringFormatter(
	"%{color}[%{time:Jan/02/2006:15:04:05 -0700}] %{shortfile} %{level:.4s} %{id:03x}%{color:reset} %{message}",
)

// Never log out passwords from configuration.
type Password string

func (p Password) Redacted() interface{} {
	return logging.Redact(string(p))
}

// The configuration holds everything necessary to manage partitions. Connection information, what to partition, and when.
var cfg = GoPartManConfig{}

type GoPartManConfig struct {
	Cron        *cron.Cron
	Servers     map[string]Server `json:"servers" yaml:"servers"`
	Connections map[string]DB
}

type Partition struct {
	Table     string `json:"table" yaml:"table"`
	Column    string `json:"column" yaml:"column"`
	Type      string `json:"type" yaml:"type"`
	Interval  string `json:"interval" yaml:"interval"`
	Retention string `json:"retention" yaml:"retention"`
}

type Server struct {
	Database   string               `json:"database" yaml:"database"`
	Host       string               `json:"host" yaml:"host"`
	Port       string               `json:"port" yaml:"port"`
	User       string               `json:"user" yaml:"user"`
	Password   Password             `json:"password" yaml:"password"`
	Partitions map[string]Partition `json:"paritions" yaml:"partitions"`
}

// A struct for records in the `partman.part_config` table.
type PartConfig struct {
	ConstraintCols     string `json:"constraint_cols yaml:"constraint_cols" db:"constraint_cols"`
	Control            string `json:"control yaml:"control" db:"control"`
	DatetimeString     string `json:"datetime_string" yaml:"datetime_string" db:"datetime_string"`
	InheritFk          bool   `json:"inherit_fk" yaml:"inherit_fk" db:"inherit_fk"`
	Jobmon             bool   `json:"jobmon" yaml:"jobmon" db:"jobmon"`
	ParentTable        string `json:"parent_table" yaml:"parent_table" db:"parent_table"`
	PartInterval       string `json:"part_interval" yaml:"part_interval" db:"part_interval"`
	Premake            int    `json:"premake" yaml:"premake" db:"premake"`
	Retention          string `json:"retention" yaml:"retention" db:"retention"`
	RetentionKeepIndex bool   `json:"retention_keep_index" yaml:"retention_keep_index" db:"retention_keep_index"`
	RetentionKeepTable bool   `json:"retention_keep_table" yaml:"retention_keep_table" db:"retention_keep_table"`
	RetentionSchema    string `json:"retention_schema" yaml:"retention_schema" db:"retention_schema"`
	Type               string `json:"type" yaml:"type" db:"type"`
	UndoInProgress     bool   `json:"undo_in_progress" yaml:"undo_in_progress" db:"undo_in_progress"`
	UseRunMaintenance  bool   `json:"use_run_maintenance" yaml:"use_run_maintenance" db:"use_run_maintenance"`
}

// Wrap sqlx.DB in order to add to it
type DB struct {
	sqlx.DB
	Partitions map[string]Partition
}

// Set up the schedule.
func newSchedule() {
	c := cron.New()

	//c.AddFunc("@hourly", func() { log.Info("Every hour") })
	//c.AddFunc("0 5 * * * *", func() { log.Info("Every 5 minutes") }, "Optional name here. Useful when inspecting.")

	c.Start()
	cfg.Cron = c
}

func AddToSchedule() {

}

func ListSchedule() {
	for _, item := range cfg.Cron.Entries() {
		log.Debug("%v", item.Name)
		log.Debug("%v", item.Next)
	}
}

func NewPostgresConnection(cfg Server) (DB, error) {
	db, err := sqlx.Connect("postgres", "host="+cfg.Host+" port="+cfg.Port+" sslmode=disable  dbname="+cfg.Database+" user="+cfg.User+" password="+string(cfg.Password))
	if err != nil {
		log.Error("%v", err)
	}
	return DB{*db, cfg.Partitions}, err
}

// Retruns a connection to a database using information from flags (CLI only).
func NewFlaggedDb() DB {
	var flaggedDB DB
	var err error
	// Setup a new Postgres connection using information from flags (if present)
	if flags.pgUser != "" && flags.pgDatabase != "" {
		flaggedDB, err = NewPostgresConnection(Server{
			Database: flags.pgDatabase,
			Host:     flags.pgHost,
			Port:     flags.pgPort,
			User:     flags.pgUser,
			Password: Password(flags.pgPassword),
			Partitions: map[string]Partition{
				"flagged": Partition{
					Table:     flags.partTable,
					Column:    flags.partColumn,
					Type:      flags.partType,
					Interval:  flags.partInterval,
					Retention: flags.partRetention,
				},
			},
		})
		// Of course close the connection when we're done in thise case
		//defer flaggedDB.Close()
		if err != nil {
			log.Critical("There was a problem connecting to the Postgres database using the provided information.")
			panic(err)
		}
	}
	return flaggedDB
}

func main() {
	GoPartManCmd.AddCommand(versionCmd)
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.daemon, "daemon", "m", false, "daemon mode")
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.verbose, "verbose", "v", false, "verbose output")

	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgHost, "host", "s", "localhost", "Database host")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgPort, "port", "o", "5432", "Database port")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgUser, "user", "u", "", "Database user")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgPassword, "password", "p", "", "Database password")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgDatabase, "database", "d", "", "Database")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partTable, "table", "t", "", "Parent table of the partition set.")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partColumn, "column", "c", "created", "Partition column")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partType, "type", "y", "time", `Type of partitioning. Valid values are "time" and "id". Not setting this argument will use undo_partition() and work on any parent/child table set.`)
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partInterval, "interval", "i", "", "Partition interval")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partRetention, "retention", "r", "", "Partition retention period")

	GoPartManCmd.PersistentFlags().BoolVarP(&flags.analyze, "analyze", "a", true, "Analyze is run on the parent to ensure statistics are updated for constraint exclusion.")
	GoPartManCmd.PersistentFlags().IntVarP(&flags.lockWaitTime, "lockwait", "l", 0, "Have a lock timeout of this many seconds on the data move. If a lock is not obtained, that batch will be tried again.")
	GoPartManCmd.PersistentFlags().IntVarP(&flags.batchCount, "batch", "b", 1, "How many times to loop through the value given for --interval. If --interval not set, will use default partition interval and undo at most -b partition(s).  Script commits at the end of each individual batch. (NOT passed as p_batch_count to undo function). If not set, all data will be moved to the parent table in a single run of the script.")
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.dropTable, "droptable", "x", false, "Switch setting for whether to drop child tables when they are empty. Do not set to just uninherit.")
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.jobmon, "jobmon", "j", true, "Use pg_jobmon")

	GoPartManCmd.AddCommand(installPartmanCmd)
	GoPartManCmd.AddCommand(reinstallPartmanCmd)
	GoPartManCmd.AddCommand(createParentCmd)
	GoPartManCmd.AddCommand(runMaintenanceCmd)
	GoPartManCmd.AddCommand(undoPartitionCmd)
	GoPartManCmd.AddCommand(getPartitionInfoCmd)

	GoPartManCmd.Execute()

	logBackend := logging.NewLogBackend(os.Stderr, "", 0)
	logBackendFormatter := logging.NewBackendFormatter(logBackend, logFormat)
	logBackendLeveled := logging.AddModuleLevel(logBackendFormatter)
	// Critical messages will always output
	logBackendLeveled.SetLevel(logging.CRITICAL, "")
	// Verbose flag will show all log messages
	if flags.verbose {
		logBackendLeveled.SetLevel(logging.DEBUG, "")
	}
	logging.SetBackend(logBackendLeveled)

	// Config based usage
	cfgPath := "/etc/gopartman.yml"
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = "./gopartman.yml"
	}
	b, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		log.Critical("Configuration could not be loaded.")
		panic(err)
	}

	err = yaml.Unmarshal(b, &cfg)
	if err != nil {
		log.Critical("error: %v", err)
		panic(err)
	}

	//log.Info("%v", cfg.Servers)

	// Set up all of the connections from the configuration and ensure they have the pg_partman schema, table, and functions loaded.
	cfg.Connections = map[string]DB{}
	for conn, credentials := range cfg.Servers {
		cfg.Connections[conn], _ = NewPostgresConnection(credentials)

		if !cfg.Connections[conn].sqlFunctionsExist() {
			cfg.Connections[conn].loadPgPartman()
		}
	}

	newSchedule()

}
