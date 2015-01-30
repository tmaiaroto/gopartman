package main

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
	"github.com/tmaiaroto/cron"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
)

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
	partRetention string
}

var flags = GoPartManFlags{}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of gopartman",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("gopartman v" + ver)
	},
}

var installPartmanCmd = &cobra.Command{
	Use:   "install",
	Short: "Installs pg_partman",
	Long:  "Installs pg_partman into a `partman` schema with its objects to manage partitions.",
	Run: func(cmd *cobra.Command, args []string) {
		if !flaggedDB.sqlFunctionsExist() {
			log.Info("Installing pg_partman on " + flags.pgDatabase)
			flaggedDB.loadPgPartman()
		} else {
			log.Info("pg_partman has already been installed on " + flags.pgDatabase)
		}
	},
}

var reinstallPartmanCmd = &cobra.Command{
	Use:   "reinstall",
	Short: "Re-installs pg_partman",
	Long:  "Note that re-installing pg_partman will drop the `partman` schema and all objects. So any existing partitions on the database will cease to be managed.",
	Run: func(cmd *cobra.Command, args []string) {
		log.Info("Re-installing pg_artman on " + flags.pgDatabase)
		flaggedDB.unloadPartman()
		flaggedDB.loadPgPartman()
	},
}

// Logging in a pretty way.
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
	Retention string `json:"retention" yaml:"retention"`
	Type      string `json:"type" yaml:"type"`
}

type Server struct {
	Database   string               `json:"database" yaml:"database"`
	Host       string               `json:"host" yaml:"host"`
	Port       string               `json:"port" yaml:"port"`
	User       string               `json:"user" yaml:"user"`
	Password   Password             `json:"password" yaml:"password"`
	Partitions map[string]Partition `json:"paritions" yaml:"partitions"`
}

// A server connection from the command line will only contain one partition (while it could take JSON, the partition types mean all sorts of different things so it's easier to do one at a time).
var flaggedDB DB

// Wrap sqlx.DB in order to add to it
type DB struct {
	sqlx.DB
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
	return DB{*db}, err
}

func main() {
	GoPartManCmd.AddCommand(versionCmd)
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.verbose, "verbose", "v", false, "verbose output")
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.daemon, "daemon", "m", false, "daemon mode")

	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgHost, "host", "a", "localhost", "host")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgPort, "port", "o", "5432", "port")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgUser, "user", "u", "", "user")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgPassword, "password", "p", "", "password")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.pgDatabase, "database", "d", "", "database")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partTable, "table", "t", "", "partition table")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partColumn, "column", "c", "created", "partition column")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partType, "type", "y", "time", "partition type")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partRetention, "retention", "r", "", "partition retention period")
	GoPartManCmd.Execute()

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
					Retention: flags.partRetention,
					Type:      flags.partType,
				},
			},
		})
		// Of course close the connection when we're done in thise case
		defer flaggedDB.Close()
		if err != nil {
			log.Critical("There was a problem connecting to the Postgres database using the provided information.")
			panic(err)
		}
	}

	// Add these commands AFTER the flaggedDB is created
	GoPartManCmd.AddCommand(installPartmanCmd)
	GoPartManCmd.AddCommand(reinstallPartmanCmd)
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

	log.Info("%v", cfg.Servers)

	// Set up all of the connections from the configuration and ensure they have the pg_partman schema, table, and functions loaded.
	cfg.Connections = map[string]DB{}
	for conn, credentials := range cfg.Servers {
		cfg.Connections[conn], _ = NewPostgresConnection(credentials)

		if !cfg.Connections[conn].sqlFunctionsExist() {
			cfg.Connections[conn].loadPgPartman()
		}
	}

	newSchedule()

	for {
	}
}
