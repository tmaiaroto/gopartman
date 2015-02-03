package main

import (
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/tmaiaroto/cron"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"runtime"
	"strconv"
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

// The configuration holds everything necessary to manage partitions. Connection information, what to partition, and when.
var cfg = GoPartManConfig{}

type GoPartManConfig struct {
	Cron *cron.Cron
	Api  struct {
		Port int `json:"port" yaml:"port"`
		Cors struct {
			AllowedOrigins []string `json:"allowedOrigins" yaml:"allowedOrigins"`
		} `json:"cors" yaml:"cors"`
		AuthKeys []string `json:"authKeys" yaml:"authKeys"`
	} `json:"api" yaml:"api"`
	Servers     map[string]Server `json:"servers" yaml:"servers"`
	Connections map[string]DB
}

type Partition struct {
	Table     string `json:"table" yaml:"table"`
	Column    string `json:"column" yaml:"column"`
	Type      string `json:"type" yaml:"type"`
	Interval  string `json:"interval" yaml:"interval"`
	Retention string `json:"retention" yaml:"retention"`
	Options   struct {
		DropTable  bool `json:"dropTable" yaml:"dropTable"`
		LockWait   int  `json:"lockWait" yaml:"lockWait"`
		Analyze    bool `json:"analyze" yaml:"analyze"`
		BatchCount int  `json:"batchCount" yaml:"batchCount"`
	} `json:"options" yaml:"optoins"`
}

type Server struct {
	Database   string               `json:"database" yaml:"database"`
	Host       string               `json:"host" yaml:"host"`
	Port       string               `json:"port" yaml:"port"`
	User       string               `json:"user" yaml:"user"`
	Password   string               `json:"password" yaml:"password"`
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

// Logging (some functions always display output while others only if `verbose` was flagged)
type logging struct {
}

// Global logging; l.Info(), l.Error() etc.
var l = logging{}

func (l logging) Info(msg interface{}) {
	if flags.verbose {
		log.Println(msg)
	}
}
func (l logging) Debug(msg interface{}) {
	if flags.verbose {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println(msg)
	}
}
func (l logging) Critical(msg interface{}) {
	log.Println(color.RedString("%v", msg))
}
func (l logging) Error(msg interface{}) {
	log.Println(color.YellowString("%v", msg))
}

// Global job pool
var c *cron.Cron

// Set up the schedule.
func newSchedule() {
	c = cron.New()

	//c.AddFunc("@hourly", func() { l.Info("Every hour") })
	//c.AddFunc("0 5 * * * *", func() { l.Info("Every 5 minutes") }, "Optional name here. Useful when inspecting.")

	c.Start()
	cfg.Cron = c
}

func AddToSchedule() {

}

func ListSchedule() {
	for _, item := range cfg.Cron.Entries() {
		l.Debug(item.Name)
		l.Debug(item.Next)
	}
}

func NewPostgresConnection(cfg Server) (DB, error) {
	db, err := sqlx.Connect("postgres", "host="+cfg.Host+" port="+cfg.Port+" sslmode=disable  dbname="+cfg.Database+" user="+cfg.User+" password="+cfg.Password)
	if err != nil {
		l.Error(err)
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
			Password: flags.pgPassword,
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
			l.Critical("There was a problem connecting to the Postgres database using the provided information.")
			panic(err)
		}
	}
	return flaggedDB
}

// --------- API Basic Auth Middleware (valid keys are defined in the gopartman.yml config, there are no roles or anything like that)
type BasicAuthMw struct {
	Realm string
	Key   string
}

func (bamw *BasicAuthMw) MiddlewareFunc(handler rest.HandlerFunc) rest.HandlerFunc {
	return func(writer rest.ResponseWriter, request *rest.Request) {

		authHeader := request.Header.Get("Authorization")
		log.Println(authHeader)
		if authHeader == "" {
			queryParams := request.URL.Query()
			if len(queryParams["apiKey"]) > 0 {
				bamw.Key = queryParams["apiKey"][0]
			} else {
				bamw.unauthorized(writer)
				return
			}
		} else {
			bamw.Key = authHeader
		}

		keyFound := false
		for _, key := range cfg.Api.AuthKeys {
			if bamw.Key == key {
				keyFound = true
			}
		}

		if !keyFound {
			bamw.unauthorized(writer)
			return
		}

		handler(writer, request)
	}
}

// Response to handle an unauthorized, unauthenticated request
func (bamw *BasicAuthMw) unauthorized(writer rest.ResponseWriter) {
	writer.Header().Set("WWW-Authenticate", "Basic realm="+bamw.Realm)
	rest.Error(writer, "Not Authorized", http.StatusUnauthorized)
}

// Helper function to get the name of a function (primarily used to show scheduled tasks)
func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
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

	// Config based usage available if running forever as a daemon. In this mode, the following happens:
	// - Partitions to be managed are defined in gopartman.yml
	// - Maintenance is regularly performed so there's no need to set any commands to run in a crontab or anything like that
	// - An API can optionally be configured to allow:
	// 		- CORS and Basic Auth settings for access to the API (configured in gopartman.yml)
	// 		- Changes to configuration
	// 		- Addition of new partitions
	// 		- Reporting with information about partition settings and state
	if flags.daemon {
		cfgPath := "/etc/gopartman.yml"
		if _, err := os.Stat(cfgPath); err != nil {
			cfgPath = "./gopartman.yml"
		}
		b, err := ioutil.ReadFile(cfgPath)
		if err != nil {
			l.Critical("Configuration could not be loaded.")
			panic(err)
		}

		err = yaml.Unmarshal(b, &cfg)
		if err != nil {
			l.Critical(err)
			panic(err)
		}

		// Create a schedule for jobs
		newSchedule()

		// Set up all of the connections from the configuration and ensure they have the pg_partman schema, table, and functions loaded.
		cfg.Connections = map[string]DB{}
		for conn, credentials := range cfg.Servers {
			cfg.Connections[conn], err = NewPostgresConnection(credentials)
			if err == nil {
				// First make sure pg_partman is on each server
				if !cfg.Connections[conn].sqlFunctionsExist() {
					cfg.Connections[conn].loadPgPartman()
				}
				// Then create the partitions based on the config
				cfg.Connections[conn].CreateParents()
				// Then schedule maintenance for the partitions

				for pName, p := range cfg.Connections[conn].Partitions {
					jobName := pName + " " + p.Interval + " partition on " + p.Table + " table maintenance"
					switch p.Interval {
					case "quarter-hour", "half-hour":
						c.AddFunc("@every 30m", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					case "hourly":
						c.AddFunc("@hourly", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					case "daily":
						c.AddFunc("@daily", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					case "weekly":
						c.AddFunc("@weekly", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					case "monthly", "quarterly":
						c.AddFunc("@monthly", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					case "yearly":
						c.AddFunc("@yearly", func() {
							cfg.Connections[conn].RunMaintenance(pName, true, true)
						}, jobName)
						break
					}

				}
			} else {
				l.Error(err)
			}
		}

		p := strconv.Itoa(cfg.Api.Port)
		// But if it can't be parsed (maybe wasn't set) then just run the daemon without the API server.
		// This means partitions will be managed, but nothing can be changed unless the daemon is retstarted.
		if p != "0" {
			restMiddleware := []rest.Middleware{}

			// If additional origins were allowed for CORS, handle them
			if len(cfg.Api.Cors.AllowedOrigins) > 0 {
				restMiddleware = append(restMiddleware,
					&rest.CorsMiddleware{
						RejectNonCorsRequests: false,
						OriginValidator: func(origin string, request *rest.Request) bool {
							for _, allowedOrigin := range cfg.Api.Cors.AllowedOrigins {
								// If the request origin matches one of the allowed origins, return true
								if origin == allowedOrigin {
									return true
								}
							}
							return false
						},
						AllowedMethods: []string{"GET", "POST", "PUT"},
						AllowedHeaders: []string{
							"Accept", "Content-Type", "X-Custom-Header", "Origin"},
						AccessControlAllowCredentials: true,
						AccessControlMaxAge:           3600,
					},
				)
			}
			// If api keys are defined, setup basic auth (any key listed allows full access, there are no roles for now, this is just very basic auth)
			if len(cfg.Api.AuthKeys) > 0 {
				restMiddleware = append(restMiddleware,
					&BasicAuthMw{
						Realm: "gopartman API",
						Key:   "",
					},
				)
			}

			handler := rest.ResourceHandler{
				EnableRelaxedContentType: true,
				PreRoutingMiddlewares:    restMiddleware,
			}
			err := handler.SetRoutes(
				&rest.Route{"GET", "/partitions", showPartitions},
				&rest.Route{"GET", "/schedule", showSchedule},
			)
			if err != nil {
				log.Fatal(err)
			}

			log.Println("gopartman API listening on port " + p)
			log.Fatal(http.ListenAndServe(":"+p, &handler))
		} else {
			log.Println("gopartman running without API")
			// Run forever
			for {

			}
		}
	}

}
