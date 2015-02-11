// gopartman will manage Postgres partitions.
//
// - Partitions to be managed are defined in gopartman.yml
// - Maintenance is regularly performed so there's no need to set any commands to run in a crontab or anything like that
// - An API can optionally be configured to allow:
// 		- CORS and Basic Auth settings for access to the API (configured in gopartman.yml)
// 		- Changes to configuration
// 		- Addition of new partitions
// 		- Reporting with information about partition settings and state

package main

import (
	"errors"
	"github.com/ant0ine/go-json-rest/rest"
	"github.com/fatih/color"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/spf13/cobra"
	"github.com/tmaiaroto/cron"
	"gopkg.in/guregu/null.v2"
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
const ver = "0.2.0"

var GoPartManCmd = &cobra.Command{
	Use:   "gopartman",
	Short: "A Postgres Partition Manager",
	//Long: ``,
	Run: func(cmd *cobra.Command, args []string) {
		// Do Stuff Here
	},
}

type GoPartManFlags struct {
	verbose    bool
	daemon     bool
	server     string
	partition  string
	configFile string
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
		Functions struct {
			RunMaintenance    map[string]interface{} `json:"runMaintenance" yaml:"runMaintenance"`
			UndoPartition     map[string]interface{} `json:"undoPartition" yaml:"undoPartition"`
			SetRetention      map[string]interface{} `json:"setRetention" yaml:"setRetention"`
			PartitionDataId   map[string]interface{} `json:"partitionDataId" yaml:"partitionDataId"`
			PartitionDataTime map[string]interface{} `json:"partitionDataTime" yaml:"partitionDataTime"`
			DropPartitionId   map[string]interface{} `json:"dropPartitionId" yaml:"dropPartitionId"`
			DropPartitionTime map[string]interface{} `json:"dropPartitionTime" yaml:"dropPartitionTime"`
		} `json:"functions" yaml:"functions"`
		RetentionSchema    null.String `json:"retentionSchema" yaml:"retentionSchema"`
		RetentionKeepTable bool        `json:"retentionKeepTable" yaml:"retentionKeepTable"`
		Jobmon             bool        `json:"jobmon" yaml:"jobmon"`
	} `json:"options" yaml:"options"`
	MaintenanceJobId int64 `json:"maintenanceJobId" yaml:"maintenanceJobId"`
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
	ConstraintCols     string `json:"constraint_cols" yaml:"constraint_cols" db:"constraint_cols"`
	Control            string `json:"control" yaml:"control" db:"control"`
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

// A struct for children partition tables
type Child struct {
	Table       string `json:"table" db:"table"`
	Records     int    `json:"records" db:"records"`
	BytesOnDisk uint64 `json:"bytesOnDisk" db:"bytesOnDisk"`
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
	c.Start()
	cfg.Cron = c
}

func NewPostgresConnection(cfg Server) (DB, error) {
	db, err := sqlx.Connect("postgres", "host="+cfg.Host+" port="+cfg.Port+" sslmode=disable  dbname="+cfg.Database+" user="+cfg.User+" password="+cfg.Password)
	if err != nil {
		l.Error(err)
	}
	return DB{*db, cfg.Partitions}, err
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

// Helper function to return the Partition from configuration if it exists
func GetPartition(serverName string, partitionName string) (*DB, *Partition, error) {
	var err error
	if sVal, ok := cfg.Connections[serverName]; ok {
		if pVal, ok := cfg.Connections[serverName].Partitions[partitionName]; ok {
			err = nil
			return &sVal, &pVal, err
		}
	}
	err = errors.New("that partition does not seem to be configured in gopartman.yml")
	return &DB{}, &Partition{}, err
}

func main() {
	GoPartManCmd.AddCommand(versionCmd)
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.daemon, "daemon", "m", false, "daemon mode")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.configFile, "config", "c", "", "An optional path to the YML configuration file")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.server, "server", "s", "", "The configured server")
	GoPartManCmd.PersistentFlags().StringVarP(&flags.partition, "partition", "p", "", "The configured partition")
	GoPartManCmd.PersistentFlags().BoolVarP(&flags.verbose, "verbose", "v", false, "verbose output")

	// Load the configured partitions
	cfgPath := "/etc/gopartman.yml"
	if _, err := os.Stat(cfgPath); err != nil {
		cfgPath = "./gopartman.yml"
	}
	// If a specific path was given
	if flags.configFile != "" {
		cfgPath = flags.configFile
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

	// Set up all of the connections from the configuration and ensure they have the pg_partman schema, table, and functions loaded.
	cfg.Connections = map[string]DB{}
	for conn, credentials := range cfg.Servers {
		cfg.Connections[conn], err = NewPostgresConnection(credentials)
		if err == nil {
			// Set some defaults from the config for partitions
			for pName, _ := range cfg.Connections[conn].Partitions {
				part := cfg.Connections[conn].Partitions[pName]

				cfg.Connections[conn].Partitions[pName] = part
			}

			// First make sure pg_partman is on each server
			if !cfg.Connections[conn].sqlFunctionsExist() {
				cfg.Connections[conn].loadPgPartman()
			}
			// Then create the partitions based on the config
			cfg.Connections[conn].CreateParents()
		} else {
			l.Error(err)
		}
	}

	// Add commands after partitions are configured
	GoPartManCmd.AddCommand(installPartmanCmd)
	GoPartManCmd.AddCommand(reinstallPartmanCmd)
	GoPartManCmd.AddCommand(createParentCmd)
	GoPartManCmd.AddCommand(runMaintenanceCmd)
	GoPartManCmd.AddCommand(undoPartitionCmd)
	GoPartManCmd.AddCommand(getPartitionInfoCmd)
	GoPartManCmd.AddCommand(setPartitionRetentionCmd)
	GoPartManCmd.AddCommand(removePartitionRetentionCmd)

	GoPartManCmd.Execute()

	// Notify, but keep running because it is possible that partitions will be added later via the API.
	if len(cfg.Connections) == 0 {
		l.Info("No configured partitions.")
	}

	// Then schedule maintenance for the partitions and optionally start API server if running forever
	if flags.daemon {
		// Create a schedule for jobs
		newSchedule()

		for conn, _ := range cfg.Servers {
			for pName, p := range cfg.Connections[conn].Partitions {
				jobName := pName + " " + p.Interval + " partition on " + p.Table + " table maintenance"
				switch p.Interval {
				case "quarter-hour", "half-hour":
					// setting a temporary "part" value as a work around for not being able to assign cfg.Connections[conn].Partitions[pName].MaintenanceJobId directly
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@every 30m", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				case "hourly":
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@hourly", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				case "daily":
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@daily", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				case "weekly":
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@weekly", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				case "monthly", "quarterly":
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@monthly", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				case "yearly":
					part := cfg.Connections[conn].Partitions[pName]
					part.MaintenanceJobId, _ = c.AddFunc("@yearly", func() {
						cfg.Connections[conn].RunMaintenance(pName, true, true)
					}, jobName)
					cfg.Connections[conn].Partitions[pName] = part
					break
				}
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
				&rest.Route{"GET", "/partition/:server/:partition", showPartition},
				&rest.Route{"GET", "/partition/:server/:partition/config", showPartitionConfig},
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
