package app

import (
	"flag"
	"github.com/rudderlabs/rudder-server/utils/types"
	"os"

	"github.com/rudderlabs/rudder-server/config"
)

// Options contains application's initialisation options
type Options struct {
	NormalMode    bool
	DegradedMode  bool
	MigrationMode string
	ClearDB       bool
	Cpuprofile    string
	Memprofile    string
	VersionFlag   bool
	EventSchemas  EventSchemaFeature
}

// LoadOptions loads application's initialisation options based on command line flags and environment
func LoadOptions() *Options {
	// Parse command line options
	normalMode := flag.Bool("normal-mode", false, "a bool")
	degradedMode := flag.Bool("degraded-mode", false, "a bool")
	clearDB := flag.Bool("cleardb", false, "a bool")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "write memory profile to `file`")
	versionFlag := flag.Bool("v", false, "Print the current version and exit")
	serverMode := os.Getenv("RSERVER_MODE")
	if serverMode == "normal" {
		*normalMode = true
	} else if serverMode == "degraded" {
		*degradedMode = true
	}

	flag.Parse()

	return &Options{
		NormalMode:    *normalMode,
		DegradedMode:  *degradedMode,
		MigrationMode: getMigrationMode(),
		ClearDB:       *clearDB,
		Cpuprofile:    *cpuprofile,
		Memprofile:    *memprofile,
		VersionFlag:   *versionFlag,
	}
}

func getMigrationMode() string {
	return config.GetEnv("MIGRATION_MODE", "")
}

// EventSchemaFeature handles event schemas
type EventSchemaFeature interface {
	Setup() types.EventSchemasI
}

// EventSchemaFeatureSetup is a function that initializes a EventSchema feature, based on application instance
type EventSchemaFeatureSetup func(Interface) EventSchemaFeature

var eventSchemasFeatureSetup EventSchemaFeatureSetup

// RegisterEventSchemaFeature registers a event-schema feature implementation
func RegisterEventSchemaFeature(f EventSchemaFeatureSetup) {
	eventSchemasFeatureSetup = f
}
