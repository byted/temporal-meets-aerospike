// Command temporal-aerospike-server runs Temporal Server with Aerospike as the
// operational persistence store.
//
// A custom datastore is registered in Go, not selected by a config string, so
// the stock temporalio/auto-setup image cannot run this store -- the binary has
// to be built with the store compiled in. That is the entire reason this
// command exists.
package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/temporal"

	// Visibility runs on Elasticsearch in the compose stack; the SQL plugins
	// are linked in so a developer can point visibility at SQLite locally
	// without rebuilding.
	_ "go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"

	"github.com/stefanselent/temporal-meets-aerospike/store/aerospike"
)

func main() {
	if err := buildCLI().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func buildCLI() *cli.App {
	app := cli.NewApp()
	app.Name = "temporal-aerospike-server"
	app.Usage = "Temporal Server backed by Aerospike for operational persistence"

	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "config-dir",
			Aliases: []string{"c"},
			Value:   "config",
			Usage:   "directory containing the server configuration",
			EnvVars: []string{"TEMPORAL_CONFIG_DIR"},
		},
		&cli.StringFlag{
			Name:    "env",
			Aliases: []string{"e"},
			Value:   "temporal",
			Usage:   "config file basename to load (<env>.yaml)",
			EnvVars: []string{"TEMPORAL_ENVIRONMENT"},
		},
		&cli.StringFlag{
			Name:    "zone",
			Aliases: []string{"az"},
			Usage:   "availability zone",
			EnvVars: []string{"TEMPORAL_AVAILABILITY_ZONE"},
		},
		&cli.BoolFlag{
			Name:  "allow-no-auth",
			Usage: "allow no authorizer to be configured",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:  "start",
			Usage: "start the server",
			Flags: []cli.Flag{
				&cli.StringSliceFlag{
					Name:    "service",
					Aliases: []string{"svc"},
					Value:   cli.NewStringSlice(temporal.DefaultServices...),
					Usage:   "services to start",
				},
			},
			Action: start,
		},
	}

	return app
}

func start(c *cli.Context) error {
	configDir := c.String("config-dir")
	env := c.String("env")
	zone := c.String("zone")

	logger := log.NewZapLogger(log.BuildZapLogger(log.Config{
		Stdout: true,
		Level:  "info",
	}))

	cfg, err := config.Load(
		config.WithConfigDir(configDir),
		config.WithEnv(env),
		config.WithZone(zone),
	)
	if err != nil {
		return fmt.Errorf("loading config from %s: %w", configDir, err)
	}

	logger.Info("starting temporal-aerospike-server",
		tag.NewStringTag("config-dir", configDir),
		tag.NewStringTag("env", env),
	)

	server, err := temporal.NewServer(
		temporal.WithConfig(cfg),
		temporal.ForServices(c.StringSlice("service")),
		temporal.WithLogger(logger),
		temporal.InterruptOn(temporal.InterruptCh()),

		// The one line that matters: without it the customDatastore block in
		// the YAML has nothing to dispatch to.
		temporal.WithCustomDataStoreFactory(aerospike.NewAbstractFactory()),
	)
	if err != nil {
		return fmt.Errorf("creating server: %w", err)
	}

	if err := server.Start(); err != nil {
		return fmt.Errorf("starting server: %w", err)
	}
	return nil
}
