package main

import (
	"flag"
  "github.com/agquick/pgpq/pkg/pgpq"
	"github.com/sirupsen/logrus"
)

func main() {
	var port int
	var store_url string
	var do_migrate bool
	var log_level_str string
	log := logrus.New()

	flag.IntVar(&port, "port", 8000, "Server port")
	flag.StringVar(&store_url, "storeurl", "", "Connection URL for queue store")
	flag.BoolVar(&do_migrate, "migrate", false, "Perform database migration")
	flag.StringVar(&log_level_str, "ll", "info", "Log Level")
	flag.Parse()


	log_level, err := logrus.ParseLevel(log_level_str)
	if err != nil {
		log.SetLevel(log_level)
	}
	pgpq.SetLogger(log)

	if do_migrate == true {
		pgpq.PerformMigration(store_url)
	} else {
		pgpq.StartServer(port, store_url)
	}
}
