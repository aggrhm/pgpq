package main

import (
	"flag"
  "github.com/agquick/pgpq/pkg/pgpq"
)

func main() {
	port := flag.Int("port", 8000, "Server port")
	store_url := flag.String("storeurl", "", "Connection URL for queue store")
	do_migrate := flag.Bool("migrate", false, "Perform database migration")
	flag.Parse()
	if do_migrate != nil && *do_migrate == true {
		pgpq.PerformMigration(*store_url)
	} else {
		pgpq.StartServer(*port, *store_url)
	}
}
