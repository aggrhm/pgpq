package main

import (
	"flag"
  "github.com/agquick/pgpq/pkg/pgpq"
)

func main() {
	port := flag.Int("port", 8000, "Server port")
	store_url := flag.String("storeurl", "", "Connection URL for queue store")
	flag.Parse()
  pgpq.StartServer(*port, *store_url)
}
