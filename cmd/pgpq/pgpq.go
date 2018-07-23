package main

import (
	"flag"
  "github.com/agquick/pgpq/pkg/pgpq"
)

func main() {
	store_url := flag.String("storeurl", "", "Connection URL for queue store")
	flag.Parse()
  pgpq.StartServer(*store_url)
}
