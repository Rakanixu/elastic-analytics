package main

import (
	"flag"
	"log"

	"github.com/Rakanixu/elastic-analytics/csv"
	"github.com/Rakanixu/elastic-analytics/db"
	_ "github.com/Rakanixu/elastic-analytics/db/elastic"
)

func main() {
	p := flag.String("path", "", "Path to CSV files")
	maxGoroutines := flag.Int("routines", 4, "Number of parallel gourotines")
	dbEndoint := flag.String("db_endpoint", "http://localhost:9200", "DB endpoint")
	flag.Parse()

	// Initialize imported DB implementation
	if err := db.Init(*dbEndoint); err != nil {
		log.Fatal(err)
	}

	parser := csv.NewParser(*p, *maxGoroutines)
	parser.ToJSON()
}
