package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Rakanixu/elastic-analytics/data"
	"github.com/Rakanixu/elastic-analytics/db"
	_ "github.com/Rakanixu/elastic-analytics/db/elastic"
)

const ()

var (
	dbEndoint     *string
	maxGoroutines *int
	wg            sync.WaitGroup
)

func main() {
	p := flag.String("p", "", "Path to CSV files")
	maxGoroutines = flag.Int("t", 4, "Number of parallel gourotines")
	dbEndoint = flag.String("db_endpoint", "http://localhost:9200", "DB endpoint")

	flag.Parse()

	// Initialize imported DB implementation
	if err := db.Init(*dbEndoint); err != nil {
		log.Fatal(err)
	}
}

func getCSVFiles(path string) []string {
	if path != "" {
		path = fmt.Sprintf("%s/*.csv", path)
	} else {
		path = "*.csv"
	}

	files, err := filepath.Glob(path)
	if err != nil {
		log.Fatal(err)
	}

	return files
}

func analyzeCSVs(paths []string) {
	// Channel buffer equal to number of gourotines
	blocker := make(chan struct{}, *maxGoroutines)

	for _, v := range paths {
		// Fill blocker channel
		blocker <- struct{}{}
		wg.Add(1)

		go func(path string) {
			s, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
			}
			defer s.Close()

			fi, err := s.Stat()
			log.Println("Process:", fi.Name(), "Bytes:", fi.Size())

			b := make([]byte, fi.Size())
			count, err := s.Read(b)
			if err != nil {
				log.Fatal(err)
			}

			records := strings.Split(string(b[:count]), "\n")
			columms := strings.Split(records[0], ",")

			i := -1
			j := -1
			l := -1
			for k, v := range columms {
				// CSV can contain CDN or "CDN"
				switch trimDoubleQuote(v) {
				case *dimension:
					i = k
				case *key:
					j = k
				case METADATA:
					l = k
				}
			}

			analyzeCSV(path, records[1:], i, j, l)
			// Read from blocker channel to allow next iteration
			<-blocker
			wg.Done()
		}(v)
	}

	wg.Wait()
}

func analyzeCSV(name string, csv []string, aggDimensionIndex, filterIndex, metadataIndex int) {
	if aggDimensionIndex >= 0 && filterIndex >= 0 /* && metadataIndex >= 0 */ {
		var n int64
		d := data.NewData(name)
		f := false

		if filterIndex > 0 && len(*value) > 0 {
			f = true
		}

		for _, v := range csv {
			r := strings.Split(v, ",")
			// Don't push records which type is different to the one set on flags
			if len(r) > 1 && len(r) > aggDimensionIndex && len(r) > filterIndex && !(f && r[filterIndex] != *value) {
				var rh string
				left := strings.Index(v, TRIM_LEFT_JSON)
				right := strings.Index(v, TRIM_RIGHT_JSON)

				// Found hash
				if left > 0 && right > 0 {
					rh = v[left+len(TRIM_LEFT_JSON)+5 : right-5] // Get the hash from the stringify JSON
				}
				des := r[aggDimensionIndex]
				n++

				if *dimension == ERR_CODE {
					// HARDCODED: specific case for a CSV specific pattern
					des = fmt.Sprintf("%s %s", r[aggDimensionIndex], r[1])
				}

				if *output == OUTPUT_ALL || *output == r[aggDimensionIndex] {
					d.AddDataRow(v)
				}

				d.AddRecord(data.NewRecord(des, rh))
			}
		}
		d.SetTotal(n)
		d.Date()

		results = append(results, d)
	}
}

func trimDoubleQuote(s string) string {
	return strings.Replace(s, `"`, "", -1)
}
